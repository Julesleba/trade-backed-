import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import crypto from 'crypto';
import pg from 'pg';

dotenv.config();

const { Pool } = pg;

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: process.env.ALLOWED_ORIGIN || '*',
    allowedHeaders: ['Content-Type', 'x-admin-secret', 'x-admin-key', 'Authorization'],
    methods: ['GET', 'POST', 'OPTIONS'],
  })
);

// ==========================================
// 1. STRATEGY DEFAULTS & CONSTANTS
// ==========================================
const DEFAULTS = {
  SYMBOL: 'SOLUSDT',
  TRADE_AMOUNT_USDT: 5.5,        // Above $5 Binance spot minimum
  BUY_DIP_PERCENT: 0.02,         // -2.0% dip from referencePrice
  TAKE_PROFIT_PERCENT: 0.05,     // +5.0% hard take-profit fallback
  STOP_LOSS_PERCENT: 0.03,       // -3.0% hard stop-loss
  TRAIL_START_PERCENT: 0.025,    // Start trailing once +2.5% above entry
  TRAIL_PULLBACK_PERCENT: 0.012, // Sell if price pulls back 1.2% from peak
  REQUIRE_BOUNCE_TICK: true,     // Require livePrice > lastPrice on dip trigger
  PRICE_WINDOW_SIZE: 20,         // Rolling price samples for context
  FEE_RATE_PER_SIDE: 0.001,      // ~0.1% spot fee estimate per side
  MAX_TRADES_PER_DAY: 5,
  MAX_CONSECUTIVE_LOSSES: 3,
  MAX_DAILY_LOSS_USDT: 3.0,      // Stop new entries if daily PnL <= -$3.00
  COOLDOWN_MINUTES: 30,
  POLL_INTERVAL_MS: 60 * 1000,
  TIMEZONE: 'Africa/Lagos',
  ACTIVE_START_HOUR: 7,          // 07:00 AM WAT
  ACTIVE_END_HOUR: 22,           // 10:00 PM WAT
};

// ==========================================
// 2. NEON DB & BINANCE SPOT CONFIG
// ==========================================
if (!process.env.DATABASE_URL) {
  console.error('Missing DATABASE_URL in environment variables.');
  process.exit(1);
}

const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// FIX 1: Env fix - supports both BINANCE_TESTNET and USE_TESTNET
const USE_BINANCE_TESTNET = (process.env.BINANCE_TESTNET ?? process.env.USE_TESTNET ?? 'true') !== 'false';
const BINANCE_BASE_URL = USE_BINANCE_TESTNET
  ? 'https://testnet.binance.vision/api'
  : 'https://api.binance.com/api'; //

const BINANCE_API_KEY = process.env.BINANCE_API_KEY || '';
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET || '';

// FIX BUG 1: Binance server time offset - fixes -1021 timestamp error on Render
let binanceTimeOffset = 0;

async function binancePublicRequest(path, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const url = `${BINANCE_BASE_URL}${path}${qs ? `?${qs}` : ''}`;
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.code) {
    throw new Error(data.msg || `Binance public request failed (${res.status})`);
  }
  return data;
}

async function syncBinanceTime() {
  try {
    const data = await binancePublicRequest('/v3/time');
    binanceTimeOffset = data.serverTime - Date.now();
    console.log(`Binance time synced, offset: ${binanceTimeOffset}ms`);
  } catch (err) {
    console.error('Binance time sync failed, using local time:', err.message);
    binanceTimeOffset = 0;
  }
}

function signBinanceParams(params = {}) {
  if (!BINANCE_API_KEY || !BINANCE_API_SECRET) {
    throw new Error('Missing BINANCE_API_KEY or BINANCE_API_SECRET in environment variables.');
  }
  // FIX BUG 1: use Binance server time + recvWindow
  const query = new URLSearchParams({
    ...params,
    recvWindow: '5000',
    timestamp: (Date.now() + binanceTimeOffset).toString(),
  }).toString();

  const signature = crypto
    .createHmac('sha256', BINANCE_API_SECRET)
    .update(query)
    .digest('hex');

  return `${query}&signature=${signature}`;
}

async function binanceSignedRequest(method, path, params = {}) {
  const signedQs = signBinanceParams(params);
  const url = `${BINANCE_BASE_URL}${path}?${signedQs}`;
  const res = await fetch(url, {
    method,
    headers: {
      'X-MBX-APIKEY': BINANCE_API_KEY,
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.code) {
    throw new Error(data.msg || `Binance signed request failed (${res.status})`);
  }
  return data;
}

// ==========================================
// 3. BOT STATE
// ==========================================
let state = {
  isRunning: true,
  tradeAmountUsdt: DEFAULTS.TRADE_AMOUNT_USDT,
  maxDailyLossUsdt: DEFAULTS.MAX_DAILY_LOSS_USDT,
  requireBounceTick: DEFAULTS.REQUIRE_BOUNCE_TICK,
  currentDate: '',
  referencePrice: null,
  buyTargetPrice: null,
  inPosition: false,
  entryPrice: null,
  highestPriceSinceEntry: null,
  trailingActive: false,
  takeProfitPrice: null,
  stopLossPrice: null,
  purchasedQty: 0,
  tradesToday: 0,
  consecutiveLosses: 0,
  winsToday: 0,
  lossesToday: 0,
  dailyPnlUsdt: 0,
  cooldownUntil: null,
  lastPrice: null,
  priceHistory: [],
  logs: [],
};

// ==========================================
// 4. DATABASE INITIALIZATION & PERSISTENCE
// ==========================================
async function initDatabase() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS bot_state (
      id INTEGER PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS trade_journal (
      id SERIAL PRIMARY KEY,
      trade_number INTEGER,
      trade_date TEXT,
      entry_price DOUBLE PRECISION,
      exit_price DOUBLE PRECISION,
      qty DOUBLE PRECISION,
      pnl_usdt DOUBLE PRECISION,
      outcome TEXT,
      reason TEXT,
      closed_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // MERGE 2: bot_logs table from my version - logs survive restarts
  await db.query(`
    CREATE TABLE IF NOT EXISTS bot_logs (
      id SERIAL PRIMARY KEY,
      message TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  const res = await db.query('SELECT data FROM bot_state WHERE id = 1');
  if (res.rows.length > 0 && res.rows[0].data) {
    state = {
      ...state,
      ...res.rows[0].data,
      logs: Array.isArray(res.rows[0].data.logs) ? res.rows[0].data.logs.slice(0, 100) : [],
      priceHistory: Array.isArray(res.rows[0].data.priceHistory)
        ? res.rows[0].data.priceHistory.slice(-DEFAULTS.PRICE_WINDOW_SIZE)
        : [],
    };
  } else {
    await saveStateToDb();
  }
}

async function saveStateToDb() {
  try {
    await db.query(
      `
      INSERT INTO bot_state (id, data, updated_at)
      VALUES (1, $1::jsonb, NOW())
      ON CONFLICT (id)
      DO UPDATE SET data = EXCLUDED.data, updated_at = NOW();
      `,
      [JSON.stringify(state)]
    );
  } catch (err) {
    console.error('Failed saving state to Neon DB:', err.message);
  }
}

async function insertTradeToDb(record) {
  try {
    await db.query(
      `
      INSERT INTO trade_journal
        (trade_number, trade_date, entry_price, exit_price, qty, pnl_usdt, outcome, reason, closed_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
      `,
      [
        record.tradeNumber,
        record.date,
        record.entryPrice,
        record.exitPrice,
        record.qty,
        record.pnlUsdt,
        record.outcome,
        record.reason,
        record.closedAt,
      ]
    );
  } catch (err) {
    console.error('Failed inserting trade to Neon DB:', err.message);
  }
}

async function getRecentTradesFromDb(limit = 50) {
  const res = await db.query(
    `
    SELECT
      id,
      trade_number AS "tradeNumber",
      trade_date AS "date",
      entry_price AS "entryPrice",
      exit_price AS "exitPrice",
      qty,
      pnl_usdt AS "pnlUsdt",
      outcome,
      reason,
      closed_at AS "closedAt"
    FROM trade_journal
    ORDER BY id DESC
    LIMIT $1;
    `,
    [limit]
  );
  return res.rows;
}

// ==========================================
// 5. HELPER FUNCTIONS
// ==========================================
function addLog(message) {
  const timestamp = new Date().toLocaleString('en-GB', { timeZone: DEFAULTS.TIMEZONE });
  const entry = `[${timestamp} WAT] ${message}`;
  console.log(entry);
  state.logs.unshift(entry);
  if (state.logs.length > 100) state.logs.pop();
  // MERGE 2: Save log to Neon - fire and forget, does not block trading logic
  db.query('INSERT INTO bot_logs (message) VALUES ($1)', [entry]).catch(() => {});
}

function getLagosTimeInfo() {
  const now = new Date();
  const dateStr = now.toLocaleDateString('en-CA', { timeZone: DEFAULTS.TIMEZONE });
  const hourStr = now.toLocaleTimeString('en-GB', {
    timeZone: DEFAULTS.TIMEZONE,
    hour: '2-digit',
    hour12: false,
  });
  return { dateStr, hour: parseInt(hourStr, 10) };
}

function getRollingAverage(prices) {
  if (!prices.length) return null;
  const sum = prices.reduce((acc, p) => acc + p, 0);
  return Number((sum / prices.length).toFixed(4));
}

async function resetDailyStats(newDateStr) {
  state.currentDate = newDateStr;
  state.referencePrice = null;
  state.buyTargetPrice = null;
  state.tradesToday = 0;
  state.consecutiveLosses = 0;
  state.winsToday = 0;
  state.lossesToday = 0;
  state.dailyPnlUsdt = 0;
  state.cooldownUntil = null;
  addLog(`New trading day initialized (${newDateStr}). Daily counters reset.`);
  await saveStateToDb();
}

async function setReferencePrice(price) {
  state.referencePrice = price;
  state.buyTargetPrice = Number((price * (1 - DEFAULTS.BUY_DIP_PERCENT)).toFixed(4));
  addLog(
    `Reference price set to $${price.toFixed(4)} | Buy Target (-2%): $${state.buyTargetPrice}`
  );
  await saveStateToDb();
}

// ==========================================
// 6. BINANCE EXCHANGE FUNCTIONS
// ==========================================
async function fetchLivePrice() {
  const data = await binancePublicRequest('/v3/ticker/price', {
    symbol: DEFAULTS.SYMBOL,
  }); //
  const price = parseFloat(data.price);
  if (!Number.isFinite(price) || price <= 0) {
    throw new Error('Ticker fetch failed: invalid price');
  }
  return price;
}

async function getSolAvailableBalance() {
  const data = await binanceSignedRequest('GET', '/v3/account');
  const assetInfo = Array.isArray(data.balances)
    ? data.balances.find((b) => b.asset === 'SOL')
    : null;
  return assetInfo ? parseFloat(assetInfo.free) : 0;
}

async function executeMarketBuy(currentPrice) {
  const amountUsdt = Number(state.tradeAmountUsdt);
  if (!Number.isFinite(amountUsdt) || amountUsdt < 5) {
    throw new Error(`Invalid tradeAmountUsdt (${amountUsdt}).Must be at least $5.`);
  }

  addLog(`Dip + bounce confirmed! Buying $${amountUsdt} of ${DEFAULTS.SYMBOL} @ $${currentPrice.toFixed(4)}...`);

  // FIX 2: Snapshot balance BEFORE buy, so we only count what bot bought
  const solBalBefore = await getSolAvailableBalance();

  await binanceSignedRequest('POST', '/v3/order', {
    symbol: DEFAULTS.SYMBOL,
    side: 'BUY',
    type: 'MARKET',
    quoteOrderQty: amountUsdt.toFixed(2),
  }); //

  await new Promise((r) => setTimeout(r, 2000));
  const solBalAfter = await getSolAvailableBalance();
  // FIX 2 + BUG 3: only the delta, floored to 0.001 step
  const sellableQty = Math.floor((solBalAfter - solBalBefore) * 1000) / 1000;

  if (sellableQty <= 0) {
    throw new Error('Buy settled with 0 sellable SOL quantity (check minimum precision/balance).');
  }

  state.inPosition = true;
  state.entryPrice = currentPrice;
  state.highestPriceSinceEntry = currentPrice;
  state.trailingActive = false;
  state.purchasedQty = sellableQty;
  state.takeProfitPrice = Number((currentPrice * (1 + DEFAULTS.TAKE_PROFIT_PERCENT)).toFixed(4));
  state.stopLossPrice = Number((currentPrice * (1 - DEFAULTS.STOP_LOSS_PERCENT)).toFixed(4));

  addLog(
    `BOUGHT ${sellableQty} SOL @ $${currentPrice.toFixed(4)} | Trail Start (+${(
      DEFAULTS.TRAIL_START_PERCENT * 100
    ).toFixed(1)}%) | Hard TP: $${state.takeProfitPrice} | SL: $${state.stopLossPrice}`
  );

  await saveStateToDb();
}

async function executeMarketSell(currentPrice, reason) {
  addLog(`Closing position (${reason}) at $${currentPrice.toFixed(4)}...`);

  // FIX 2: Sell ONLY what bot bought, not whole wallet. Cap by balance for safety.
  const solBal = await getSolAvailableBalance();
  const qtyToSell = Math.floor(Math.min(state.purchasedQty || 0, solBal) * 1000) / 1000;

  if (qtyToSell <= 0) {
    addLog('Warning: No SOL balance found to sell. Resetting position state.');
    state.inPosition = false;
    state.entryPrice = null;
    state.highestPriceSinceEntry = null;
    state.trailingActive = false;
    state.takeProfitPrice = null;
    state.stopLossPrice = null;
    state.purchasedQty = 0;
    await saveStateToDb();
    return;
  }

  await binanceSignedRequest('POST', '/v3/order', {
    symbol: DEFAULTS.SYMBOL,
    side: 'SELL',
    type: 'MARKET',
    quantity: qtyToSell.toString(),
  }); //

  const entryRef = state.entryPrice || currentPrice;
  const grossPnl = (currentPrice - entryRef) * qtyToSell;
  const estimatedFees =
    (entryRef * qtyToSell + currentPrice * qtyToSell) * DEFAULTS.FEE_RATE_PER_SIDE;
  const netPnl = Number((grossPnl - estimatedFees).toFixed(4));
  const isWin = netPnl >= 0;

  state.tradesToday += 1;
  state.dailyPnlUsdt = Number((state.dailyPnlUsdt + netPnl).toFixed(4));

  if (isWin) {
    state.winsToday += 1;
    state.consecutiveLosses = 0;
  } else {
    state.lossesToday += 1;
    state.consecutiveLosses += 1;
  }

  const record = {
    tradeNumber: state.tradesToday,
    date: state.currentDate,
    entryPrice: entryRef,
    exitPrice: currentPrice,
    qty: qtyToSell,
    pnlUsdt: netPnl,
    outcome: isWin ? 'WIN' : 'LOSS',
    reason,
    closedAt: new Date().toISOString(),
  };

  await insertTradeToDb(record);

  addLog(
    `SOLD ${qtyToSell} SOL @ $${currentPrice.toFixed(4)} | ${record.outcome} ($${netPnl} net) | Daily PnL: $${state.dailyPnlUsdt}`
  );

  state.inPosition = false;
  state.entryPrice = null;
  state.highestPriceSinceEntry = null;
  state.trailingActive = false;
  state.takeProfitPrice = null;
  state.stopLossPrice = null;
  state.purchasedQty = 0;

  state.referencePrice = null;
  state.buyTargetPrice = null;
  state.cooldownUntil = Date.now() + DEFAULTS.COOLDOWN_MINUTES * 60 * 1000;

  addLog(`Cooldown active for ${DEFAULTS.COOLDOWN_MINUTES}m. Reference price resets afterward.`);
  await saveStateToDb();
}

// ==========================================
// 7. MAIN TRADING ENGINE LOOP
// ==========================================
async function runTradingCycle() {
  if (!state.isRunning) return;

  try {
    const { dateStr, hour } = getLagosTimeInfo();

    if (state.currentDate !== dateStr) {
      await resetDailyStats(dateStr);
    }

    const prevPrice = state.lastPrice;
    const livePrice = await fetchLivePrice();
    state.lastPrice = livePrice;

    state.priceHistory.push(livePrice);
    if (state.priceHistory.length > DEFAULTS.PRICE_WINDOW_SIZE) {
      state.priceHistory.shift();
    }

    // 1. OPEN POSITION MANAGEMENT (Trailing TP + Hard TP + Hard SL)
    if (state.inPosition && state.entryPrice) {
      if (
        state.highestPriceSinceEntry === null ||
        livePrice > state.highestPriceSinceEntry
      ) {
        state.highestPriceSinceEntry = livePrice;
      }

      const trailActivationPrice = state.entryPrice * (1 + DEFAULTS.TRAIL_START_PERCENT);
      if (livePrice >= trailActivationPrice) {
        state.trailingActive = true;
      }

      const trailingStopPrice = state.trailingActive
        ? Number((state.highestPriceSinceEntry * (1 - DEFAULTS.TRAIL_PULLBACK_PERCENT)).toFixed(4))
        : null;

      if (trailingStopPrice !== null && livePrice <= trailingStopPrice) {
        await executeMarketSell(
          livePrice,
          `TRAILING_TP (peak $${state.highestPriceSinceEntry.toFixed(4)}, pull -${(
            DEFAULTS.TRAIL_PULLBACK_PERCENT * 100
          ).toFixed(1)}%)`
        );
      } else if (livePrice >= state.takeProfitPrice) {
        await executeMarketSell(livePrice, 'HARD_TAKE_PROFIT (+5%)');
      } else if (livePrice <= state.stopLossPrice) {
        await executeMarketSell(livePrice, 'STOP_LOSS (-3%)');
      } else {
        const trailInfo = trailingStopPrice
          ? ` | Trail Stop: $${trailingStopPrice}`
          : ` | Trail Starts @ $${trailActivationPrice.toFixed(4)}`;
        addLog(
          `Holding SOL | Now: $${livePrice.toFixed(4)} | Peak: $${state.highestPriceSinceEntry.toFixed(
            4
          )}${trailInfo} | Hard SL: $${state.stopLossPrice}`
        );
      }

      await saveStateToDb();
      return;
    }

    // 2. DAILY CIRCUIT BREAKERS
    if (state.tradesToday >= DEFAULTS.MAX_TRADES_PER_DAY) {
      return;
    }

    if (state.consecutiveLosses >= DEFAULTS.MAX_CONSECUTIVE_LOSSES) {
      return;
    }

    if (state.dailyPnlUsdt <= -Math.abs(Number(state.maxDailyLossUsdt))) {
      return;
    }

    // 3. ACTIVE WAT HOURS CHECK
    if (hour < DEFAULTS.ACTIVE_START_HOUR || hour >= DEFAULTS.ACTIVE_END_HOUR) {
      return;
    }

    // 4. COOLDOWN CHECK
    if (state.cooldownUntil && Date.now() < state.cooldownUntil) {
      return;
    }

    // 5. SET MORNING OR POST-COOLDOWN REFERENCE ANCHOR
    if (!state.referencePrice) {
      state.cooldownUntil = null;
      await setReferencePrice(livePrice);
      return;
    }

    // 6. DIP + BOUNCE BUY TRIGGER
    const dipReached = livePrice <= state.buyTargetPrice;
    const bounceConfirmed = !state.requireBounceTick || prevPrice === null || livePrice > prevPrice;

    if (dipReached && bounceConfirmed) {
      await executeMarketBuy(livePrice);
    } else {
      await saveStateToDb();
    }
  } catch (error) {
    addLog(`ERROR in trading cycle: ${error.message}`);
    await saveStateToDb();
  }
}

// ==========================================
// 8. ROUTES (PUBLIC HEALTH + PROTECTED ADMIN API)
// ==========================================
function requireAdminAuth(req, res, next) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret) {
    return res.status(500).json({ error: 'ADMIN_SECRET is not configured on the server.' });
  }

  const headerSecret = req.headers['x-admin-secret'] || req.headers['x-admin-key'];
  const authHeader = req.headers['authorization'];
  const bearerToken =
    authHeader && authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : null;
  // MERGE 5: Allow ?key= for iPhone Safari - no header needed
  const queryKey = req.query.key || req.query.adminKey;

  const provided = headerSecret || bearerToken || queryKey;
  if (!provided || provided !== secret) {
    return res.status(401).json({ error: 'Unauthorized: invalid or missing admin secret.' });
  }

  next();
}

// MERGE 1: Full public status - for Render, UptimeRobot & quick iPhone check
app.get('/', (req, res) => {
  res.json({
    service: 'SOL/USDT Spot Bot Backend',
    network: USE_BINANCE_TESTNET ? 'BINANCE_TESTNET' : 'BINANCE_LIVE',
    status: state.isRunning ? 'RUNNING' : 'PAUSED',
    dateWAT: state.currentDate,
    lastPrice: state.lastPrice,
    inPosition: state.inPosition,
    referencePrice: state.referencePrice,
    buyTargetPrice: state.buyTargetPrice,
    takeProfitPrice: state.takeProfitPrice,
    stopLossPrice: state.stopLossPrice,
    highestPriceSinceEntry: state.highestPriceSinceEntry,
    trailingActive: state.trailingActive,
    statsToday: {
      trades: `${state.tradesToday}/${DEFAULTS.MAX_TRADES_PER_DAY}`,
      wins: state.winsToday,
      losses: state.lossesToday,
      consecutiveLosses: state.consecutiveLosses,
      dailyPnlUsdt: state.dailyPnlUsdt,
    },
  });
});

// Full dashboard snapshot for Admin Frontend
app.get('/api/admin/dashboard', requireAdminAuth, async (req, res) => {
  try {
    const trades = await getRecentTradesFromDb(50);
    res.json({
      network: USE_BINANCE_TESTNET ? 'BINANCE_TESTNET' : 'BINANCE_LIVE',
      status: state.isRunning ? 'RUNNING' : 'PAUSED',
      config: {
        symbol: DEFAULTS.SYMBOL,
        tradeAmountUsdt: state.tradeAmountUsdt,
        maxDailyLossUsdt: state.maxDailyLossUsdt,
        requireBounceTick: state.requireBounceTick,
        buyDipPercent: DEFAULTS.BUY_DIP_PERCENT,
        trailStartPercent: DEFAULTS.TRAIL_START_PERCENT,
        trailPullbackPercent: DEFAULTS.TRAIL_PULLBACK_PERCENT,
        stopLossPercent: DEFAULTS.STOP_LOSS_PERCENT,
        maxTradesPerDay: DEFAULTS.MAX_TRADES_PER_DAY,
        maxConsecutiveLosses: DEFAULTS.MAX_CONSECUTIVE_LOSSES,
        activeHoursWAT: `${DEFAULTS.ACTIVE_START_HOUR}:00 - ${DEFAULTS.ACTIVE_END_HOUR}:00`,
      },
      market: {
        dateWAT: state.currentDate,
        lastPrice: state.lastPrice,
        rollingAvg: getRollingAverage(state.priceHistory),
        referencePrice: state.referencePrice,
        buyTargetPrice: state.buyTargetPrice,
        cooldownUntil: state.cooldownUntil,
      },
      position: {
        inPosition: state.inPosition,
        entryPrice: state.entryPrice,
        highestPriceSinceEntry: state.highestPriceSinceEntry,
        trailingActive: state.trailingActive,
        takeProfitPrice: state.takeProfitPrice,
        stopLossPrice: state.stopLossPrice,
        purchasedQty: state.purchasedQty,
      },
      statsToday: {
        trades: `${state.tradesToday}/${DEFAULTS.MAX_TRADES_PER_DAY}`,
        wins: state.winsToday,
        losses: state.lossesToday,
        consecutiveLosses: state.consecutiveLosses,
        dailyPnlUsdt: state.dailyPnlUsdt,
      },
      trades,
      logs: state.logs,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Update runtime strategy controls (e.g., increase/decrease tradeAmountUsdt)
app.post('/api/admin/config', requireAdminAuth, async (req, res) => {
  try {
    const { tradeAmountUsdt, maxDailyLossUsdt, requireBounceTick } = req.body || {};

    if (tradeAmountUsdt !== undefined) {
      const amt = Number(tradeAmountUsdt);
      if (!Number.isFinite(amt) || amt < 5) {
        return res.status(400).json({ error: 'tradeAmountUsdt must be a number >= 5.' });
      }
      state.tradeAmountUsdt = Number(amt.toFixed(2));
    }

    if (maxDailyLossUsdt !== undefined) {
      const cap = Number(maxDailyLossUsdt);
      if (!Number.isFinite(cap) || cap <= 0) {
        return res.status(400).json({ error: 'maxDailyLossUsdt must be a positive number.' });
      }
      state.maxDailyLossUsdt = Number(cap.toFixed(2));
    }

    if (requireBounceTick !== undefined) {
      state.requireBounceTick = Boolean(requireBounceTick);
    }

    addLog(
      `Admin updated config: tradeAmountUsdt=$${state.tradeAmountUsdt}, maxDailyLossUsdt=$${state.maxDailyLossUsdt}, requireBounceTick=${state.requireBounceTick}`
    );
    await saveStateToDb();

    res.json({
      message: 'Configuration updated',
      tradeAmountUsdt: state.tradeAmountUsdt,
      maxDailyLossUsdt: state.maxDailyLossUsdt,
      requireBounceTick: state.requireBounceTick,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/pause', requireAdminAuth, async (req, res) => {
  state.isRunning = false;
  addLog('Bot PAUSED via Admin Dashboard.');
  await saveStateToDb();
  res.json({ message: 'Bot paused', isRunning: state.isRunning });
});

app.post('/api/admin/resume', requireAdminAuth, async (req, res) => {
  state.isRunning = true;
  addLog('Bot RESUMED via Admin Dashboard.');
  await saveStateToDb();
  res.json({ message: 'Bot resumed', isRunning: state.isRunning });
});

app.post('/api/admin/reset-anchor', requireAdminAuth, async (req, res) => {
  try {
    if (state.inPosition) {
      return res.status(400).json({ error: 'Cannot reset anchor while holding an open position.' });
    }
    const price = await fetchLivePrice();
    state.lastPrice = price;
    await setReferencePrice(price);
    res.json({
      message: 'Reference price reset',
      referencePrice: state.referencePrice,
      buyTargetPrice: state.buyTargetPrice,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/admin/panic-sell', requireAdminAuth, async (req, res) => {
  try {
    if (!state.inPosition) {
      return res.status(400).json({ error: 'No open position to close.' });
    }
    const price = await fetchLivePrice();
    state.lastPrice = price;
    await executeMarketSell(price, 'MANUAL_PANIC_SELL');
    res.json({ message: 'Emergency sell executed', exitPrice: price });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// 9. STARTUP
// ==========================================
const PORT = process.env.PORT || 3000;

// MERGE 4: Don't crash if Neon hiccups - start anyway with in-memory state
initDatabase()
  .then(() => {
    console.log('Neon DB connected');
  })
  .catch((err) => {
    console.error('Database initialization failed, continuing with in-memory state:', err.message);
  })
  .finally(() => {
    app.listen(PORT, async () => {
      addLog(
        `Server started on port ${PORT} (${
          USE_BINANCE_TESTNET ? 'BINANCE_TESTNET' : 'BINANCE_LIVE'
        }) with Neon DB`
      );
      // FIX BUG 1: sync time before first trade
      await syncBinanceTime();
      runTradingCycle();
      setInterval(runTradingCycle, DEFAULTS.POLL_INTERVAL_MS);
      // FIX 3: re-sync Binance time every hour so -1021 never comes back
      setInterval(syncBinanceTime, 60 * 60 * 1000);
    });
  });
