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
  TRADE_AMOUNT_USDT: 5.5,        // Dashboard Total Amount - bot uses EXACTLY this per trade, NOT 99% of balance
  MIN_TRADE_USDT: 5.10,          // Must stay above Binance $5.00 minimum
  BUY_DIP_PERCENT: 0.02,         // -2.0% dip from referencePrice - UNTOUCHED
  TAKE_PROFIT_PERCENT: 0.022,    // UPDATED Sell Fast V3: was 0.05 -> +2.2% hard take-profit fallback
  STOP_LOSS_PERCENT: 0.008,      // UPDATED Sell Fast V3: was 0.03 -> -0.8% hard stop-loss
  TRAIL_START_PERCENT: 0.022,    // UPDATED Sell Fast V3: was 0.025 -> Start trailing once +2.2% above entry
  TRAIL_PULLBACK_PERCENT: 0.005, // UPDATED Sell Fast V3: was 0.012 -> Sell if price pulls back 0.5% from peak
  REQUIRE_BOUNCE_TICK: true,     // Require livePrice > lastPrice on dip trigger
  PRICE_WINDOW_SIZE: 20,         // Rolling price samples for context - 20 x 30s = 10 MIN TREND WINDOW
  FEE_RATE_PER_SIDE: 0.001,      // ~0.1% spot fee estimate per side
  // UPDATED: MAX_TRADES_PER_DAY REMOVED - No limit, keep going 24/7
  MAX_CONSECUTIVE_LOSSES: 4,     // UPDATED Sell Fast V3: was 2 -> 4 so 1 win can cover 2 losses
  // UPDATED: Daily loss is AUTO = TRADE_AMOUNT * 0.08. $5.50 -> $0.44, $100 -> $8.00
  DAILY_LOSS_PERCENT_OF_AMOUNT: 0.08,
  MAX_DAILY_LOSS_USDT: 0.44,     // Auto-calculated from TRADE_AMOUNT_USDT, do not set manually
  COOLDOWN_MINUTES: 5,           // UPDATED Sell Fast V3: was 30 -> 5 min WATCH mode
  POLL_INTERVAL_MS: 30 * 1000,   // UPDATED: was 60s -> 30s
  TIMEZONE: 'Africa/Lagos',
  // REMOVED: ACTIVE_START_HOUR / ACTIVE_END_HOUR - now True 24/7
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

const USE_BINANCE_TESTNET = (process.env.BINANCE_TESTNET ?? process.env.USE_TESTNET ?? 'true') !== 'false';
const BINANCE_BASE_URL = USE_BINANCE_TESTNET
  ? 'https://testnet.binance.vision/api'
  : 'https://api.binance.com/api';

const BINANCE_API_KEY = process.env.BINANCE_API_KEY || '';
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET || '';

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
  const query = new URLSearchParams({
    ...params,
    recvWindow: '5000',
    timestamp: (Date.now() + binanceTimeOffset).toString(),
  }).toString();
  const signature = crypto.createHmac('sha256', BINANCE_API_SECRET).update(query).digest('hex');
  return `${query}&signature=${signature}`;
}

async function binanceSignedRequest(method, path, params = {}) {
  const signedQs = signBinanceParams(params);
  const url = `${BINANCE_BASE_URL}${path}?${signedQs}`;
  const res = await fetch(url, {
    method,
    headers: { 'X-MBX-APIKEY': BINANCE_API_KEY },
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

// NEW: Helper - Daily loss sets itself from Total Amount in dashboard
function calcDailyLossFromAmount(amountUsdt) {
  return Number((amountUsdt * DEFAULTS.DAILY_LOSS_PERCENT_OF_AMOUNT).toFixed(2));
}

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
      // UPDATED: Auto daily loss from saved tradeAmount, never use old saved 0.45 / 3.0
      tradeAmountUsdt: Number(res.rows[0].data.tradeAmountUsdt || DEFAULTS.TRADE_AMOUNT_USDT),
      maxDailyLossUsdt: calcDailyLossFromAmount(Number(res.rows[0].data.tradeAmountUsdt || DEFAULTS.TRADE_AMOUNT_USDT)),
      logs: Array.isArray(res.rows[0].data.logs) ? res.rows[0].data.logs.slice(0, 100) : [],
      priceHistory: Array.isArray(res.rows[0].data.priceHistory)
        ? res.rows[0].data.priceHistory.slice(-DEFAULTS.PRICE_WINDOW_SIZE)
        : [],
    };
  } else {
    state.maxDailyLossUsdt = calcDailyLossFromAmount(state.tradeAmountUsdt);
    await saveStateToDb();
  }
}

async function saveStateToDb() {
  try {
    await db.query(
      `INSERT INTO bot_state (id, data, updated_at) VALUES (1, $1::jsonb, NOW())
       ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = NOW();`,
      [JSON.stringify(state)]
    );
  } catch (err) {
    console.error('Failed saving state to Neon DB:', err.message);
  }
}

async function insertTradeToDb(record) {
  try {
    await db.query(
      `INSERT INTO trade_journal
        (trade_number, trade_date, entry_price, exit_price, qty, pnl_usdt, outcome, reason, closed_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`,
      [record.tradeNumber, record.date, record.entryPrice, record.exitPrice, record.qty, record.pnlUsdt, record.outcome, record.reason, record.closedAt]
    );
  } catch (err) {
    console.error('Failed inserting trade to Neon DB:', err.message);
  }
}

async function getRecentTradesFromDb(limit = 50) {
  const res = await db.query(
    `SELECT id, trade_number AS "tradeNumber", trade_date AS "date", entry_price AS "entryPrice",
      exit_price AS "exitPrice", qty, pnl_usdt AS "pnlUsdt", outcome, reason, closed_at AS "closedAt"
     FROM trade_journal ORDER BY id DESC LIMIT $1;`,
    [limit]
  );
  return res.rows;
}

// NEW: Daily + Weekly report helpers - reads from trade_journal, does not touch trading logic
async function getDailyReport(dateStr) {
  const res = await db.query(
    `SELECT COUNT(*)::int AS trades, COALESCE(SUM(pnl_usdt),0)::float AS total_pnl,
      COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END),0)::int AS wins,
      COALESCE(SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END),0)::int AS losses,
      COALESCE(SUM(CASE WHEN pnl_usdt>0 THEN pnl_usdt ELSE 0 END),0)::float AS gross_win,
      COALESCE(SUM(CASE WHEN pnl_usdt<0 THEN pnl_usdt ELSE 0 END),0)::float AS gross_loss
     FROM trade_journal WHERE trade_date = $1;`,
    [dateStr]
  );
  const r = res.rows[0];
  return {
    date: dateStr,
    trades: r.trades,
    wins: r.wins,
    losses: r.losses,
    winRate: r.trades ? Number(((r.wins / r.trades) * 100).toFixed(1)) : 0,
    totalPnlUsdt: Number(r.total_pnl.toFixed(4)),
    grossWin: Number(r.gross_win.toFixed(4)),
    grossLoss: Number(r.gross_loss.toFixed(4)),
    verdict: r.total_pnl > 0 ? 'WIN DAY' : r.total_pnl < 0 ? 'LOSS DAY' : 'NO TRADES / BREAKEVEN',
  };
}

async function getWeeklyReport() {
  // Last 7 days in Lagos time (trade_date is YYYY-MM-DD)
  const res = await db.query(
    `SELECT trade_date AS date, COUNT(*)::int AS trades,
      COALESCE(SUM(pnl_usdt),0)::float AS total_pnl,
      COALESCE(SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END),0)::int AS wins,
      COALESCE(SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END),0)::int AS losses
     FROM trade_journal
     WHERE trade_date >= TO_CHAR((NOW() AT TIME ZONE 'Africa/Lagos') - INTERVAL '6 days', 'YYYY-MM-DD')
     GROUP BY trade_date ORDER BY trade_date ASC;`
  );
  const days = res.rows.map((d) => ({
    date: d.date,
    trades: d.trades,
    wins: d.wins,
    losses: d.losses,
    totalPnlUsdt: Number(d.total_pnl.toFixed(4)),
  }));
  const totalPnl = days.reduce((a, d) => a + d.totalPnlUsdt, 0);
  const totalTrades = days.reduce((a, d) => a + d.trades, 0);
  const totalWins = days.reduce((a, d) => a + d.wins, 0);
  return {
    period: 'Last 7 Days',
    days,
    summary: {
      totalTrades,
      totalWins,
      totalLosses: totalTrades - totalWins,
      winRate: totalTrades ? Number(((totalWins / totalTrades) * 100).toFixed(1)) : 0,
      totalPnlUsdt: Number(totalPnl.toFixed(4)),
      verdict: totalPnl > 0 ? 'WIN WEEK' : totalPnl < 0 ? 'LOSS WEEK' : 'BREAKEVEN WEEK',
    },
  };
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
  db.query('INSERT INTO bot_logs (message) VALUES ($1)', [entry]).catch(() => {});
}

function getLagosTimeInfo() {
  const now = new Date();
  const dateStr = now.toLocaleDateString('en-CA', { timeZone: DEFAULTS.TIMEZONE });
  const hourStr = now.toLocaleTimeString('en-GB', { timeZone: DEFAULTS.TIMEZONE, hour: '2-digit', hour12: false });
  return { dateStr, hour: parseInt(hourStr, 10) };
}

function getRollingAverage(prices) {
  if (!prices.length) return null;
  const sum = prices.reduce((acc, p) => acc + p, 0);
  return Number((sum / prices.length).toFixed(4));
}

async function resetDailyStats(newDateStr) {
  // NEW: Before wiping, log yesterday's total win/loss report
  if (state.currentDate) {
    try {
      const y = await getDailyReport(state.currentDate);
      addLog(`DAILY REPORT ${y.date} | Trades: ${y.trades} | Wins: ${y.wins} | Losses: ${y.losses} | Total PnL: $${y.totalPnlUsdt} | ${y.verdict}`);
    } catch (e) { /* report is extra, never block reset */ }
  }
  state.currentDate = newDateStr;
  state.referencePrice = null;
  state.buyTargetPrice = null;
  state.tradesToday = 0;
  state.consecutiveLosses = 0;
  state.winsToday = 0;
  state.lossesToday = 0;
  state.dailyPnlUsdt = 0;
  state.cooldownUntil = null;
  // UPDATED: Daily loss resets daily and is auto-calculated from dashboard Total Amount
  state.maxDailyLossUsdt = calcDailyLossFromAmount(state.tradeAmountUsdt);
  addLog(`New trading day initialized (${newDateStr}). Daily counters reset. Total Amount: $${state.tradeAmountUsdt} | Auto Daily Loss Limit: $${state.maxDailyLossUsdt}`);
  await saveStateToDb();
}

async function setReferencePrice(price) {
  state.referencePrice = price;
  state.buyTargetPrice = Number((price * (1 - DEFAULTS.BUY_DIP_PERCENT)).toFixed(4));
  addLog(`Reference price set to $${price.toFixed(4)} | Buy Target (-2%): $${state.buyTargetPrice}`);
  await saveStateToDb();
}

// NEW: Update trailing reference upward only - never down, only when not in position
async function updateTrailingReference(livePrice) {
  if (state.inPosition) return;
  if (!state.referencePrice) return;
  if (livePrice > state.referencePrice) {
    state.referencePrice = livePrice;
    state.buyTargetPrice = Number((livePrice * (1 - DEFAULTS.BUY_DIP_PERCENT)).toFixed(4));
    addLog(`Trailing UP: Reference $${state.referencePrice.toFixed(4)} | New Buy Target: $${state.buyTargetPrice}`);
    await saveStateToDb();
  }
}

// CHANGED 2026-11-25: TREND FILTER = 10-MIN INTRADAY, NOT 5-DAY DAILY.
// Uses priceHistory (20 polls x 30s = 10 min). Excludes current tick so livePrice is not compared to itself.
// If <5 samples (just started / after restart), allow buy - don't block on no data.
// Old 5-day klines code REMOVED - it blocked for days when SOL was $68 vs $70.32 avg.
async function isUptrend(livePrice) {
  try {
    const history = (state.priceHistory || []).slice(0, -1).slice(-DEFAULTS.PRICE_WINDOW_SIZE);
    if (history.length < 5) return true;
    const avg = history.reduce((a, b) => a + b, 0) / history.length;
    const up = livePrice > avg;
    if (!up) addLog(`TREND FILTER BLOCKED BUY: Price $${livePrice.toFixed(4)} is BELOW 10m avg $${avg.toFixed(4)} (downtrend last 10m) - Waiting`);
    return up;
  } catch (err) {
    addLog(`Trend check failed (${err.message}) - allowing buy to keep old behaviour`);
    return true;
  }
}

// ==========================================
// 6. BINANCE EXCHANGE FUNCTIONS
// ==========================================
async function fetchLivePrice() {
  const data = await binancePublicRequest('/v3/ticker/price', { symbol: DEFAULTS.SYMBOL });
  const price = parseFloat(data.price);
  if (!Number.isFinite(price) || price <= 0) throw new Error('Ticker fetch failed: invalid price');
  return price;
}

async function getSolAvailableBalance() {
  const data = await binanceSignedRequest('GET', '/v3/account');
  const assetInfo = Array.isArray(data.balances) ? data.balances.find((b) => b.asset === 'SOL') : null;
  return assetInfo ? parseFloat(assetInfo.free) : 0;
}

// NEW: Dynamic USDT - real free balance before every BUY
async function getUsdtAvailableBalance() {
  const data = await binanceSignedRequest('GET', '/v3/account');
  const assetInfo = Array.isArray(data.balances) ? data.balances.find((b) => b.asset === 'USDT') : null;
  return assetInfo ? parseFloat(assetInfo.free) : 0;
}

async function executeMarketBuy(currentPrice) {
  // UPDATED 2026-11-23: HYBRID AMOUNT - Fixed + 99% cut.
  // Dashboard amount is the TARGET. Actual buy = min(Dashboard, Free x 0.99).
  // So if balance drops after a loss, bot buys a little smaller and NEVER deadlocks.
  // Dashboard amount (state.tradeAmountUsdt) is NOT overwritten, so Auto Daily Loss stays correct.
  const dashboardAmount = Number(state.tradeAmountUsdt || DEFAULTS.TRADE_AMOUNT_USDT);
  const freeUsdt = await getUsdtAvailableBalance();
  const amountUsdt = Number(Math.min(dashboardAmount, freeUsdt * 0.99).toFixed(2));
  if (amountUsdt < DEFAULTS.MIN_TRADE_USDT) {
    throw new Error(`Trade amount $${amountUsdt.toFixed(2)} < minimum $${DEFAULTS.MIN_TRADE_USDT}. Increase Total Amount in dashboard or top up USDT. Free: $${freeUsdt.toFixed(2)} | Dashboard: $${dashboardAmount.toFixed(2)}`);
  }

  addLog(`Dip + bounce + 10m UPTREND confirmed! Buying $${amountUsdt.toFixed(2)} of ${DEFAULTS.SYMBOL} @ ~$${currentPrice.toFixed(4)} (Dashboard: $${dashboardAmount.toFixed(2)} | Free USDT: $${freeUsdt.toFixed(2)})...`);

  const solBalBefore = await getSolAvailableBalance();

  // NEW: Capture Binance order receipt to get ACTUAL fill price
  const order = await binanceSignedRequest('POST', '/v3/order', {
    symbol: DEFAULTS.SYMBOL,
    side: 'BUY',
    type: 'MARKET',
    quoteOrderQty: amountUsdt.toFixed(2),
  });

  await new Promise((r) => setTimeout(r, 2000));
  const solBalAfter = await getSolAvailableBalance();
  const sellableQty = Math.floor((solBalAfter - solBalBefore) * 1000) / 1000;
  if (sellableQty <= 0) throw new Error('Buy settled with 0 sellable SOL quantity (check minimum precision/balance).');

  // NEW: Actual fill = cummulativeQuoteQty / executedQty, fallback to ticker if Binance returns 0
  let actualEntryPrice = currentPrice;
  const execQty = parseFloat(order.executedQty || '0');
  const cumQuote = parseFloat(order.cummulativeQuoteQty || '0');
  if (execQty > 0 && cumQuote > 0) actualEntryPrice = cumQuote / execQty;

  state.inPosition = true;
  state.entryPrice = Number(actualEntryPrice.toFixed(4));
  state.highestPriceSinceEntry = state.entryPrice;
  state.trailingActive = false;
  state.purchasedQty = sellableQty;
  state.takeProfitPrice = Number((state.entryPrice * (1 + DEFAULTS.TAKE_PROFIT_PERCENT)).toFixed(4));
  state.stopLossPrice = Number((state.entryPrice * (1 - DEFAULTS.STOP_LOSS_PERCENT)).toFixed(4));

  addLog(`BOUGHT ${sellableQty} SOL @ ACTUAL $${state.entryPrice.toFixed(4)} (ticker was $${currentPrice.toFixed(4)}) | Trail Start (+${(DEFAULTS.TRAIL_START_PERCENT * 100).toFixed(1)}%) | Hard TP: $${state.takeProfitPrice} | SL: $${state.stopLossPrice}`);
  await saveStateToDb();
}

async function executeMarketSell(currentPrice, reason) {
  addLog(`Closing position (${reason}) at ~$${currentPrice.toFixed(4)}...`);
  const solBal = await getSolAvailableBalance();
  const qtyToSell = Math.floor(Math.min(state.purchasedQty || 0, solBal) * 1000) / 1000;
  if (qtyToSell <= 0) {
    addLog('Warning: No SOL balance found to sell. Resetting position state.');
    state.inPosition = false; state.entryPrice = null; state.highestPriceSinceEntry = null;
    state.trailingActive = false; state.takeProfitPrice = null; state.stopLossPrice = null; state.purchasedQty = 0;
    await saveStateToDb();
    return;
  }

  const order = await binanceSignedRequest('POST', '/v3/order', {
    symbol: DEFAULTS.SYMBOL, side: 'SELL', type: 'MARKET', quantity: qtyToSell.toString(),
  });

  // NEW: Actual sell fill price from receipt
  let actualExitPrice = currentPrice;
  const execQty = parseFloat(order.executedQty || '0');
  const cumQuote = parseFloat(order.cummulativeQuoteQty || '0');
  if (execQty > 0 && cumQuote > 0) actualExitPrice = cumQuote / execQty;
  actualExitPrice = Number(actualExitPrice.toFixed(4));

  const entryRef = state.entryPrice || actualExitPrice;
  const grossPnl = (actualExitPrice - entryRef) * qtyToSell;
  const estimatedFees = (entryRef * qtyToSell + actualExitPrice * qtyToSell) * DEFAULTS.FEE_RATE_PER_SIDE;
  const netPnl = Number((grossPnl - estimatedFees).toFixed(4));
  const isWin = netPnl >= 0;

  state.tradesToday += 1;
  state.dailyPnlUsdt = Number((state.dailyPnlUsdt + netPnl).toFixed(4));
  if (isWin) { state.winsToday += 1; state.consecutiveLosses = 0; }
  else { state.lossesToday += 1; state.consecutiveLosses += 1; }

  const record = {
    tradeNumber: state.tradesToday, date: state.currentDate, entryPrice: entryRef,
    exitPrice: actualExitPrice, qty: qtyToSell, pnlUsdt: netPnl,
    outcome: isWin ? 'WIN' : 'LOSS', reason, closedAt: new Date().toISOString(),
  };
  await insertTradeToDb(record);
  addLog(`SOLD ${qtyToSell} SOL @ ACTUAL $${actualExitPrice.toFixed(4)} | ${record.outcome} ($${netPnl} net) | Daily PnL: $${state.dailyPnlUsdt}`);

  state.inPosition = false; state.entryPrice = null; state.highestPriceSinceEntry = null;
  state.trailingActive = false; state.takeProfitPrice = null; state.stopLossPrice = null; state.purchasedQty = 0;
  // UPDATED Sell Fast V3: DO NOT wipe reference. Keep it at sell price so cooldown WATCH mode can keep trailing it up.
  // Old code set referencePrice = null here, which made bot blind during cooldown.
  state.referencePrice = actualExitPrice;
  state.buyTargetPrice = Number((actualExitPrice * (1 - DEFAULTS.BUY_DIP_PERCENT)).toFixed(4));
  state.cooldownUntil = Date.now() + DEFAULTS.COOLDOWN_MINUTES * 60 * 1000;
  addLog(`Cooldown WATCH active for ${DEFAULTS.COOLDOWN_MINUTES}m. Reference kept at $${state.referencePrice} | Buy Target: $${state.buyTargetPrice} | Watching, not buying.`);
  await saveStateToDb();
}
// ==========================================
// 7. MAIN TRADING ENGINE LOOP
// ==========================================
async function runTradingCycle() {
  if (!state.isRunning) return;
  try {
    const { dateStr } = getLagosTimeInfo();
    if (state.currentDate !== dateStr) await resetDailyStats(dateStr);

    const prevPrice = state.lastPrice;
    const livePrice = await fetchLivePrice();
    state.lastPrice = livePrice;
    state.priceHistory.push(livePrice);
    if (state.priceHistory.length > DEFAULTS.PRICE_WINDOW_SIZE) state.priceHistory.shift();

    // 1. OPEN POSITION MANAGEMENT (Trailing TP + Hard TP + Hard SL) - UNTOUCHED LOGIC, only numbers changed in DEFAULTS
    if (state.inPosition && state.entryPrice) {
      if (state.highestPriceSinceEntry === null || livePrice > state.highestPriceSinceEntry) {
        state.highestPriceSinceEntry = livePrice;
      }
      const trailActivationPrice = state.entryPrice * (1 + DEFAULTS.TRAIL_START_PERCENT);
      if (livePrice >= trailActivationPrice) state.trailingActive = true;
      const trailingStopPrice = state.trailingActive
        ? Number((state.highestPriceSinceEntry * (1 - DEFAULTS.TRAIL_PULLBACK_PERCENT)).toFixed(4))
        : null;
      if (trailingStopPrice !== null && livePrice <= trailingStopPrice) {
        await executeMarketSell(livePrice, `TRAILING_TP (peak $${state.highestPriceSinceEntry.toFixed(4)}, pull -${(DEFAULTS.TRAIL_PULLBACK_PERCENT * 100).toFixed(1)}%)`);
      } else if (livePrice >= state.takeProfitPrice) {
        await executeMarketSell(livePrice, 'HARD_TAKE_PROFIT (+2.2%)');
      } else if (livePrice <= state.stopLossPrice) {
        await executeMarketSell(livePrice, 'STOP_LOSS (-0.8%)');
      } else {
        const trailInfo = trailingStopPrice ? ` | Trail Stop: $${trailingStopPrice}` : ` | Trail Starts @ $${trailActivationPrice.toFixed(4)}`;
        addLog(`Holding SOL | Now: $${livePrice.toFixed(4)} | Peak: $${state.highestPriceSinceEntry.toFixed(4)}${trailInfo} | Hard SL: $${state.stopLossPrice}`);
      }
      await saveStateToDb();
      return;
    }

    // 2. DAILY CIRCUIT BREAKERS - UPDATED: NO MAX TRADES PER DAY, 24/7
    // REMOVED: if (state.tradesToday >= MAX_TRADES_PER_DAY) return;
    if (state.consecutiveLosses >= DEFAULTS.MAX_CONSECUTIVE_LOSSES) return; // Sell Fast V3: now 4
    if (state.dailyPnlUsdt <= -Math.abs(Number(state.maxDailyLossUsdt))) return; // auto from dashboard Total Amount

    // 3. REMOVED: ACTIVE WAT HOURS CHECK - Now True 24/7

    // 4. COOLDOWN CHECK - UPDATED Sell Fast V3: WATCH MODE, not blind sleep
    if (state.cooldownUntil && Date.now() < state.cooldownUntil) {
      // WATCH ONLY: follow price up, update reference, but DO NOT BUY
      if (!state.referencePrice) {
        state.referencePrice = livePrice;
        state.buyTargetPrice = Number((livePrice * (1 - DEFAULTS.BUY_DIP_PERCENT)).toFixed(4));
      } else {
        await updateTrailingReference(livePrice);
      }
      const minsLeft = Math.max(0, Math.ceil((state.cooldownUntil - Date.now()) / 60000));
      addLog(`COOLDOWN WATCHING | Price $${livePrice.toFixed(4)} | Reference $${state.referencePrice.toFixed(4)} | Buy Target $${state.buyTargetPrice} | ${minsLeft}m left - watching, not buying`);
      await saveStateToDb();
      return;
    }

    // 5. SET MORNING OR POST-COOLDOWN REFERENCE ANCHOR - UNTOUCHED
    if (!state.referencePrice) {
      state.cooldownUntil = null;
      await setReferencePrice(livePrice);
      return;
    }

    // 5b. NEW: TRAILING REFERENCE UPWARD - only when not in position
    await updateTrailingReference(livePrice);

    // 6. DIP + BOUNCE BUY TRIGGER - UNTOUCHED LOGIC + NEW 10m TREND FILTER CHECK FIRST
    const dipReached = livePrice <= state.buyTargetPrice;
    const bounceConfirmed = !state.requireBounceTick || prevPrice === null || livePrice > prevPrice;
    if (dipReached && bounceConfirmed) {
      // CHANGED: Don't buy in downtrend - 10m intraday check, not 5-day
      const uptrend = await isUptrend(livePrice);
      if (!uptrend) {
        await saveStateToDb();
        return;
      }
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
// 8. ROUTES
// ==========================================
function requireAdminAuth(req, res, next) {
  const secret = process.env.ADMIN_SECRET;
  if (!secret) return res.status(500).json({ error: 'ADMIN_SECRET is not configured on the server.' });
  const headerSecret = req.headers['x-admin-secret'] || req.headers['x-admin-key'];
  const authHeader = req.headers['authorization'];
  const bearerToken = authHeader && authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : null;
  const queryKey = req.query.key || req.query.adminKey;
  const provided = headerSecret || bearerToken || queryKey;
  if (!provided || provided !== secret) return res.status(401).json({ error: 'Unauthorized: invalid or missing admin secret.' });
  next();
}

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
      trades: state.tradesToday, // UPDATED: No daily limit, shows total count only
      wins: state.winsToday, losses: state.lossesToday,
      consecutiveLosses: state.consecutiveLosses, dailyPnlUsdt: state.dailyPnlUsdt,
    },
  });
});

app.get('/api/admin/dashboard', requireAdminAuth, async (req, res) => {
  try {
    const trades = await getRecentTradesFromDb(50);
    const dailyReport = state.currentDate ? await getDailyReport(state.currentDate) : null;
    const weeklyReport = await getWeeklyReport();
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
        maxTradesPerDay: 'UNLIMITED - 24/7',
        maxConsecutiveLosses: DEFAULTS.MAX_CONSECUTIVE_LOSSES,
        activeHoursWAT: '24/7 - No Restriction',
      },
      market: {
        dateWAT: state.currentDate, lastPrice: state.lastPrice,
        rollingAvg: getRollingAverage(state.priceHistory),
        referencePrice: state.referencePrice, buyTargetPrice: state.buyTargetPrice,
        cooldownUntil: state.cooldownUntil,
      },
      position: {
        inPosition: state.inPosition, entryPrice: state.entryPrice,
        highestPriceSinceEntry: state.highestPriceSinceEntry, trailingActive: state.trailingActive,
        takeProfitPrice: state.takeProfitPrice, stopLossPrice: state.stopLossPrice, purchasedQty: state.purchasedQty,
      },
      statsToday: {
        trades: state.tradesToday,
        wins: state.winsToday, losses: state.lossesToday,
        consecutiveLosses: state.consecutiveLosses, dailyPnlUsdt: state.dailyPnlUsdt,
      },
      dailyReport,
      weeklyReport,
      trades,
      logs: state.logs,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// NEW: Daily report endpoint - total win/loss for a day
app.get('/api/admin/report/daily', requireAdminAuth, async (req, res) => {
  try {
    const date = req.query.date || state.currentDate || getLagosTimeInfo().dateStr;
    const report = await getDailyReport(date);
    res.json(report);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// NEW: Weekly report endpoint - total win/loss last 7 days
app.get('/api/admin/report/weekly', requireAdminAuth, async (req, res) => {
  try {
    const report = await getWeeklyReport();
    res.json(report);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/config', requireAdminAuth, async (req, res) => {
  try {
    const { tradeAmountUsdt, maxDailyLossUsdt, requireBounceTick } = req.body || {};
    if (tradeAmountUsdt !== undefined) {
      const amt = Number(tradeAmountUsdt);
      if (!Number.isFinite(amt) || amt < 5) return res.status(400).json({ error: 'tradeAmountUsdt must be a number >= 5.' });
      state.tradeAmountUsdt = Number(amt.toFixed(2));
      // UPDATED: Daily loss sets itself from Total Amount - you don't set it manually anymore
      state.maxDailyLossUsdt = calcDailyLossFromAmount(state.tradeAmountUsdt);
    }
    // UPDATED: maxDailyLossUsdt in request is IGNORED - it is auto-calculated. Kept for backward compat, does not override.
    if (requireBounceTick !== undefined) state.requireBounceTick = Boolean(requireBounceTick);
    addLog(`Admin updated config: tradeAmountUsdt=$${state.tradeAmountUsdt}, AUTO maxDailyLossUsdt=$${state.maxDailyLossUsdt}, requireBounceTick=${state.requireBounceTick}`);
    await saveStateToDb();
    res.json({ message: 'Configuration updated', tradeAmountUsdt: state.tradeAmountUsdt, maxDailyLossUsdt: state.maxDailyLossUsdt, requireBounceTick: state.requireBounceTick });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/pause', requireAdminAuth, async (req, res) => {
  state.isRunning = false; addLog('Bot PAUSED via Admin Dashboard.'); await saveStateToDb();
  res.json({ message: 'Bot paused', isRunning: state.isRunning });
});

app.post('/api/admin/resume', requireAdminAuth, async (req, res) => {
  state.isRunning = true; addLog('Bot RESUMED via Admin Dashboard.'); await saveStateToDb();
  res.json({ message: 'Bot resumed', isRunning: state.isRunning });
});

app.post('/api/admin/reset-anchor', requireAdminAuth, async (req, res) => {
  try {
    if (state.inPosition) return res.status(400).json({ error: 'Cannot reset anchor while holding an open position.' });
    const price = await fetchLivePrice(); state.lastPrice = price; await setReferencePrice(price);
    res.json({ message: 'Reference price reset', referencePrice: state.referencePrice, buyTargetPrice: state.buyTargetPrice });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/panic-sell', requireAdminAuth, async (req, res) => {
  try {
    if (!state.inPosition) return res.status(400).json({ error: 'No open position to close.' });
    const price = await fetchLivePrice(); state.lastPrice = price; await executeMarketSell(price, 'MANUAL_PANIC_SELL');
    res.json({ message: 'Emergency sell executed', exitPrice: price });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ==========================================
// 9. STARTUP
// ==========================================
const PORT = process.env.PORT || 3000;
initDatabase()
  .then(() => console.log('Neon DB connected'))
  .catch((err) => console.error('Database initialization failed, continuing with in-memory state:', err.message))
  .finally(() => {
    app.listen(PORT, async () => {
      addLog(`Server started on port ${PORT} (${USE_BINANCE_TESTNET ? 'BINANCE_TESTNET' : 'BINANCE_LIVE'}) with Neon DB`);
      await syncBinanceTime();
      runTradingCycle();
      setInterval(runTradingCycle, DEFAULTS.POLL_INTERVAL_MS);
      setInterval(syncBinanceTime, 60 * 60 * 1000);
    });
  });
