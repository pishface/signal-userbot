import os
import re
import sqlite3
import asyncio
import logging
from contextlib import closing
from datetime import datetime, timezone

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from metaapi_cloud_sdk import MetaApi

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("signal-userbot")

# ================== CONFIG ==================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
METAAPI_TOKEN = os.getenv("API_KEY") or os.getenv("METAAPI_TOKEN")
ACCOUNT_ID = os.environ["ACCOUNT_ID"]

LOT_SIZE = float(os.getenv("LOT_SIZE", "0.01"))                      # per TP position
BE_BUFFER_PIPS = float(os.getenv("BE_BUFFER_PIPS", "5"))
MAX_DAILY_LOSS_PERCENT = float(os.getenv("MAX_DAILY_LOSS_PERCENT", "0.5"))   # stop new trades
KILL_SWITCH_PERCENT = float(os.getenv("KILL_SWITCH_PERCENT", "10"))        # close all + pause

# Railway disk is ephemeral: mount a volume (e.g. /data) and set DB_PATH=/data/trades.db
DB_PATH = os.getenv("DB_PATH", "trades.db")
# Comma-separated Telegram chat IDs to trade from (e.g. -1001234567890). Empty = all groups/channels.
ALLOWED_CHATS = {x.strip() for x in os.getenv("ALLOWED_CHATS", "").split(",") if x.strip()}
# Broker symbol suffix, e.g. "m" or ".r" if your broker uses XAUUSDm
SYMBOL_SUFFIX = os.getenv("SYMBOL_SUFFIX", "")
# Close ALL XAUUSD on any generic "close now" message (not a reply). Risky, off by default.
ALLOW_GENERIC_CLOSE = os.getenv("ALLOW_GENERIC_CLOSE", "0") == "1"
# Your personal Telegram user ID: DM commands from it are accepted and alerts are sent to it.
OWNER_ID = int(os.getenv("OWNER_ID", "0")) or None

# ================== STATE ==================
PAUSED = False           # manual pause (/pause)
KILL_SWITCH = False      # hard stop until /resume
risk_paused = False      # daily-loss pause, auto-clears next UTC day
day_start_equity = None
current_day = None
tg = None                # Telegram client
me_id = None

# ================== DATABASE ==================
def run_sql(sql, args=(), fetch=None):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            cur = conn.execute(sql, args)
            if fetch == "all":
                return cur.fetchall()
            if fetch == "one":
                return cur.fetchone()
            return cur.lastrowid


def init_db():
    run_sql('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        group_name TEXT,
        chat_id TEXT,
        message_id INTEGER,
        symbol TEXT,
        direction TEXT,
        entry REAL,
        sl REAL,
        tp1 REAL,
        tp2 REAL,
        tps TEXT,
        status TEXT,
        stage INTEGER DEFAULT 0,
        error_message TEXT,
        profit REAL DEFAULT 0
    )''')
    run_sql('''CREATE TABLE IF NOT EXISTS daily (
        day TEXT PRIMARY KEY,
        start_balance REAL,
        end_balance REAL
    )''')
    # migrate older DBs
    cols = [r[1] for r in run_sql("PRAGMA table_info(signals)", fetch="all")]
    if "chat_id" not in cols:
        run_sql("ALTER TABLE signals ADD COLUMN chat_id TEXT")


def insert_signal(group_name, chat_id, message_id, symbol, direction, entry, sl, tps):
    tp1 = float(tps[0]) if tps else None
    tp2 = float(tps[1]) if len(tps) > 1 else None
    return run_sql(
        '''INSERT INTO signals
           (timestamp, group_name, chat_id, message_id, symbol, direction, entry, sl,
            tp1, tp2, tps, status, stage, error_message, profit)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, '', 0)''',
        (datetime.now(timezone.utc).isoformat(), group_name, chat_id, message_id, symbol,
         direction, float(entry) if entry else None, float(sl) if sl else None,
         tp1, tp2, ",".join(tps)))


def set_status(sig_id, status, error=""):
    run_sql("UPDATE signals SET status=?, error_message=? WHERE id=?", (status, error, sig_id))


def update_stage(sig_id, stage):
    run_sql("UPDATE signals SET stage=? WHERE id=?", (stage, sig_id))


def get_signal_id(chat_id, message_id):
    row = run_sql("SELECT id FROM signals WHERE chat_id=? AND message_id=? ORDER BY id DESC LIMIT 1",
                  (chat_id, message_id), "one")
    return row[0] if row else None


def get_active_trades():
    return run_sql("""SELECT id, symbol, direction, tp1, tp2, stage, timestamp
                      FROM signals WHERE status IN ('SUCCESS','PARTIAL') AND stage < 2""", fetch="all")


def get_group_stats():
    return run_sql('''SELECT group_name, COUNT(*),
                             SUM(CASE WHEN status IN ('SUCCESS','PARTIAL','DONE') THEN 1 ELSE 0 END),
                             SUM(profit)
                      FROM signals WHERE direction IN ('BUY','SELL')
                      GROUP BY group_name''', fetch="all")


def format_stats():
    msg = "📈 Group Stats:\n\n"
    for group, total, success, profit in get_group_stats():
        msg += f"{group}\n  Placed: {success}/{total}\n  Profit: {profit or 0:.2f}\n\n"
    return msg


# ================== HELPERS ==================
def pip_size(symbol):
    s = symbol.upper()
    if s.startswith("XAU"):
        return 0.1
    if s.startswith("XAG"):
        return 0.01
    if s.startswith(("BTC", "ETH")):
        return 1.0
    if s.endswith("JPY"):
        return 0.01
    return 0.0001


async def notify(msg):
    log.info(msg)
    for target in ([OWNER_ID] if OWNER_ID else []) + ["me"]:
        try:
            await tg.send_message(target, msg)
            return
        except Exception as e:
            log.warning("notify to %s failed: %s", target, e)


# ================== BROKER ==================
class Broker:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None
        self._lock = asyncio.Lock()

    async def connect(self):
        if self.connection:
            return self.connection
        async with self._lock:
            if self.connection:
                return self.connection
            self.api = MetaApi(METAAPI_TOKEN)
            self.account = await self.api.metatrader_account_api.get_account(ACCOUNT_ID)
            if self.account.state != "DEPLOYED":
                await self.account.deploy()
            await self.account.wait_connected()
            conn = self.account.get_rpc_connection()
            await conn.connect()
            await conn.wait_synchronized()
            self.connection = conn
            return conn

    async def get_account_info(self):
        c = await self.connect()
        return await c.get_account_information()

    async def get_balance(self):
        return float((await self.get_account_info()).get("balance", 0))

    async def get_equity(self):
        info = await self.get_account_info()
        return float(info.get("equity") or info.get("balance", 0))

    @staticmethod
    def _tagged(items, sig_id):
        prefix = f"S{sig_id}T"
        return [x for x in items if str(x.get("comment", "")).startswith(prefix)]

    async def place(self, sig_id, symbol, direction, order_type, entry, sl, tps):
        """One position per TP, each tagged with comment S<id>T<n>. Returns (ok_list, error_list)."""
        c = await self.connect()
        sym = symbol + SYMBOL_SUFFIX
        vol = round(LOT_SIZE, 2)
        ok, errs = [], []
        buy = direction == "BUY"
        for i, tp in enumerate(tps, start=1):
            kw = dict(stop_loss=float(sl), take_profit=float(tp), options={"comment": f"S{sig_id}T{i}"})
            try:
                if order_type == "MARKET":
                    fn = c.create_market_buy_order if buy else c.create_market_sell_order
                    await fn(sym, vol, **kw)
                elif order_type == "LIMIT":
                    fn = c.create_limit_buy_order if buy else c.create_limit_sell_order
                    await fn(sym, vol, float(entry), **kw)
                else:  # STOP
                    fn = c.create_stop_buy_order if buy else c.create_stop_sell_order
                    await fn(sym, vol, float(entry), **kw)
                ok.append(f"TP{i}:{tp}")
            except Exception as e:
                errs.append(f"TP{i}: {e}")
        return ok, errs

    async def close_signal(self, sig_id):
        c = await self.connect()
        pos = self._tagged(await c.get_positions(), sig_id)
        orders = self._tagged(await c.get_orders(), sig_id)
        for p in pos:
            await c.close_position(p["id"])
        for o in orders:
            await c.cancel_order(o["id"])
        return len(pos), len(orders)

    async def close_symbol(self, symbol):
        c = await self.connect()
        sym = (symbol + SYMBOL_SUFFIX).upper()
        pos = [p for p in await c.get_positions() if p["symbol"].upper() == sym]
        orders = [o for o in await c.get_orders() if o["symbol"].upper() == sym]
        for p in pos:
            await c.close_position(p["id"])
        for o in orders:
            await c.cancel_order(o["id"])
        return len(pos), len(orders)

    async def close_all(self):
        try:
            c = await self.connect()
            pos = await c.get_positions()
            orders = await c.get_orders()
            for p in pos:
                await c.close_position(p["id"])
            for o in orders:
                await c.cancel_order(o["id"])
            return True, f"Closed {len(pos)} positions, cancelled {len(orders)} orders"
        except Exception as e:
            return False, str(e)

    async def move_sl(self, sig_id, direction, sl_price=None, be_buffer=0.0):
        """Move SL on every open position of a signal. Never loosens an existing SL."""
        c = await self.connect()
        handled = 0
        for p in self._tagged(await c.get_positions(), sig_id):
            if sl_price is not None:
                new_sl = float(sl_price)
            else:
                op = float(p["openPrice"])
                new_sl = op + be_buffer if direction == "BUY" else op - be_buffer
            cur = p.get("stopLoss")
            if cur and ((direction == "BUY" and float(cur) >= new_sl) or
                        (direction == "SELL" and float(cur) <= new_sl)):
                handled += 1
                continue
            kw = {"stop_loss": new_sl}
            if p.get("takeProfit"):
                kw["take_profit"] = float(p["takeProfit"])
            await c.modify_position(p["id"], **kw)
            handled += 1
        return handled

    async def price(self, symbol):
        c = await self.connect()
        return await c.get_symbol_price(symbol + SYMBOL_SUFFIX)


broker = Broker()

# ================== PARSER ==================
NUM = r"(\d+(?:\.\d+)?)"
NOISE = r"(?:\s*(?:LIMIT|STOP|NOW|AT|@|:))*"
SYMBOLS = ["XAUUSD", "XAGUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF",
           "AUDUSD", "USDCAD", "NZDUSD", "BTCUSD", "ETHUSD"]


def parse_signal(text):
    t = text.upper().replace(",", ".")
    t = re.sub(r"\bGOLD\b", "XAUUSD", t)

    has_dir = re.search(r"\b(BUY|SELL)\b", t)
    if re.search(r"\bHIT\b", t) and not has_dir:
        return None
    if not has_dir:
        return None
    direction = "BUY" if re.search(r"\bBUY\b", t) else "SELL"

    order_type = "MARKET"
    if re.search(r"\bLIMIT\b", t):
        order_type = "LIMIT"
    elif re.search(r"\b(?:BUY|SELL)\s+STOP\b", t):
        order_type = "STOP"

    symbol = next((s for s in SYMBOLS if s in t), None)
    if not symbol:
        return None

    entry = None
    head = rf"\b{direction}{NOISE}(?:\s+{symbol}{NOISE})?\s*"
    m = re.search(head + NUM + r"\s*(?:-|–|—|/|TO)\s*" + NUM, t)
    if m:  # entry zone -> limit at the better edge
        lo, hi = sorted((float(m.group(1)), float(m.group(2))))
        entry = str(lo if direction == "BUY" else hi)
        order_type = "LIMIT"
    else:
        m = re.search(head + NUM, t)
        if m:
            entry = m.group(1)
        else:
            m = re.search(NUM + r"\s*(?:-|–|—|/|TO)\s*" + NUM, t)
            if m:
                lo, hi = sorted((float(m.group(1)), float(m.group(2))))
                entry = str(lo if direction == "BUY" else hi)
                order_type = "LIMIT"

    m = re.search(r"\b(?:SL|STOP\s*LOSS|S/L)\b[:\s@=\-]*" + NUM, t)
    sl = m.group(1) if m else None

    # "TP1 2660", "TP1: 2660", "TP 2660", "TP2 - 1.0850" all supported
    tps = re.findall(r"\bTP(?:\s*[¹²³⁴⁵⁶⁷⁸⁹]|\s*\d(?![\d.]))?\s*[:@\-=]?\s*" + NUM, t)
    if not tps:
        tps = re.findall(r"TAKE\s*PROFIT[:\s@=\-]*" + NUM, t)

    if not (sl and tps):
        return None

    # de-dupe + order TPs from nearest to furthest
    tps = sorted(set(tps), key=float, reverse=(direction == "SELL"))
    sl_f = float(sl)

    # sanity checks: wrong-sided SL/TP means a misparse
    if direction == "BUY" and not all(float(x) > sl_f for x in tps):
        log.warning("Rejected signal: BUY with TP below SL (%s)", text[:80])
        return None
    if direction == "SELL" and not all(float(x) < sl_f for x in tps):
        log.warning("Rejected signal: SELL with TP above SL (%s)", text[:80])
        return None
    if order_type in ("LIMIT", "STOP") and not entry:
        log.warning("Rejected signal: %s without entry price (%s)", order_type, text[:80])
        return None
    if entry:
        e = float(entry)
        if (direction == "BUY" and sl_f >= e) or (direction == "SELL" and sl_f <= e):
            log.warning("Rejected signal: SL on wrong side of entry (%s)", text[:80])
            return None

    return symbol, direction, order_type, entry, sl, tps


# ================== RISK ==================
async def ensure_baseline():
    global day_start_equity, current_day, risk_paused
    today = datetime.now(timezone.utc).date().isoformat()
    if today != current_day:
        current_day = today
        risk_paused = False
        day_start_equity = None
    if day_start_equity is None:
        row = run_sql("SELECT start_balance FROM daily WHERE day=?", (today,), "one")
        if row and row[0]:
            day_start_equity = float(row[0])
        else:
            eq = await broker.get_equity()
            run_sql("INSERT OR REPLACE INTO daily (day, start_balance) VALUES (?, ?)", (today, eq))
            day_start_equity = eq


async def loss_percent():
    await ensure_baseline()
    eq = await broker.get_equity()
    loss = ((day_start_equity - eq) / day_start_equity * 100) if day_start_equity else 0.0
    return loss, eq


async def check_risk():
    """Returns True if new trades are allowed."""
    global KILL_SWITCH, risk_paused
    try:
        loss, _ = await loss_percent()
    except Exception as e:
        log.error("Risk check error (blocking new trades): %s", e)
        return False

    if loss >= KILL_SWITCH_PERCENT and not KILL_SWITCH:
        KILL_SWITCH = True
        _, res = await broker.close_all()
        await notify(f"🛑 KILL SWITCH – equity down {loss:.2f}% today. {res}. Send /resume to re-enable.")
    elif loss >= MAX_DAILY_LOSS_PERCENT and not risk_paused and not KILL_SWITCH:
        risk_paused = True
        await notify(f"⚠️ Daily loss limit hit – down {loss:.2f}%. No new trades until tomorrow (UTC) or /resume.")
    return not (KILL_SWITCH or risk_paused)


async def risk_loop():
    while True:
        await check_risk()
        await asyncio.sleep(60)


# ================== STAGED SL ==================
def _age_seconds(ts):
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return 9999


async def staged_sl_loop():
    while True:
        try:
            trades = get_active_trades()
            if trades:
                c = await broker.connect()
                positions = await c.get_positions()
                orders = await c.get_orders()

                for sig_id, symbol, direction, tp1, tp2, stage, ts in trades:
                    live = Broker._tagged(positions, sig_id) + Broker._tagged(orders, sig_id)
                    if not live:
                        if _age_seconds(ts) > 120:
                            set_status(sig_id, "DONE")
                        continue
                    if not tp1:
                        continue

                    px = await broker.price(symbol)
                    cur = float(px["bid"] if direction == "BUY" else px["ask"])
                    be_buffer = BE_BUFFER_PIPS * pip_size(symbol)

                    if stage == 0:
                        if (direction == "BUY" and cur >= tp1) or (direction == "SELL" and cur <= tp1):
                            if await broker.move_sl(sig_id, direction, be_buffer=be_buffer):
                                update_stage(sig_id, 1)
                                log.info("Stage 1: %s #%s SL -> breakeven", symbol, sig_id)
                    elif stage == 1 and tp2:
                        if (direction == "BUY" and cur >= tp2) or (direction == "SELL" and cur <= tp2):
                            if await broker.move_sl(sig_id, direction, sl_price=tp1):
                                update_stage(sig_id, 2)
                                log.info("Stage 2: %s #%s SL -> TP1 (%s)", symbol, sig_id, tp1)
        except Exception as e:
            log.error("Staged SL error: %s", e)
        await asyncio.sleep(30)


# ================== DAILY SUMMARY ==================
async def daily_summary_loop():
    last_sent = None
    while True:
        now = datetime.now(timezone.utc)
        if now.hour >= 21 and last_sent != now.date():
            try:
                balance = await broker.get_balance()
                msg = f"📊 Daily Summary\nBalance: {balance:.2f}\n\n"
                for group, total, success, profit in get_group_stats():
                    msg += f"{group}: {success}/{total} | Profit: {profit or 0:.2f}\n"
                await notify(msg)
                last_sent = now.date()
            except Exception as e:
                log.error("Daily summary error: %s", e)
        await asyncio.sleep(60)


# ================== COMMANDS (Saved Messages only) ==================
async def handle_command(event, text):
    global PAUSED, KILL_SWITCH, risk_paused
    cmd = text.strip().lower()
    if cmd == "/pause":
        PAUSED = True
        await event.reply("⏸ Bot paused")
    elif cmd == "/resume":
        PAUSED = False
        KILL_SWITCH = False
        risk_paused = False
        await event.reply("▶️ Bot resumed, kill switch and daily limit cleared")
    elif cmd == "/status":
        status = "KILL SWITCH" if KILL_SWITCH else ("LOSS LIMIT" if risk_paused else ("PAUSED" if PAUSED else "RUNNING"))
        loss, eq = await loss_percent()
        await event.reply(f"Status: {status}\nEquity: {eq:.2f}\nToday: {-loss:+.2f}%")
    elif cmd == "/stats":
        await event.reply(format_stats())
    elif cmd == "/closeall":
        _, res = await broker.close_all()
        await event.reply(res)


# ================== MAIN ==================
async def main():
    global tg, me_id, PAUSED, KILL_SWITCH
    init_db()
    log.info("Database ready (%s)", DB_PATH)

    if not SESSION_STRING:
        raise SystemExit("SESSION_STRING is empty - generate one locally and set it in Railway variables.")

    tg = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await tg.connect()
    if not await tg.is_user_authorized():
        raise SystemExit("SESSION_STRING is invalid or expired - generate a new one.")
    me = await tg.get_me()
    me_id = me.id
    log.info("Userbot running as %s", me.username or me.id)
    if not ALLOWED_CHATS:
        log.warning("ALLOWED_CHATS is empty: trading from EVERY group/channel you are in!")

    await ensure_baseline()
    log.info("Day-start equity: %s", day_start_equity)

    tasks = [  # keep 
