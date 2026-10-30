import os
import re
import sqlite3
import asyncio
from datetime import datetime, date
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from metaapi_cloud_sdk import MetaApi

# ================== CONFIG ==================
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
METAAPI_TOKEN = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

LOT_SIZE = float(os.getenv("LOT_SIZE", "0.01"))
BE_BUFFER_PIPS = float(os.getenv("BE_BUFFER_PIPS", "5"))
MAX_DAILY_LOSS_PERCENT = float(os.getenv("MAX_DAILY_LOSS_PERCENT", "5"))  # stop new trades
KILL_SWITCH_PERCENT = float(os.getenv("KILL_SWITCH_PERCENT", "10"))       # close all + pause

# ================== STATE ==================
PAUSED = False
KILL_SWITCH = False
day_start_balance = None

# ================== DATABASE ==================
def init_db():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        group_name TEXT,
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
    c.execute('''CREATE TABLE IF NOT EXISTS daily (
        day TEXT PRIMARY KEY,
        start_balance REAL,
        end_balance REAL
    )''')
    conn.commit()
    conn.close()

def log_signal(group_name, message_id, symbol, direction, entry, sl, tps, status, error="", profit=0.0):
    tp1 = float(tps[0]) if tps else None
    tp2 = float(tps[1]) if len(tps) > 1 else None
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute('''INSERT INTO signals 
        (timestamp, group_name, message_id, symbol, direction, entry, sl, tp1, tp2, tps, status, stage, error_message, profit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)''',
        (datetime.utcnow().isoformat(), group_name, message_id, symbol, direction,
         float(entry) if entry else None, float(sl) if sl else None,
         tp1, tp2, ",".join(tps) if tps else "", status, error, profit))
    conn.commit()
    conn.close()

def get_active_trades():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("SELECT id, symbol, direction, entry, tp1, tp2, stage FROM signals WHERE status='SUCCESS' AND stage < 2")
    rows = c.fetchall()
    conn.close()
    return rows

def update_stage(trade_id, stage):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("UPDATE signals SET stage=? WHERE id=?", (stage, trade_id))
    conn.commit()
    conn.close()

def get_signal_by_message_id(message_id):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("SELECT symbol FROM signals WHERE message_id=? AND status='SUCCESS' ORDER BY id DESC LIMIT 1", (message_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def get_group_stats():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute('''SELECT group_name, 
                        COUNT(*) as total,
                        SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) as success,
                        SUM(profit) as total_profit
                 FROM signals 
                 WHERE status IN ('SUCCESS', 'CLOSED', 'FAILED')
                 GROUP BY group_name''')
    rows = c.fetchall()
    conn.close()
    return rows

# ================== BROKER ==================
class Broker:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None

    async def connect(self):
        if self.connection:
            return self.connection
        self.api = MetaApi(METAAPI_TOKEN)
        self.account = await self.api.metatrader_account_api.get_account(ACCOUNT_ID)
        if self.account.state != 'DEPLOYED':
            await self.account.deploy()
            await self.account.wait_connected()
        self.connection = self.account.get_rpc_connection()
        await self.connection.connect()
        await self.connection.wait_synchronized()
        return self.connection

    async def get_balance(self):
        connection = await self.connect()
        account_info = await connection.get_account_information()
        return float(account_info.get("balance", 0))

    async def place(self, symbol, direction, order_type, entry, sl, tps):
        connection = await self.connect()
        results = []
        n = len(tps)
        if n == 0:
            return False, "No TPs"

        vol = round(LOT_SIZE, 2)

        try:
            for i, tp in enumerate(tps):
                opts = {"stop_loss": float(sl), "take_profit": float(tp)}
                if order_type == "MARKET":
                    if direction == "BUY":
                        await connection.create_market_buy_order(symbol, vol, **opts)
                    else:
                        await connection.create_market_sell_order(symbol, vol, **opts)
                else:
                    if direction == "BUY":
                        await connection.create_limit_buy_order(symbol, vol, float(entry), **opts)
                    else:
                        await connection.create_limit_sell_order(symbol, vol, float(entry), **opts)
                results.append(f"TP{i+1}:{tp}")
            return True, " | ".join(results)
        except Exception as e:
            return False, str(e)

    async def close_symbol(self, symbol):
        connection = await self.connect()
        try:
            positions = await connection.get_positions()
            closed = 0
            for pos in positions:
                if pos["symbol"].upper() == symbol.upper():
                    await connection.close_position(pos["id"])
                    closed += 1
            return (True, f"Closed {closed} positions") if closed else (False, "No positions")
        except Exception as e:
            return False, str(e)

    async def close_all(self):
        connection = await self.connect()
        try:
            positions = await connection.get_positions()
            for pos in positions:
                await connection.close_position(pos["id"])
            return True, f"Closed {len(positions)} positions"
        except Exception as e:
            return False, str(e)

    async def modify_sl(self, symbol, entry, new_sl):
        connection = await self.connect()
        try:
            positions = await connection.get_positions()
            count = 0
            for pos in positions:
                if pos["symbol"].upper() == symbol.upper() and abs(float(pos["openPrice"]) - float(entry)) < 1.5:
                    await connection.modify_position(pos["id"], stop_loss=float(new_sl))
                    count += 1
            return count
        except Exception as e:
            print("Modify SL error:", e)
            return 0

broker = Broker()

# ================== PARSER ==================
def parse_signal(text):
    text = text.upper().replace(",", ".")
    text = text.replace("GOLD", "XAUUSD")

    if "HIT" in text and "BUY" not in text and "SELL" not in text:
        return None

    direction = None
    if re.search(r'\bBUY\b', text):
        direction = "BUY"
    elif re.search(r'\bSELL\b', text):
        direction = "SELL"
    if not direction:
        return None

    order_type = "MARKET"
    if re.search(r'\bLIMIT\b', text):
        order_type = "LIMIT"
    elif re.search(r'\bBUY STOP\b|\bSELL STOP\b', text):
        order_type = "STOP"

    symbols = ["XAUUSD", "XAGUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "BTCUSD", "ETHUSD"]
    symbol = next((s for s in symbols if s in text), None)
    if not symbol:
        return None

    entry = None
    m = re.search(rf'{symbol}\s+{direction}\s+([\d.]+)', text)
    if not m:
        m = re.search(rf'{direction}\s+([\d.]+)', text)
    if m:
        entry = m.group(1)

    if not entry:
        m = re.search(r'([\d.]+)\s*[-–/]{1,2}\s*([\d.]+)', text)
        if m:
            low, high = float(m.group(1)), float(m.group(2))
            entry = str(min(low, high) if direction == "BUY" else max(low, high))
            order_type = "LIMIT"

    sl = None
    m = re.search(r'(?:SL|STOP\s*LOSS|S/L)[:\s]*([\d.]+)', text)
    if m:
        sl = m.group(1)

    tps = re.findall(r'TP\s*[¹²³⁴⁵⁶⁷⁸⁹0-9]*\s*([\d.]+)', text)
    if not tps:
        tps = re.findall(r'(?:TP\d*|TAKE\s*PROFIT)[:\s]*([\d.]+)', text)

    if direction and symbol and sl and tps:
        return symbol, direction, order_type, entry, sl, tps
    return None

# ================== RISK CHECK ==================
async def check_risk():
    global PAUSED, KILL_SWITCH, day_start_balance

    try:
        balance = await broker.get_balance()

        if day_start_balance is None:
            day_start_balance = balance
            return True

        loss_percent = ((day_start_balance - balance) / day_start_balance) * 100

        if loss_percent >= KILL_SWITCH_PERCENT:
            KILL_SWITCH = True
            PAUSED = True
            await broker.close_all()
            print(f"KILL SWITCH triggered – down {loss_percent:.2f}%")
            return False

        if loss_percent >= MAX_DAILY_LOSS_PERCENT:
            PAUSED = True
            print(f"Daily loss limit reached – down {loss_percent:.2f}%")
            return False

        return True
    except Exception as e:
        print("Risk check error:", e)
        return True

# ================== STAGED SL ==================
async def staged_sl_loop():
    while True:
        try:
            trades = get_active_trades()
            if trades:
                connection = await broker.connect()
                positions = await connection.get_positions()

                for trade_id, symbol, direction, entry, tp1, tp2, stage in trades:
                    if not entry or not tp1:
                        continue

                    current = None
                    for pos in positions:
                        if pos["symbol"].upper() == symbol.upper() and abs(float(pos["openPrice"]) - float(entry)) < 1.5:
                            current = float(pos.get("currentPrice") or pos.get("current_price", 0))
                            break
                    if current is None:
                        continue

                    if stage == 0:
                        hit = (direction == "BUY" and current >= tp1) or (direction == "SELL" and current <= tp1)
                        if hit:
                            be = entry + (BE_BUFFER_PIPS * 0.1) if direction == "BUY" else entry - (BE_BUFFER_PIPS * 0.1)
                            if await broker.modify_sl(symbol, entry, be):
                                update_stage(trade_id, 1)
                                print(f"✅ Stage 1: {symbol} → SL to BE ({be})")

                    elif stage == 1 and tp2:
                        hit = (direction == "BUY" and current >= tp2) or (direction == "SELL" and current <= tp2)
                        if hit:
                            if await broker.modify_sl(symbol, entry, tp1):
                                update_stage(trade_id, 2)
                                print(f"✅ Stage 2: {symbol} → SL to TP1 ({tp1})")
        except Exception as e:
            print("Staged SL error:", e)

        await asyncio.sleep(30)

# ================== DAILY SUMMARY ==================
async def daily_summary_loop(client):
    while True:
        now = datetime.utcnow()
        # Run around 21:00 UTC
        if now.hour == 21 and now.minute < 2:
            try:
                stats = get_group_stats()
                balance = await broker.get_balance()
                msg = f"📊 Daily Summary\nBalance: {balance:.2f}\n\n"
                for group, total, success, profit in stats:
                    msg += f"{group}: {success}/{total} | Profit: {profit or 0:.2f}\n"
                await client.send_message("me", msg)
            except Exception as e:
                print("Daily summary error:", e)
            await asyncio.sleep(120)
        await asyncio.sleep(30)

# ================== MAIN ==================
async def main():
    global PAUSED, KILL_SWITCH, day_start_balance
    init_db()
    print("Database ready")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    print("Userbot running – listening to all groups...")

    day_start_balance = await broker.get_balance()
    print(f"Starting balance today: {day_start_balance}")

    asyncio.create_task(staged_sl_loop())
    asyncio.create_task(daily_summary_loop(client))
    print("Staged SL + Daily summary started")

    @client.on(events.NewMessage(chats=None))
    async def handler(event):
        global PAUSED, KILL_SWITCH
        text = event.message.message
        if not text:
            return

        # ===== Commands =====
        if event.is_private:
            cmd = text.strip().lower()
            if cmd == "/pause":
                PAUSED = True
                await event.reply("⏸ Bot paused")
                return
            elif cmd == "/resume":
                PAUSED = False
                KILL_SWITCH = False
                await event.reply("▶️ Bot resumed + kill switch cleared")
                return
            elif cmd == "/status":
                status = "KILL SWITCH" if KILL_SWITCH else ("PAUSED" if PAUSED else "RUNNING")
                bal = await broker.get_balance()
                await event.reply(f"Status: {status}\nBalance: {bal:.2f}")
                return
            elif cmd == "/stats":
                stats = get_group_stats()
                msg = "📈 Group Stats:\n\n"
                for group, total, success, profit in stats:
                    msg += f"{group}\n  Trades: {success}/{total}\n  Profit: {profit or 0:.2f}\n\n"
                await event.reply(msg)
                return

        if PAUSED or KILL_SWITCH:
            return

        # Risk check before placing new trades
        if not await check_risk():
            return

        chat = await event.get_chat()
        group_name = getattr(chat, "title", "Unknown")
        message_id = event.message.id
        text_upper = text.upper()

        # ---------- CLOSE ----------
        is_close = "CLOSE" in text_upper and any(w in text_upper for w in ["PIP", "TRADE", "HERE", "SCALPER", "NOW", "HALF"])

        if event.message.is_reply and "CLOSE" in text_upper:
            print(f"\n=== CLOSE (reply) from {group_name} ===")
            symbol = get_signal_by_message_id(event.message.reply_to_msg_id) or "XAUUSD"
            success, result = await broker.close_symbol(symbol)
            print(("✅" if success else "❌"), result)
            log_signal(group_name, message_id, symbol, "CLOSE", None, None, [], "CLOSED" if success else "CLOSE_FAILED", result)
            return

        if is_close:
            print(f"\n=== CLOSE from {group_name} ===")
            success, result = await broker.close_symbol("XAUUSD")
            print(("✅" if success else "❌"), result)
            log_signal(group_name, message_id, "XAUUSD", "CLOSE", None, None, [], "CLOSED" if success else "CLOSE_FAILED", result)
            return

        # ---------- ENTRY ----------
        parsed = parse_signal(text)
        if not parsed:
            return

        symbol, direction, order_type, entry, sl, tps = parsed

        print(f"\n=== SIGNAL from {group_name} ===")
        print(f"{direction} {order_type} {symbol} | Entry: {entry or 'Market'} | SL: {sl} | TPs: {tps}")

        success, result = await broker.place(symbol, direction, order_type, entry, sl, tps)

        if success:
            print("✅", result)
            log_signal(group_name, message_id, symbol, direction, entry, sl, tps, "SUCCESS")
        else:
            print("❌", result)
            log_signal(group_name, message_id, symbol, direction, entry, sl, tps, "FAILED", result)

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())nt.start()
    print("Userbot running – listening to all groups...")

    asyncio.create_task(staged_sl_loop())
    print("Staged SL protector started")

    @client.on(events.NewMessage(chats=None))
    async def handler(event):
        global PAUSED
        text = event.message.message
        if not text:
            return

        # ===== Commands (only from Saved Messages / yourself) =====
        if event.is_private:
            cmd = text.strip().lower()
            if cmd == "/pause":
                PAUSED = True
                await event.reply("⏸ Bot paused – no new trades will be opened")
                return
            elif cmd == "/resume":
                PAUSED = False
                await event.reply("▶️ Bot resumed")
                return
            elif cmd == "/status":
                status = "⏸ PAUSED" if PAUSED else "▶️ RUNNING"
                await event.reply(f"Bot status: {status}")
                return

        if PAUSED:
            return

        chat = await event.get_chat()
        group_name = getattr(chat, "title", "Unknown")
        message_id = event.message.id
        text_upper = text.upper()

        # ---------- CLOSE ----------
        is_close = "CLOSE" in text_upper and any(w in text_upper for w in ["PIP", "TRADE", "HERE", "SCALPER", "NOW", "HALF"])

        if event.message.is_reply and "CLOSE" in text_upper:
            print(f"\n=== CLOSE (reply) from {group_name} ===")
            original_symbol = get_signal_by_message_id(event.message.reply_to_msg_id)
            symbol = original_symbol or "XAUUSD"
            success, result = await broker.close_symbol(symbol)
            print(("✅" if success else "❌"), result)
            log_signal(group_name, message_id, symbol, "CLOSE", None, None, [], "CLOSED" if success else "CLOSE_FAILED", result)
            return

        if is_close:
            print(f"\n=== CLOSE from {group_name} ===")
            success, result = await broker.close_symbol("XAUUSD")
            print(("✅" if success else "❌"), result)
            log_signal(group_name, message_id, "XAUUSD", "CLOSE", None, None, [], "CLOSED" if success else "CLOSE_FAILED", result)
            return

        # ---------- ENTRY ----------
        parsed = parse_signal(text)
        if not parsed:
            return

        symbol, direction, order_type, entry, sl, tps = parsed

        print(f"\n=== SIGNAL from {group_name} ===")
        print(f"{direction} {order_type} {symbol} | Entry: {entry or 'Market'} | SL: {sl} | TPs: {tps}")

        success, result = await broker.place(symbol, direction, order_type, entry, sl, tps)

        if success:
            print("✅", result)
            log_signal(group_name, message_id, symbol, direction, entry, sl, tps, "SUCCESS")
        else:
            print("❌", result)
            log_signal(group_name, message_id, symbol, direction, entry, sl, tps, "FAILED", result)

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
