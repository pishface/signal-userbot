import os
import re
import sqlite3
import asyncio
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from metaapi_cloud_sdk import MetaApi

# === Environment variables ===
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

METAAPI_TOKEN = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

# === Database setup ===
def init_db():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        group_name TEXT,
        symbol TEXT,
        direction TEXT,
        entry TEXT,
        sl TEXT,
        tps TEXT,
        status TEXT,
        error_message TEXT,
        profit REAL
    )''')
    conn.commit()
    conn.close()

def log_signal(group_name, symbol, direction, entry, sl, tps, status, error_message="", profit=0.0):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute('''INSERT INTO signals 
        (timestamp, group_name, symbol, direction, entry, sl, tps, status, error_message, profit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (datetime.utcnow().isoformat(), group_name, symbol, direction, entry, sl, 
         ",".join(tps) if tps else "", status, error_message, profit))
    conn.commit()
    conn.close()

# === MetaAPI helpers ===
async def get_connection():
    api = MetaApi(METAAPI_TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)
    if account.state != 'DEPLOYED':
        await account.deploy()
        await account.wait_connected()
    connection = account.get_rpc_connection()
    await connection.connect()
    await connection.wait_synchronized()
    return connection

async def place_trade(symbol, direction, order_type, entry, sl, tps, volume=0.01):
    connection = await get_connection()
    results = []
    volume_per_tp = max(0.01, round(volume / len(tps), 2)) if tps else 0.01

    try:
        for i, tp in enumerate(tps):
            current_volume = volume_per_tp if i < len(tps) - 1 else round(volume - volume_per_tp * (len(tps) - 1), 2)
            options = {"stop_loss": float(sl), "take_profit": float(tp)}

            if order_type == "MARKET":
                if direction == "BUY":
                    await connection.create_market_buy_order(symbol, current_volume, **options)
                else:
                    await connection.create_market_sell_order(symbol, current_volume, **options)
            elif order_type == "LIMIT":
                if direction == "BUY":
                    await connection.create_limit_buy_order(symbol, current_volume, float(entry), **options)
                else:
                    await connection.create_limit_sell_order(symbol, current_volume, float(entry), **options)

            results.append(f"TP{i+1}:{tp}")

        return True, " | ".join(results)
    except Exception as e:
        return False, str(e)
    finally:
        await connection.close()

# === Signal parser ===
def parse_signal(text):
    text = text.upper().replace(",", ".")
    text = text.replace("GOLD", "XAUUSD")

    # Ignore pure TP HIT messages
    if "HIT" in text and "BUY" not in text and "SELL" not in text:
        return None

    # Direction
    direction = None
    if re.search(r'\bBUY\b', text):
        direction = "BUY"
    elif re.search(r'\bSELL\b', text):
        direction = "SELL"

    if not direction:
        return None

    # Order type
    order_type = "MARKET"
    if "LIMIT" in text:
        order_type = "LIMIT"
    elif "STOP" in text:
        order_type = "STOP"

    # Symbol
    symbols = ["XAUUSD", "XAGUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "BTCUSD", "ETHUSD"]
    symbol = None
    for s in symbols:
        if s in text:
            symbol = s
            break
    if not symbol:
        return None

    # Entry - several possible patterns
    entry = None

    # Pattern 1: GOLD SELL 4120  or  XAUUSD BUY 2650
    entry_match = re.search(rf'{symbol}\s+{direction}\s+([\d.]+)', text)
    if entry_match:
        entry = entry_match.group(1)
    else:
        # Pattern 2: SELL 4120 or BUY @ 4120
        entry_match = re.search(rf'{direction}\s*(?:AT|@)?\s*([\d.]+)', text)
        if entry_match:
            entry = entry_match.group(1)
        else:
            # Pattern 3: classic ENTRY / AT
            entry_match = re.search(r'(?:ENTRY\s*POINT|ENTRY|AT|@|PRICE|NOW)[:\s]*([\d.]+)', text)
            if entry_match:
                entry = entry_match.group(1)

    # Range support (e.g. 4120-4130 or 4120 / 4130)
    range_match = re.search(r'([\d.]+)\s*[-–/]{1,2}\s*([\d.]+)', text)
    if range_match and not entry:
        low = float(range_match.group(1))
        high = float(range_match.group(2))
        if direction == "BUY":
            entry = str(min(low, high))
            order_type = "LIMIT"
        else:
            entry = str(max(low, high))
            order_type = "LIMIT"

    # SL - handles SL:::::4137 and normal SL 4137
    sl = None
    sl_match = re.search(r'(?:SL|STOP\s*LOSS|S/L)[:\s]*([\d.]+)', text)
    if sl_match:
        sl = sl_match.group(1)

    # TPs - handles TP¹ 4117, TP2 4114, TP ⁸4090, etc.
    tps = re.findall(r'(?:TP\s*[¹²³⁴⁵⁶⁷⁸⁹0-9]*|TAKE\s*PROFIT|T/P)[:\s]*([\d.]+)', text)

    # Also catch cases like TP ⁸4090 (number stuck to the symbol)
    if not tps:
        tps = re.findall(r'TP\s*[¹²³⁴⁵⁶⁷⁸⁹0-9]*\s*([\d.]+)', text)

    if direction and symbol and sl and tps:
        # If no clear entry and it's a market-style signal, treat as MARKET
        if not entry:
            order_type = "MARKET"
        return symbol, direction, order_type, entry, sl, tps

    return None

# === Main ===
async def main():
    init_db()
    print("Database ready.")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    print("Userbot is running and listening to groups...")

    @client.on(events.NewMessage(chats=None))
    async def handler(event):
        text = event.message.message
        if not text:
            return

        chat = await event.get_chat()
        group_name = getattr(chat, "title", "Unknown")

        parsed = parse_signal(text)
        if not parsed:
            return

        symbol, direction, order_type, entry, sl, tps = parsed

        print(f"\n=== New signal from: {group_name} ===")
        print(f"{direction} {order_type} {symbol} | Entry: {entry or 'Market'} | SL: {sl} | TPs: {tps}")

        success, result = await place_trade(symbol, direction, order_type, entry, sl, tps)

        if success:
            print(f"✅ Trade placed: {result}")
            log_signal(group_name, symbol, direction, entry, sl, tps, "SUCCESS")
        else:
            print(f"❌ Failed: {result}")
            log_signal(group_name, symbol, direction, entry, sl, tps, "FAILED", error_message=result)

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
