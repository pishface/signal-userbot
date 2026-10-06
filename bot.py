import os
import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# Environment variables
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
SESSION_STRING = os.getenv("SESSION_STRING", "")

# MetaAPI (we will use later)
METAAPI_TOKEN = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

async def main():
    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        client = TelegramClient("session", API_ID, API_HASH)

    await client.start(phone=PHONE)
    print("Userbot is running and listening to groups...")

    @client.on(events.NewMessage(chats=None))  # listens to all groups/channels the account is in
    async def handler(event):
        text = event.message.message
        if not text:
            return

        text_upper = text.upper()

        # Very basic signal detection for now
        if any(word in text_upper for word in ["BUY", "SELL"]) and any(sym in text_upper for sym in ["XAUUSD", "GOLD", "EURUSD", "GBPUSD"]):
            chat = await event.get_chat()
            chat_title = getattr(chat, "title", "Private")
            print(f"\n--- Possible signal from: {chat_title} ---")
            print(text)
            print("----------------------------------------")

            # For now we just log it. Later we will place the trade.

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
