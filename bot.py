import os
import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
SESSION_STRING = os.getenv("SESSION_STRING", "")
TELEGRAM_CODE = os.getenv("TELEGRAM_CODE", "")

METAAPI_TOKEN = os.getenv("API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

async def main():
    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        client = TelegramClient("session", API_ID, API_HASH)

    await client.start(phone=PHONE, code_callback=lambda: TELEGRAM_CODE)
    print("Userbot is running and listening to groups...")

    # Save the session string so we don't need the code next time
    session_str = client.session.save()
    print("\n=== SESSION STRING (save this!) ===")
    print(session_str)
    print("===================================\n")

    @client.on(events.NewMessage(chats=None))
    async def handler(event):
        text = event.message.message
        if not text:
            return

        text_upper = text.upper()

        if any(word in text_upper for word in ["BUY", "SELL"]) and any(sym in text_upper for sym in ["XAUUSD", "GOLD", "EURUSD", "GBPUSD"]):
            chat = await event.get_chat()
            chat_title = getattr(chat, "title", "Private")
            print(f"\n--- Possible signal from: {chat_title} ---")
            print(text)
            print("----------------------------------------")

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
