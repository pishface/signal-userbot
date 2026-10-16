import os
import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
TELEGRAM_CODE = os.getenv("TELEGRAM_CODE", "").strip()

async def main():
    if SESSION_STRING:
        print("Using existing SESSION_STRING...")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            print("SESSION_STRING is invalid or expired.")
            return
    else:
        print("No SESSION_STRING found. Starting login process...")
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            await client.send_code_request(PHONE)
            print("Code requested. Waiting for TELEGRAM_CODE...")

            if not TELEGRAM_CODE:
                print("ERROR: TELEGRAM_CODE is empty. Add it and redeploy quickly.")
                return

            try:
                await client.sign_in(PHONE, TELEGRAM_CODE)
                print("Login successful!")
            except Exception as e:
                print(f"Login failed: {e}")
                return

        # Save the permanent session string
        session_str = client.session.save()
        print("\n" + "="*50)
        print("SESSION STRING (COPY THIS ENTIRELY):")
        print(session_str)
        print("="*50 + "\n")

    print("Userbot is running and listening to groups...")

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
