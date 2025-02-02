import os
import asyncio
import json
import csv
import getpass
from collections import defaultdict
from typing import List, Dict, Tuple, Optional, Callable
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from pymongo import MongoClient
from gridfs import GridFS
import sys
from dotenv import load_dotenv
from telethon.tl.types import Message
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, SessionPasswordNeededError

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger

logger = setup_logger("scraper")

# Load environment variables
load_dotenv()

# Constants
MEDIA_DIR = "media"
OUTPUT_DIR = "output"
SESSION_FILE = os.path.join('..', 'fetching-E-commerce-data.session')
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "telegram_data"

class MongoStorage:
    def __init__(self, db_name: str = DB_NAME):
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[db_name]
        self.fs = GridFS(self.db)

    def save_metadata(self, data: dict, collection: str = "messages"):
        self.db[collection].insert_one(data)

    def save_media(self, file_path: str, metadata: dict) -> Optional[str]:
        with open(file_path, "rb") as f:
            file_id = self.fs.put(f, **metadata)
        return str(file_id)

class TelegramScraper:
    def __init__(self, api: TelegramAPI, storage: MongoStorage, media_dir: str = MEDIA_DIR):
        self.api = api
        self.storage = storage
        self.media_dir = media_dir

    async def scrape(self, channels: List[str], limit: int = 100):
        for channel in channels:
            messages, medias = await self._fetch_messages(channel, limit)
            await self._process_media(messages, medias, channel)

    async def _fetch_messages(self, channel: str, limit: int) -> Tuple[List[dict], List[events.NewMessage]]:
        messages_data = []
        medias = []
        async for message in self.api.client.iter_messages(channel, limit=limit):
            msg_data = {
                "message_id": message.id,
                "text": message.text,
                "date": message.date.isoformat(),
                "sender_id": message.sender_id,
                "channel": channel,
                "media_id": None
            }
            if message.media:
                medias.append(message)
            messages_data.append(msg_data)
        return messages_data, medias

    async def _process_media(self, messages: List[dict], medias: List[events.NewMessage], channel: str):
        os.makedirs(self.media_dir, exist_ok=True)
        for message in medias:
            file_path = await message.download_media(self.media_dir)
            if file_path:
                file_id = self.storage.save_media(file_path, {"channel": channel, "message_id": message.id})
                for msg in messages:
                    if msg["message_id"] == message.id:
                        msg["media_id"] = file_id
        for msg in messages:
            self.storage.save_metadata(msg)

class TelegramMonitor:
    def __init__(self, api: TelegramAPI, storage: MongoStorage, media_dir: str = MEDIA_DIR):
        self.api = api
        self.storage = storage
        self.media_dir = media_dir

    async def monitor(self, channels: List[str]):
        @self.api.client.on(events.NewMessage(chats=channels))
        async def handler(event):
            await self._process_message(event)
        await self.api.client.run_until_disconnected()

    async def _process_message(self, event):
        msg_data = {
            "message_id": event.message.id,
            "text": event.message.message or '',
            "date": event.message.date.isoformat(),
            "sender_id": event.message.sender_id,
            "channel": event.chat.username,
            "media_id": None
        }
        if event.message.media:
            file_path = await event.message.download_media(self.media_dir)
            if file_path:
                file_id = self.storage.save_media(file_path, {"channel": event.chat.username, "message_id": event.message.id})
                msg_data["media_id"] = file_id
        self.storage.save_metadata(msg_data)

async def main():
    api = TelegramAPI(
        api_id=os.getenv("API_ID"),
        api_hash=os.getenv("API_HASH"),
        phone_number=os.getenv("PHONE_NUMBER")
    )
    storage = MongoStorage()
    await api.authenticate()
    
    scraper = TelegramScraper(api, storage)
    monitor = TelegramMonitor(api, storage)
    
    channels = ["ZemenExpress"]
    await scraper.scrape(channels, limit=10)
    await monitor.monitor(channels)
    
    await api.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
