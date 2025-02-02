import os
import sys
import csv
import json
import asyncio
from typing import List, Dict, Tuple
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import Message
from telethon.errors import FloodWaitError

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json

from scripts.utils.database_manager import PostgresManager# StorageInterface  # Unified storage interface
from scripts.utils.telegram_client import TelegramAPI #download_media, create_client, save_session, authenticate_client

logger = setup_logger("scraper")

CONFIG_PATH = os.path.join('..', 'resources', 'configs')
channels_filepath = os.path.join(CONFIG_PATH, 'channels.json')

SESSION_FILE = os.path.join('..', 'fetching-E-commerce-data.session')
DATA_PATH = os.path.join('..', 'resources', 'data')
OUTPUT_DIR = os.path.join(DATA_PATH, 'raw')
MEDIA_DIR = os.path.join(DATA_PATH, 'photos')

# Ensure the data directories exist
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)


class TelegramScraper:
    def __init__(self, api: TelegramAPI, storage: PostgresManager):

        self.api = api
        self.storage = storage  # Storage backend (MongoDB, Postgres, Local JSON/CSV)

    async def fetch_messages(self, channel_username: str, limit: int = 100) -> List[Dict]:
        """Fetch and group messages from a Telegram channel."""
        messages_data = defaultdict(lambda: {
            "Group ID": None,
            "Message IDs": [],
            "Text": None,
            "Message": "",
            "Date": None,
            "Sender ID": None,
            "Media Path": []
        })
        medias = []
        async for message in self.api.client.iter_messages(channel_username, limit=limit):
            group_id = message.grouped_id if message.grouped_id else message.id
            msg_entry = messages_data[group_id]
            msg_entry.update({
                "Group ID": group_id,
                "Message IDs": msg_entry["Message IDs"] + [message.id],
                "Text": msg_entry["Text"] or message.text,
                "Message": msg_entry["Message"] or message.message,
                "Date": msg_entry["Date"] or (message.date.isoformat() if message.date else None),
                "Sender ID": msg_entry["Sender ID"] or message.sender_id,
            })
            if message.media:
                medias.append(message)
                msg_entry["Media Path"].append(None)
        return list(messages_data.values()), medias

    async def process_channel(self, channel: str, media_dir: str, limit: int):
        """Process messages and media from a single channel."""
        try:
            messages, medias = await self.fetch_messages(channel, limit)
            media_paths = await self.api.download_media(medias, os.path.join(media_dir, channel))
            media_map = {media.id: path for media, path in zip(medias, media_paths)}
            for msg in messages:
                msg["Media Path"] = [media_map.get(mid) for mid in msg["Message IDs"] if mid in media_map]
            await self.storage.save_messages(channel, messages)
            logger.info(f"Processed {len(messages)} messages from {channel}")
        except FloodWaitError as e:
            logger.warning(f"Flood wait {e.seconds} sec for {channel}")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error processing {channel}: {e}")

    async def scrape_channels(self, channels: List[str], media_dir: str, limit: int):
        """Process multiple Telegram channels."""
        tasks = [self.process_channel(channel, media_dir, limit) for channel in channels]
        await asyncio.gather(*tasks)

    async def close(self):
        await self.api.client.cleanup()
        await self.api.client.close()

def sync(func):
    """Decorator to run async functions synchronously."""
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper

@sync
def run_fetch_process(channels, output_dir=OUTPUT_DIR, media_dir=MEDIA_DIR, limit=100):
    """Main function to orchestrate fetching and saving messages."""
    async def main():

        # Load credentials from environment variables
        api_id = os.getenv("API_ID")
        api_hash = os.getenv("API_HASH")
        phone_number = os.getenv("PHONE_NUMBER")
        semaphore_limit = int(os.getenv("TELEGRAM_SCRAPER_SEMAPHORE"))
        SESSION_FILE = "telegram_scraper.session"
        STORAGE_BACKEND = "mongo"  # Choose storage: "mongo", "postgres", "json"
        allowed_media = ["photo"]

        # Initialize scraper
        storage = PostgresManager.create_storage(STORAGE_BACKEND)
        api = TelegramAPI(api_id, api_hash, phone_number, semaphore_limit, allowed_media, SESSION_FILE)        

        try:
            await api.authenticate()

            # async with TelegramScraper(api_id, api_hash, phone_number, semaphore_limit, session_file=SESSION_FILE) as scraper:
            scraper = TelegramScraper(api, storage)
            
            # Fetch messages from channels
            await scraper.scrape_channels(CHANNELS, MEDIA_DIR, LIMIT)
                        
        finally:
            # Always disconnect the client
            logger.info("Disconnecting client...")
            
            # Clean up sensitive data
            await api.cleanup()
            await api.close()

    # Run the asynchronous process
    return main()

if __name__ == "__main__":
    
    # CHANNELS = [
    #     "ZemenExpress", "nevacomputer", "meneshayeofficial", "ethio_brand_collection", "Leyueqa",
    #     "sinayelj", "Shewabrand", "helloomarketethiopia", "modernshoppingcenter", "qnashcom",
    #     "Fashiontera", "kuruwear", "gebeyaadama", "MerttEka", "forfreemarket", "classybrands",
    #     "marakibrand", "aradabrand2", "marakisat2", "belaclassic", "AwasMart", "qnashcom"
    # ]
    
    # Load a list of channels from a JSON file to scrape from
    channels = load_json(channels_filepath)
    CHANNELS = channels.get('CHANNELS', [])

    # Run only for selected channels
    LIMIT = 500
    run_fetch_process(channels=CHANNELS[10:15], limit=LIMIT)
