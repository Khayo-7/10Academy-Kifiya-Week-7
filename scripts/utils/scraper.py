import os
import sys
import csv
import json
import asyncio
from collections import defaultdict
from typing import List, Dict, Tuple

from telethon import events
from dotenv import load_dotenv
from telethon.errors import FloodWaitError

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json
from scripts.utils.telegram_client import TelegramAPI
from scripts.utils.StorageInterface import StorageInterface

logger = setup_logger("scraper")

# Load environment variables
load_dotenv()

CONFIG_PATH = os.path.join('..', 'resources', 'configs')
channels_filepath = os.path.join(CONFIG_PATH, 'channels.json')

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "telegram_data"
SESSION_FILE = os.path.join('..', 'fetching-E-commerce-data.session')
DATA_PATH = os.path.join('..', 'resources', 'data')
MEDIA_DIR = os.path.join('..', 'resources', 'media')
OUTPUT_DIR = os.path.join(DATA_PATH, 'raw')

# Ensure the data directories exist
os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(MEDIA_DIR, exist_ok=True)

class TelegramScraper:
    def __init__(self, api_id, api_hash, phone_number, semaphore_limit, allowed_media, SESSION_FILE, storage: str):

        self.api = TelegramAPI(
            api_id, api_hash, phone_number, semaphore_limit, allowed_media, SESSION_FILE
        )
        
        self.storage = StorageInterface.create_storage(
            storage,
            uri=os.getenv("MONGO_URI"),
            db_name="telegram_data",
            collection_name="messages"
        )  # Storage backend (MongoDB, Postgres, Local JSON/CSV)

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
            
            # Filter out failed downloads
            valid_media = [(media, path) for media, path in zip(medias, media_paths) if path]
            media_map = {media.id: path for media, path in valid_media}
            
            for msg in messages:
                msg["Media Path"] = [
                    media_map.get(mid) 
                    for mid in msg["Message IDs"] 
                    if mid in media_map
                ]
            
            # Use save_data() instead of save_messages()
            await self.storage.save_data(messages)
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
        await self.api.cleanup()
        await self.api.close()
        # await self.storage.close()

def sync(func):
    """Decorator to run async functions synchronously."""
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper

@sync
def run_fetch_process(channels, output_dir=OUTPUT_DIR, media_dir=MEDIA_DIR, limit=100):
    async def main():
        # Load credentials and configuration
        api_id = os.getenv("API_ID")
        api_hash = os.getenv("API_HASH")
        phone_number = os.getenv("PHONE_NUMBER")
        semaphore_limit = int(os.getenv("TELEGRAM_SCRAPER_SEMAPHORE", '5'))
        
        # Initialize storage with required parameters
        allowed_media = ["photo"]
        storage = "mongo"
        scraper = TelegramScraper(api_id, api_hash, phone_number, semaphore_limit, allowed_media, SESSION_FILE, storage)
        
        try:
            await scraper.api.authenticate()
            await scraper.scrape_channels(channels, media_dir, limit)
        finally:
            await scraper.close()
    
    return main()
    # return asyncio.run(main())

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
    channels = ["ZemenExpress"]

    # Run only for selected channels
    LIMIT = 10
    run_fetch_process(channels, limit=LIMIT)
    # run_fetch_process(channels=CHANNELS[10:15], limit=LIMIT)
