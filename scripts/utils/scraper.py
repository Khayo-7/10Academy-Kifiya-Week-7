import os
import sys
import csv
import json
import asyncio
from dotenv import load_dotenv
from collections import defaultdict
from typing import List, Dict, Tuple
from telethon.errors import FloodWaitError

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json
from scripts.utils.telegram_client import TelegramAPI
from scripts.utils.storage_interface import StorageInterface

logger = setup_logger("scraper")

# Load environment variables
load_dotenv()

CONFIG_PATH = os.path.join('..', 'resources', 'configs')
LAST_ID_FILE = os.path.join(CONFIG_PATH, 'last_id.json')
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
    def __init__(self, api, storage: str, media_dir: str = MEDIA_DIR):

        self.api = api
        self.storage = storage
        self.media_dir = media_dir

    async def fetch_messages(self, channel: str, limit: int = 100, last_id: int = None) -> Tuple[List[Dict], List]:
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

        async for message in self.api.client.iter_messages(channel, limit=limit):
            
            # if message.id >= last_id:
            #     continue
            
            group_id = message.grouped_id if message.grouped_id else message.id
            msg_entry = messages_data[group_id]
            msg_entry.update({
                "Group ID": group_id,
                # "Message IDs": msg_entry["Message IDs"].append([message.id]),
                "Message IDs": msg_entry["Message IDs"] + [message.id],
                "Text": msg_entry["Text"] or message.text,
                "Message": msg_entry["Message"] or message.message,
                "Date": msg_entry["Date"] or (message.date.isoformat() if message.date else None),
                "Sender ID": msg_entry["Sender ID"] or message.sender_id,
            })

            if message.media:
                medias.append(message)
                msg_entry["Media Path"].append(None)
            
            last_id = message.id

        return list(messages_data.values()), medias, last_id

    async def process_channel(self, channel: str, limit: int, start_from_id: int = None):
        """Process messages and media from a single channel."""
        
        last_id = start_from_id if start_from_id is not None else get_last_id(channel)

        try:
            channel_media_dir = os.path.join(self.media_dir, channel)
            os.makedirs(channel_media_dir, exist_ok=True)
            messages, medias, last_id = await self.fetch_messages(channel, limit, last_id)
            media_paths = await self.api.download_media(medias, channel_media_dir)
            
            # Filter out failed downloads
            valid_media = [(media, path) for media, path in zip(medias, media_paths) if path]
            media_map = {media.id: path for media, path in valid_media}
            
            for msg in messages:
                msg["Media Path"] = [
                    media_map.get(mid) 
                    for mid in msg["Message IDs"]
                    if mid in media_map
                ]
            
            await self.storage.save_data(messages, channel)
            logger.info(f"Processed {len(messages)} messages from {channel}")
        except FloodWaitError as e:
            logger.warning(f"Flood wait {e.seconds} sec for {channel}")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error processing {channel}: {e}")
        finally:
            save_last_id(channel, last_id)

    async def scrape_channels(self, channels: List[str], limit: int):
        """Process multiple Telegram channels."""
        tasks = [self.process_channel(channel, limit) for channel in channels]
        await asyncio.gather(*tasks)

    async def close(self):
        await self.api.cleanup()
        await self.api.close()
        # await self.storage.close()

def get_last_id(channel, filepath=LAST_ID_FILE):
    """Retrieve the last processed ID for a given channel."""
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get(channel, {}).get('last_id', 0)
    except FileNotFoundError:
        logger.warning(f"No last ID file found. Starting from 0.")
        return 0
    except json.JSONDecodeError:
        logger.error("Error decoding JSON from last_id.json. Starting from 0.")
        return 0

def save_last_id(channel, last_id, filepath=LAST_ID_FILE):
    """Save the last processed ID for a given channel."""
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    except json.JSONDecodeError:
        logger.error("Error decoding JSON from last_id.json. Starting with an empty entry.")
        data = {}
    # Update the last_id for the specific channel
    data[channel] = {'last_id': last_id}

    # Write updated data back to the file
    with open(filepath, 'w') as f:
        json.dump(data, f)
        logger.info(f"Saved last processed ID {last_id} for {channel}.")

def sync(func):
    """Decorator to run async functions synchronously."""
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper

@sync
def run_fetch_process(channels, storage_type, allowed_media, media_dir=MEDIA_DIR, limit=100):
    async def main():
        
        storage = await StorageInterface.create_storage(storage_type)
        api = TelegramAPI(
            api_id=os.getenv("API_ID"),
            api_hash=os.getenv("API_HASH"),
            phone_number=os.getenv("PHONE_NUMBER"),
            semaphore_limit=int(os.getenv("TELEGRAM_SCRAPER_SEMAPHORE", '5')),
            allowed_media=allowed_media,
            session_file=SESSION_FILE
        )
        
        try:
            await api.authenticate()
            scraper = TelegramScraper(api, storage, media_dir)
            await scraper.scrape_channels(channels, limit)
        except Exception as e:
            logger.error(f"Error occured while scrapping. {e}")
        finally:
            # await scraper.close()
            await api.cleanup()
            await api.close()
            # await storage.close()
    
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
    CHANNELS = channels.get('channels', [])

    # Initialize Telegram API and storage with required parameters
    LIMIT = 10
    storage_type = 'json' # Use storage backend (MongoDB, Postgres, Local JSON/CSV)
    allowed_media = ["photo"]

    # Run only for selected channels
    run_fetch_process(CHANNELS, storage_type, allowed_media, limit=LIMIT)
