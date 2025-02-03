import os
import sys
import json
import asyncio
from typing import List
from telethon import events
from dotenv import load_dotenv

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json
from scripts.utils.StorageInterface import StorageInterface
from scripts.utils.telegram_client import TelegramAPI

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)
logger = setup_logger("scraper")

# Load environment variables
load_dotenv()

CONFIG_PATH = os.path.join('..', 'resources', 'configs')
MEDIA_DIR = os.path.join('..', 'resources', 'downloads')
SESSION_FILENAME = 'monitoring-E-commerce-channels.session'
channels_filepath = os.path.join(CONFIG_PATH, 'channels.json')

class TelegramMonitor:
    def __init__(self, api: TelegramAPI, storage: StorageInterface, media_dir: str = MEDIA_DIR):
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

async def main(channels_filepath, media_dir):
    """Main function to set up the client and listen for new messages."""

    # Load channel usernames from a JSON file
    channels = load_json(channels_filepath)
    channel_usernames = channels.get('CHANNELS', [])
    channel_usernames = ["ZemenExpress"]

    api = TelegramAPI(
        api_id=os.getenv("API_ID"),
        api_hash=os.getenv("API_HASH"),
        phone_number=os.getenv("PHONE_NUMBER")
    )
    storage = MongoStorage()
    await api.authenticate()
    
    monitor = TelegramMonitor(api, storage)
    
    await monitor.monitor(channel_usernames)
    
    await api.cleanup()

if __name__ == '__main__':

    asyncio.run(main(channels_filepath, MEDIA_DIR))