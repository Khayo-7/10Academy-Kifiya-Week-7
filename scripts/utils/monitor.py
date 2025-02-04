import os
import sys
import json
import asyncio
from typing import List, Dict
from telethon import events
from dotenv import load_dotenv
from collections import defaultdict

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json
from scripts.utils.telegram_client import TelegramAPI
from scripts.utils.storage_interface import StorageInterface

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)
logger = setup_logger("scraper")

# Load environment variables
load_dotenv()

CONFIG_PATH = os.path.join('..', 'resources', 'configs')
MEDIA_DIR = os.path.join('..', 'resources', 'downloads')
SESSION_FILE = os.path.join('..', 'fetching-E-commerce-data.session')
channels_filepath = os.path.join(CONFIG_PATH, 'channels.json')

class TelegramMonitor:
    def __init__(self, api: TelegramAPI, storage: StorageInterface, media_dir: str = MEDIA_DIR):
        """
        Initialize the Telegram monitor.

        Args:
            api (TelegramAPI): The Telegram API client.
            storage (StorageInterface): The storage backend for saving data.
            media_dir (str): Directory to save downloaded media files.
        """
        self.api = api
        self.storage = storage
        self.media_dir = media_dir
        self.group_cache = defaultdict(list)
        self.group_timeouts = {}

    async def monitor(self, channels: List[str]):
        """
        Monitor specified Telegram channels for new messages.

        Args:
            channels (List[str]): List of channel usernames to monitor.
        """
        @self.api.client.on(events.NewMessage(chats=channels))
        async def handler(event):
            """Handler for new messages."""
            try:
                await self._process_message(event)
            except Exception as e:
                logger.error(f"Error processing message from {event.chat.username}: {e}")

        logger.info(f"Monitoring channels: {', '.join(channels)}")
        await self.api.client.run_until_disconnected()

    async def _process_message(self, event):
        """
        Process a new message event, including group ID.

        Args:
            event: The new message event.
        """
        group_id = event.message.grouped_id or event.message.id
        self.group_cache[group_id].append(event.message)
        self.group_timeouts[group_id] = asyncio.get_event_loop().time()  # Update last access time

        # Process the group after a delay (5 seconds of inactivity)
        await self._process_group_if_ready(group_id)

    async def _process_group_if_ready(self, group_id):
        """
        Process a group after a delay of inactivity.

        Args:
            group_id: The group ID to process.
        """
        await asyncio.sleep(5)  # Wait for potential grouped messages

        # Check if the group is still inactive
        current_time = asyncio.get_event_loop().time()
        if group_id in self.group_timeouts and current_time - self.group_timeouts[group_id] >= 5:
            messages = self.group_cache.pop(group_id, [])
            self.group_timeouts.pop(group_id, None)

            if messages:
                # Aggregate messages in the group
                aggregated_data = await self._aggregate_messages(messages)
                await self.storage.save_data([aggregated_data])

    async def _aggregate_messages(self, messages: List) -> Dict:
        """
        Aggregate messages in a group into a single data structure.

        Args:
            messages (List): List of messages in the group.

        Returns:
            Dict: Aggregated data for the group.
        """
        group_id = messages[0].grouped_id or messages[0].id
        aggregated_data = {
            "Group ID": group_id,
            "Message IDs": [message.id for message in messages],
            "Message": "\n".join(message.message for message in messages if message.message),
            "Text": "\n".join(message.text for message in messages if message.text),
            "Date": messages[0].date.isoformat(),  # Use the earliest message's date
            "Sender ID": messages[0].sender_id,  # Use the first message's sender
            "Channel": messages[0].chat.username,
            "Media Path": []
        }

        # Download media and save metadata
        for message in messages:
            if message.media:
                try:
                    channel_media_dir = os.path.join(self.media_dir, message.chat.username)
                    os.makedirs(channel_media_dir, exist_ok=True)
                    media_paths = await self.api.download_media([message], channel_media_dir)
                    if media_paths and media_paths[0]:
                        file_id = None
                        try:
                            file_id = self.storage.save_media(
                                media_paths[0],
                                {"group_id": group_id, "message_id": message.id}
                            )
                        except:
                            file_id = media_paths[0]
                        aggregated_data["Media Path"].append(file_id)
                except Exception as e:
                    logger.error(f"Error downloading media from {message.chat.username}: {e}")

        return aggregated_data

    async def close(self):
        """Clean up resources."""
        await self.api.cleanup()
        await self.api.close()
        await self.storage.close()
        logger.info("Monitoring stopped and resources cleaned up.")


async def main(channels_filepath: str, media_dir: str):
    """
    Main function to set up the client and listen for new messages.

    Args:
        channels_filepath (str): Path to the JSON file containing channel usernames.
        media_dir (str): Directory to save downloaded media files.
    """
    try:
        channels = load_json(channels_filepath)
        channel_usernames = channels.get('channels', [])
    except FileNotFoundError:
        logger.error(f"Channels file not found: {channels_filepath}")
        return
    except Exception as e:
        logger.error(f"Error loading channels file: {e}")
        return

    # Initialize Telegram API and storage with required parameters
    storage_type = 'json' # Use storage backend (MongoDB, Postgres, Local JSON/CSV)
    allowed_media = ["photo"]

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
        monitor = TelegramMonitor(api, storage, media_dir=media_dir)
        await monitor.monitor(channel_usernames)
    except Exception as e:
        logger.error(f"Error during monitoring: {e}")
    finally:
        # await monitor.close()
        await api.close()
        # await storage.close()
        logger.info("Monitoring stopped and resources cleaned up.")

if __name__ == '__main__':

    asyncio.run(main(channels_filepath, MEDIA_DIR))