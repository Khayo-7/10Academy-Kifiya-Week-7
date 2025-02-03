import os
import sys
import getpass
import asyncio
from functools import wraps
from dotenv import load_dotenv
from typing import List, Dict, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json

logger = setup_logger("scraper")

# Load environment variables
load_dotenv()

def download_concurrently(func):
    """Decorator to download concurrently."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        tasks = await func(*args, **kwargs)
        return await asyncio.gather(*tasks)
        return await asyncio.gather(*tasks, return_exceptions=True)
    return wrapper

class TelegramAPI:
    def __init__(self, api_id: str, api_hash: str, phone_number: str, semaphore_limit: int, allowed_media: List[str], session_file: str = "telegram.session"):
        """
        Initialize the Telegram API manager.
        
        Args:
            api_id (str): Telegram API ID.
            api_hash (str): Telegram API hash.
            phone_number (str): User's phone number for authentication.
            semaphore_limit (int): Maximum concurrent downloads.
            allowed_media (List[str]): Allowed media types (photo, video, document, etc.).
            session_file (str): Path to the session file.
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.session_file = session_file
        self.semaphore = asyncio.Semaphore(semaphore_limit)
        self.allowed_media = set(allowed_media)
        self.client = self._create_client()
    
    def _create_client(self) -> TelegramClient:
        """Create and return a Telegram client."""
        if os.path.exists(self.session_file):
            # with open(self.session_file, 'rb') as f:
            with open(self.session_file, 'r') as f:
                session_str = f.read().strip()
            return TelegramClient(StringSession(session_str), self.api_id, self.api_hash)
        return TelegramClient(StringSession(), self.api_id, self.api_hash)
    
    async def save_session(self) -> None:
        """Save the current session to a file."""
        with open(self.session_file, 'w') as f:
            f.write(await self.client.session.save())
    
    async def authenticate(self) -> None:
        """Authenticate the Telegram client."""
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logger.info("Logging in...")
            try:
                await self._handle_authentication()
            except FloodWaitError as e:
                logger.warning(f"FloodWaitError: Wait {e.seconds} seconds before retrying.")
                raise
            except Exception as e:
                logger.error(f"Login error: {e}")
                raise
    
    async def _handle_authentication(self) -> None:
        try:
            await self.client.send_code_request(self.phone_number)
            code = input("Enter Telegram auth code: ")
            await self.client.sign_in(self.phone_number, code)
        except SessionPasswordNeededError:
            password = getpass.getpass("Enter 2FA password: ")
            await self.client.sign_in(password=password)
        
        await self.save_session()
    
    @download_concurrently
    async def download_media(self, medias: List[Message], media_dir: str) -> List[Optional[str]]:
        """
        Download media from messages while respecting the semaphore limit.

        Args:
            medias (List[Message]): List of Telegram messages containing media.
            media_dir (str): Directory to save downloaded media.

        Returns:
            List[Optional[str]]: List of file paths for downloaded media (or None for failed downloads).
        """
        os.makedirs(media_dir, exist_ok=True)
        
        async def download(message: Message) -> Optional[str]:
            """Download a single media file."""
            try:
                if isinstance(message.media, MessageMediaPhoto) and "photo" in self.allowed_media:
                    file_ext = "jpg"
                elif isinstance(message.media, MessageMediaDocument) and "document" in self.allowed_media:
                    file_ext = message.media.document.mime_type.split('/')[-1] if message.media.document.mime_type else "bin"
                else:
                    return None
                
                filename = f"{message.id}.{file_ext}"
                media_path = os.path.join(media_dir, filename)
                
                async with self.semaphore:
                    media_path = await message.download_media(media_path)
                    logger.info(f"Downloaded: {media_path}")
                    return media_path
            
            except Exception as e:
                logger.error(f"Failed to download media for message {message.id}: {e}")
                return None

        tasks = [download(media) for media in medias]
        return tasks
        # return await asyncio.gather(*tasks)

    async def cleanup(self) -> None:
        """Clean up sensitive data from memory."""
        # zeroize 
        del self.api_id
        del self.api_hash
        del self.phone_number
        logger.info("Sensitive data cleared from memory.")
    
    async def close(self) -> None:
        """Disconnect the Telegram client."""
        await self.client.disconnect()
        logger.info("Telegram client disconnected.")
