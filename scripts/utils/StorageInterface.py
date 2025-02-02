import os
import json
import asyncio
import gridfs
import psycopg2
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId


class StorageInterface(ABC):
    """
    Abstract base class defining a common interface for storage backends.
    """
    
    @abstractmethod
    def save_data(self, data: List[Dict[str, Any]]):
        pass
    
    @abstractmethod
    def close(self):
        pass


class MongoDBStorage(StorageInterface):
    """
    MongoDB storage implementation using GridFS for media storage.
    """
    
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.fs = gridfs.GridFS(self.db)
    
    def save_data(self, data: List[Dict[str, Any]]):
        """Save structured data into MongoDB."""
        if data:
            self.collection.insert_many(data)
    
    def save_media(self, file_path: str) -> Optional[str]:
        """Save media file using GridFS and return its ObjectId."""
        try:
            with open(file_path, "rb") as f:
                file_id = self.fs.put(f, filename=os.path.basename(file_path))
            return str(file_id)
        except Exception as e:
            print(f"Error saving media to GridFS: {e}")
            return None
    
    def close(self):
        self.client.close()


class PostgresStorage(StorageInterface):
    """
    PostgreSQL storage implementation.
    """
    
    def __init__(self, dsn: str, table_name: str):
        self.conn = psycopg2.connect(dsn)
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self._create_table()
    
    def _create_table(self):
        """Ensure the table exists."""
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY,
                group_id BIGINT,
                message TEXT,
                date TIMESTAMP,
                sender_id BIGINT,
                media_path TEXT[]
            )
        ''')
        self.conn.commit()
    
    def save_data(self, data: List[Dict[str, Any]]):
        """Insert data into PostgreSQL."""
        query = f'''
            INSERT INTO {self.table_name} (group_id, message, date, sender_id, media_path)
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = [(
            d.get("Group ID"), d.get("Message"), d.get("Date"), d.get("Sender ID"), d.get("Media Path")
        ) for d in data]
        self.cursor.executemany(query, values)
        self.conn.commit()
    
    def close(self):
        self.cursor.close()
        self.conn.close()


class LocalStorage(StorageInterface):
    """
    Local storage implementation supporting JSON and CSV formats.
    """
    
    def __init__(self, file_path: str, file_format: str = "json"):
        self.file_path = file_path
        self.file_format = file_format
    
    def save_data(self, data: List[Dict[str, Any]]):
        """Save data to local file in JSON format."""
        if self.file_format == "json":
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        else:
            raise ValueError("Unsupported file format")
    
    def close(self):
        pass


async def download_media(client, messages: List[Any], media_dir: str) -> Dict[int, str]:
    """
    Optimized function for downloading media concurrently.
    
    Args:
        client: Telegram client instance.
        messages (List[Any]): List of Telegram messages containing media.
        media_dir (str): Directory to save downloaded media.
    
    Returns:
        Dict[int, str]: Mapping of message IDs to their downloaded file paths.
    """
    os.makedirs(media_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(5)  # Limit concurrent downloads
    
    async def download(message) -> Optional[Tuple[int, str]]:
        try:
            if message.media:
                filename = f"{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                async with semaphore:
                    await client.download_media(message, file=media_path)
                return message.id, media_path
        except Exception as e:
            print(f"Failed to download media {message.id}: {e}")
        return None
    
    tasks = [download(msg) for msg in messages]
    results = await asyncio.gather(*tasks)
    return {msg_id: path for msg_id, path in results if msg_id}
