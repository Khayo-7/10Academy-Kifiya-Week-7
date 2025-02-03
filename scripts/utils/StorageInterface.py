import os
import sys
import csv
import json
import gridfs
import asyncpg
import psycopg2
import aiofiles
import motor.motor_asyncio
from dotenv import load_dotenv
from pymongo import MongoClient
from abc import ABC, abstractmethod
from pymongo.errors import ConnectionFailure
from typing import List, Dict, Any, Optional


# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json
from scripts.utils.telegram_client import TelegramAPI

logger = setup_logger("StorageInterface")

# Load environment variables
load_dotenv()

class StorageInterface(ABC):
    """
    Abstract base class defining a common interface for storage backends.
    """

    @staticmethod
    def get_config_info(storage_type: str) -> Dict[str, Any]:
        """Retrieve configuration information from environment variables."""
        if storage_type not in ["mongo", "postgres", "json", "csv"]:
            raise ValueError(f"Unsupported storage type: {storage_type}")

        config_info = {}

        if storage_type == "mongo":
            config_info = {
                "uri": os.getenv("MONGO_URI"),
                "db_name": os.getenv("MONGO_DB_NAME"),
                "collection_name": os.getenv("MONGO_COLLECTION_NAME")
            }

        elif storage_type == "postgres":
            config_info = {
                "db_url": os.getenv("POSTGRES_DB_URL"),
                "table_name": os.getenv("POSTGRES_TABLE_NAME")
            }
        elif storage_type == "json":
            config_info = {
                "dir_path": os.getenv("LOCAL_STORAGE_PATH"),
                "file_format": "json"
            }
        elif storage_type == "csv":
            config_info = {
                "dir_path": os.getenv("LOCAL_STORAGE_PATH"),
                "file_format": "csv"
            }

        return config_info

    @staticmethod
    def create_storage(storage_type: str) -> 'StorageInterface':
        """Create a storage instance based on the specified storage type."""
        if storage_type not in ["mongo", "postgres", "json", "csv"]:
            raise ValueError(f"Unsupported storage type: {storage_type}")

        config_info = StorageInterface.get_config_info(storage_type)
        if storage_type == "mongo":
            return MongoDBStorage(
                uri=config_info["uri"],
                db_name=config_info["db_name"],
                collection_name=config_info["collection_name"]
            )
        elif storage_type == "postgres":
            return PostgresStorage(
                db_url=config_info["db_url"],
                table_name=config_info["table_name"]
            )
        elif storage_type == "json":
            return LocalStorage(
                dir_path=config_info["dir_path"],
                file_format="json"
            )
        elif storage_type == "csv":
            return LocalStorage(
                dir_path=config_info["dir_path"],
                file_format="csv"
            )
        else:
            raise ValueError("Unsupported storage type")

    @abstractmethod
    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save data to the storage backend."""
        raise NotImplementedError("This method should be implemented in subclasses")

    @abstractmethod
    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from the storage backend."""
        raise NotImplementedError("This method should be implemented in subclasses")

    @abstractmethod
    async def close(self) -> None:
        """Close the storage connection."""
        raise NotImplementedError("This method should be implemented in subclasses")


class MongoDBStorage(StorageInterface):
    """
    MongoDB storage implementation using GridFS for media storage.
    """
    def __init__(self, uri: str, db_name: str, collection_name: str, use_gridfs: bool = False):
        try:
            # self.client = MongoClient(uri)
            self.client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            self.use_gridfs = use_gridfs
            self.fs = gridfs.GridFS(self.db) if use_gridfs else None
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save structured data into MongoDB."""
        if data:
            try:
                # self.collection.insert_many(data)
                # self.collection.insert_one(data)
                await self.collection.insert_many(data)
            except Exception as e:
                logger.error(f"Error saving data to MongoDB: {e}")
                raise

    async def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from MongoDB based on the query."""
        try:
            cursor = self.collection.find(query, {'_id': False})
            return await cursor.to_list(length=None)
            # return list(cursor)
        except Exception as e:
            logger.error(f"Error retrieving data from MongoDB: {e}")
            raise

    async def save_media(self, file_path: str, metadata: Optional[dict] = None) -> Optional[str]:
        """Save media file using GridFS and return its ObjectId."""
        if not self.use_gridfs:
            raise ValueError("GridFS is not enabled")
        try:
            async with aiofiles.open(file_path, "rb") as f:
                file_data = await f.read()
                file_id = await self.fs.put(file_data, filename=os.path.basename(file_path), **(metadata or {}))
            return str(file_id)
        except Exception as e:
            logger.error(f"Error saving media to GridFS: {e}")
            return None
        
    async def retrieve_media(self, file_id: str, output_path: str) -> None:
        """Retrieve media file from GridFS."""
        if not self.use_gridfs:
            raise ValueError("GridFS is not enabled")
        try:
            file_data = await self.fs.get(file_id)
            with open(output_path, "wb") as f:
                f.write(await file_data.read())
        except gridfs.NoFile as e:
            logger.error(f"File not found in GridFS: {e}")
            raise
        except Exception as e:
            logger.error(f"Error retrieving media from GridFS: {e}")
            raise

    async def close(self):
        """Close the MongoDB connection."""
        try:
            self.client.close()
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")


class PostgresStorage(StorageInterface):
    """
    PostgreSQL storage implementation.
    """
    
    def __init__(self, db_url: str, table_name: str):
        try:
            self.db_url = db_url
            self.table_name = table_name
            self.conn = None

        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    async def connect(self):
        """Connect to the PostgreSQL database."""
        try:
            self.conn = await asyncpg.connect(self.db_url)
            # self.conn = psycopg2.connect(self.db_url)
            # self.cursor = self.conn.cursor()
            await self._create_table()
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    async def _create_table(self):
        """Ensure the table exists."""
        try:
            await self.conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id SERIAL PRIMARY KEY,
                    group_id BIGINT UNIQUE,
                    message_ids BIGINT[],
                    message TEXT,
                    date TIMESTAMP,
                    sender_id BIGINT,
                    media_path TEXT[]
                )
            ''')
            # await self.conn.execute(f'''
            #     CREATE TABLE IF NOT EXISTS {self.table_name} (
            #         id SERIAL PRIMARY KEY,
            #         channel_title TEXT,
            #         channel_username TEXT,
            #         group_id BIGINT UNIQUE,
            #         message_id BIGINT,
            #         message TEXT,
            #         date TIMESTAMP,
            #         sender_id BIGINT,
            #         media_path TEXT[],
            #         emoji_used TEXT[],
            #         youtube_links TEXT[]
            #     )
            # ''')

        except Exception as e:
            logger.error(f"Error creating table in PostgreSQL: {e}")
            raise

    async def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Insert data into PostgreSQL."""
        if not data:
            return
        
        try:
            # query = f'''
            #     INSERT INTO {self.table_name} (group_id, message_ids, message, date, sender_id, media_path)
            #     VALUES (%s, %s, %s, %s, %s)
            # '''
            query = f'''
                INSERT INTO {self.table_name} (group_id, message_ids, message, date, sender_id, media_path)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            '''
            values = [(
                d.get("Group ID"), d.get("Message IDs"), d.get("Message"), d.get("Date"), d.get("Sender ID"), d.get("Media Path")
            ) for d in data]

            # query = f'''
            #     INSERT INTO {self.table_name} (channel_title, channel_username, group_id, message_id, message, date, sender_id, media_path, emoji_used, youtube_links)
            #     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            # '''
            # values = [
            #     (
            #         d.get("Channel Title"),
            #         d.get("Channel Username"),
            #         d.get("Group ID"),
            #         d.get("Message IDs"),
            #         d.get("Message"),
            #         d.get("Date"),
            #         d.get("Sender ID"),
            #         d.get("Media Path"),
            #         d.get("Emoji Used"),
            #         d.get("YouTube Links")
            #     ) for d in data
            # ]

            await self.conn.executemany(query, values)
            # self.cursor.executemany(query, values)
            # self.conn.commit()
        # except psycopg2.Error as e:
        except Exception as e:
            logger.error(f"Error saving data to PostgreSQL: {e}")
            # self.conn.rollback()
            logger.error(f"Error saving data to PostgreSQL: {e}")
            raise
    
    async def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from PostgreSQL based on the query."""
        try:
            conditions = ' AND '.join([f"{k}=${i+1}" for i, k in enumerate(query.keys())])
            query_sql = f"SELECT * FROM {self.table_name} WHERE {conditions}" if query else f"SELECT * FROM {self.table_name}"
            rows = await self.conn.fetch(query_sql, *query.values())
            return [dict(row) for row in rows]
            
            # conditions = ' AND '.join([f"{k}=%s" for k in query.keys()])
            # self.cursor.execute(query_sql, list(query.values()))
            # return [dict(zip([desc[0] for desc in self.cursor.description], row)) for row in self.cursor.fetchall()]

        except Exception as e:
            logger.error(f"Error retrieving data from PostgreSQL: {e}")
            raise

    async def close(self):
        """Close the PostgreSQL connection."""
        try:
            # self.cursor.close()
            self.conn.close()
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection: {e}")

class LocalStorage(StorageInterface):
    """
    Local storage implementation supporting JSON and CSV formats.
    """
        
    def __init__(self, dir_path: str, file_format: str = "json"):
        
        if file_format.lower() not in ["json", "csv"]:
            raise ValueError("Unsupported file format. Supported formats are 'json' and 'csv'.")
        
        self.dir_path = dir_path
        self.file_format = file_format.lower()
        self.filename = "Messages" + '.' + self.file_format
        self.file_path = os.path.join(dir_path, self.filename)
        os.makedirs(self.dir_path, exist_ok=True)

    async def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save data to local file in JSON/CSV format."""

        try:
            if self.file_format == "json":
                async with aiofiles.open(self.file_path, "w", encoding="utf-8") as f:
                    await f.write(json.dumps(data, indent=4, ensure_ascii=False))
            elif self.file_format == "csv":
                async with aiofiles.open(self.file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    await writer.writeheader()
                    await writer.writerows(data)
        except Exception as e:
            logger.error(f"Error saving data to local file: {e}")
            raise
    
    async def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from the local file based on the query."""

        try:
            if self.file_format == "json":
                async with aiofiles.open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.loads(await f.read())
            elif self.file_format == "csv":
                async with aiofiles.open(self.file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    data = [row for row in reader]
            else:
                raise ValueError("Unsupported file format")

            return [row for row in data if all(row[k] == v for k, v in query.items())]
        except Exception as e:
            logger.error(f"Error retrieving data from local file: {e}")
            raise

    async def close(self):
        """Close any resources held by LocalStorage (if necessary)."""
        pass