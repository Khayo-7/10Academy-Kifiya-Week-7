import os
import csv
import json
import asyncio
import gridfs
import pymongo
import psycopg2
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class StorageInterface(ABC):
    """
    Abstract base class defining a common interface for storage backends.
    """
    @staticmethod
    def create_storage(storage_type: str, **kwargs) -> 'StorageInterface':
        if storage_type == "mongo":
            return MongoDBStorage(
                uri=kwargs["uri"], 
                db_name=kwargs["db_name"], 
                collection_name=kwargs["collection_name"]
            )
        elif storage_type == "postgres":
            return PostgresStorage(
                db_url=kwargs["db_url"], 
                table_name=kwargs["table_name"]
            )
        elif storage_type == "json":
            return LocalStorage(
                file_path=kwargs["file_path"], 
                file_format="json"
            )
        else:
            raise ValueError("Unsupported storage type")
        
    @abstractmethod
    def save_data(self, data: List[Dict[str, Any]]) -> None:
        raise NotImplementedError("This method should be implemented in subclasses")
    
    @abstractmethod
    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError("This method should be implemented in subclasses")

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError("This method should be implemented in subclasses")

class MongoDBStorage(StorageInterface):
    """
    MongoDB storage implementation using GridFS for media storage.
    """
    def __init__(self, uri: str, db_name: str, collection_name: str, use_gridfs: bool = False):
        
        try:
            self.client = MongoClient(uri)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            self.use_gridfs = use_gridfs
            self.fs = gridfs.GridFS(self.db) if use_gridfs else None
            # self.fs = gridfs.GridFS(self.db)
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save structured data into MongoDB."""
        if data:
            self.collection.insert_many(data)

    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(self.collection.find(query, {'_id': False}))
    
    def save_metadata(self, data: dict):
        """Save metadata into MongoDB."""
        self.collection.insert_one(data)

    def save_media(self, file_path: str, metadata: dict=None) -> Optional[str]:
        """Save media file using GridFS and return its ObjectId."""
        if not self.use_gridfs:
            raise ValueError("GridFS is not enabled")
        try:
            with open(file_path, "rb") as f:
                file_id = self.fs.put(f, filename=os.path.basename(file_path), **metadata)
            return str(file_id)
        except Exception as e:
            print(f"Error saving media to GridFS: {e}")
            return None
    
    def retrieve_media(self, file_id: str, output_path: str) -> None:
        if not self.use_gridfs:
            raise ValueError("GridFS is not enabled")
        try:
            file_data = self.fs.get(file_id)
            with open(output_path, "wb") as f:
                f.write(file_data.read())
        except gridfs.NoFile as e:
            print(f"File not found in GridFS: {e}")
            raise
        except Exception as e:
            print(f"Error retrieving media from GridFS: {e}")
            raise

    async def close(self):
        await self.client.close()
        pass

class PostgresStorage(StorageInterface):
    """
    PostgreSQL storage implementation.
    """
    
    def __init__(self, db_url: str, table_name: str):
        
        try:
            self.conn = psycopg2.connect(db_url)
            self.cursor = self.conn.cursor()
            self.table_name = table_name
            self._create_table()
        except psycopg2.Error as e:
            print(f"Failed to connect to PostgreSQL: {e}")
            raise

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

    def save_data(self, data: List[Dict[str, Any]]) -> None:   
        """Insert data into PostgreSQL."""
        if not data:
            return
        
        try:
            # columns = data[0].keys()
            # values = [[row[col] for col in columns] for row in data]
            # placeholders = ', '.join(['%s'] * len(columns))
            # query = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            query = f'''
                INSERT INTO {self.table_name} (group_id, message, date, sender_id, media_path)
                VALUES (%s, %s, %s, %s, %s)
            '''
            values = [(
                d.get("Group ID"), d.get("Message"), d.get("Date"), d.get("Sender ID"), d.get("Media Path")
            ) for d in data]

            self.cursor.executemany(query, values)
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error saving data to PostgreSQL: {e}")
            self.conn.rollback()
            raise
    
    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        conditions = ' AND '.join([f"{k}=%s" for k in query.keys()])
        query_sql = f"SELECT * FROM {self.table_name} WHERE {conditions}" if query else f"SELECT * FROM {self.table_name}"
        self.cursor.execute(query_sql, list(query.values()))
        return [dict(zip([desc[0] for desc in self.cursor.description], row)) for row in self.cursor.fetchall()]

    async def close(self):
        await self.cursor.close()
        await self.conn.close()

class LocalStorage(StorageInterface):
    """
    Local storage implementation supporting JSON and CSV formats.
    """
        
    def __init__(self, file_path: str, file_format: str = "json"):
        if file_format.lower() not in ["json", "csv"]:
            raise ValueError("Unsupported file format. Supported formats are 'json' and 'csv'.")
        self.file_path = file_path
        self.file_format = file_format.lower()

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """Save data to local file in JSON/CSV format."""
        
        try:
            if self.file_format == "json":
                with open(self.file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
            elif self.file_format == "csv":
                with open(self.file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
            else:
                raise ValueError("Unsupported file format")
        except IOError as e:
            print(f"Error saving data to local file: {e}")
            raise
    
    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        if self.file_format == "json":
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        elif self.file_format == "csv":
            with open(self.file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                data = [row for row in reader]
        else:
            raise ValueError("Unsupported file format")
        return [row for row in data if all(row[k] == v for k, v in query.items())]

    async def close(self):
        pass