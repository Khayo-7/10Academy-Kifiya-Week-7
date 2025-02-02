import os
import json
import csv
import pymongo
import gridfs
import psycopg2
from typing import List, Dict, Any, Union


class DatabaseManager:
    def save_data(self, data: List[Dict[str, Any]]) -> None:
        raise NotImplementedError("This method should be implemented in subclasses")
    
    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError("This method should be implemented in subclasses")


class MongoDBManager(DatabaseManager):
    def __init__(self, uri: str, db_name: str, collection_name: str, use_gridfs: bool = False):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.use_gridfs = use_gridfs
        self.fs = gridfs.GridFS(self.db) if use_gridfs else None

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        self.collection.insert_many(data)

    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(self.collection.find(query, {'_id': False}))
    
    def save_file(self, file_path: str) -> str:
        if not self.use_gridfs:
            raise ValueError("GridFS is not enabled")
        with open(file_path, "rb") as f:
            file_id = self.fs.put(f, filename=os.path.basename(file_path))
        return str(file_id)

    def retrieve_file(self, file_id: str, output_path: str) -> None:
        if not self.use_gridfs:
            raise ValueError("GridFS is not enabled")
        file_data = self.fs.get(file_id)
        with open(output_path, "wb") as f:
            f.write(file_data.read())


class PostgresManager(DatabaseManager):
    def __init__(self, db_url: str, table_name: str):
        self.conn = psycopg2.connect(db_url)
        self.cursor = self.conn.cursor()
        self.table_name = table_name

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        if not data:
            return
        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        self.cursor.executemany(query, values)
        self.conn.commit()

    def retrieve_data(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        conditions = ' AND '.join([f"{k}=%s" for k in query.keys()])
        query_sql = f"SELECT * FROM {self.table_name} WHERE {conditions}" if query else f"SELECT * FROM {self.table_name}"
        self.cursor.execute(query_sql, list(query.values()))
        return [dict(zip([desc[0] for desc in self.cursor.description], row)) for row in self.cursor.fetchall()]


class FileStorageManager(DatabaseManager):
    def __init__(self, file_path: str, file_format: str = "json"):
        self.file_path = file_path
        self.file_format = file_format.lower()

    def save_data(self, data: List[Dict[str, Any]]) -> None:
        if self.file_format == "json":
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        elif self.file_format == "csv":
            with open(self.file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        else:
            raise ValueError("Unsupported file format")

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
