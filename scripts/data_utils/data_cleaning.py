import re
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from sqlalchemy import create_engine

import re
import os
import sys
import emoji
import pandas as pd
from typing import Dict


# Setup logger for cleaning operations
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger

logger = setup_logger("data_cleaning")

# ==========================================
# Data Cleaning Pipeline
# ==========================================

class TelegramDataCleaningPipeline:
    def __init__(self, mongo_uri, pg_uri, mongo_db, mongo_collection, pg_table):
        self.mongo_client = MongoClient(mongo_uri)
        self.pg_engine = create_engine(pg_uri)
        self.mongo_db = self.mongo_client[mongo_db]
        self.mongo_collection = self.mongo_db[mongo_collection]
        self.pg_table = pg_table

    def load_raw_data(self):
        """Loads raw data from MongoDB."""
        try:
            data = list(self.mongo_collection.find())
            df = pd.DataFrame(data)
            logger.info(f"Loaded {len(df)} raw Telegram messages from MongoDB")
            return df
        except Exception as e:
            logger.error(f"Error loading data from MongoDB: {e}")
            return pd.DataFrame()

    def extract_links(self, message):
        """Extracts all URLs from a message text."""
        url_pattern = r'(https?://\S+|www\.\S+)'
        links = re.findall(url_pattern, message) if isinstance(message, str) else []
        return links if links else None  # Return None if no links found

    def aggregate_group_messages(self, df):
        """
        Groups messages by 'group_id', consolidating multiple message IDs and media paths into a single entry.
        The first message in a group keeps the text, while others contribute only media paths.
        """
        if 'group_id' not in df.columns:
            df['group_id'] = df['message_id']  # If no group_id exists, treat each message as its own group

        grouped_df = (
            df.groupby('group_id')
            .agg({
                'message_id': lambda x: list(x),  # Collect all message IDs in the group
                'message_text': 'first',  # Keep text from the first message
                'sender_id': 'first',  # Assume same sender for the group
                'channel': 'first',  # Keep channel info
                'message_date': 'first',  # Keep the earliest date
                'media_path': lambda x: [path for path in x if path and path != 'No Media'],  # Collect all media paths
                'links': lambda x: sum(x, []) if isinstance(x.iloc[0], list) else [],  # Flatten links list
            })
            .reset_index()
        )
        logger.info(f"Grouped messages into {len(grouped_df)} unique groups")
        return grouped_df

    def remove_duplicates(self, df):
        """Removes duplicate messages based on unique message_id."""
        initial_count = len(df)
        df = df.drop_duplicates(subset=['message_id'], keep='first')
        logger.info(f"Removed {initial_count - len(df)} duplicate messages")
        return df

    def handle_missing_values(self, df):
        """Handles missing values by filling or dropping based on business rules."""
        df.fillna({
            'sender_id': 'Unknown Sender',
            'message_text': 'No Text',
            'media_path': 'No Media',
            'links': []
        }, inplace=True)

        df.dropna(subset=['message_id', 'message_date'], inplace=True)  # These fields are mandatory
        logger.info("Handled missing values")
        return df

    def standardize_formats(self, df):
        """Standardizes text formats, timestamps, and media paths."""
        df['message_text'] = df['message_text'].astype(str).str.strip().str.lower()
        df['sender_id'] = df['sender_id'].astype(str).str.strip()
        
        # Convert Telegram timestamps to a uniform format
        df['message_date'] = pd.to_datetime(df['message_date'], errors='coerce').fillna(pd.Timestamp.now())

        # Standardize media paths (e.g., ensure consistent URL format)
        df['media_path'] = df['media_path'].apply(lambda x: [re.sub(r'\s+', '', str(path)) for path in x] if isinstance(x, list) else x)

        logger.info("Standardized message formats")
        return df

    def validate_data(self, df):
        """Validates essential fields and logs issues."""
        invalid_messages = df[df['message_text'].str.len() < 2]
        if not invalid_messages.empty:
            logger.warning(f"Found {len(invalid_messages)} very short messages")

        unknown_senders = df[df['sender_id'] == 'Unknown Sender']
        if not unknown_senders.empty:
            logger.warning(f"Found {len(unknown_senders)} messages from unknown senders")

        return df

    def store_cleaned_data(self, df):
        """Stores cleaned data in MongoDB and PostgreSQL."""
        try:
            self.mongo_collection.drop()
            self.mongo_collection.insert_many(df.to_dict('records'))
            logger.info("Stored cleaned Telegram data in MongoDB")
        except Exception as e:
            logger.error(f"Error storing data in MongoDB: {e}")

        try:
            df.drop(columns=['_id'], inplace=True, errors='ignore')
            df.to_sql(self.pg_table, self.pg_engine, if_exists='replace', index=False)
            logger.info("Stored cleaned Telegram data in PostgreSQL")
        except Exception as e:
            logger.error(f"Error storing data in PostgreSQL: {e}")

    def run(self):
        """Executes the data cleaning pipeline."""
        df = self.load_raw_data()
        if df.empty:
            logger.warning("No data to process")
            return

        df['links'] = df['message_text'].apply(self.extract_links)  # Extract links from messages
        df = self.remove_duplicates(df)
        df = self.handle_missing_values(df)
        df = self.aggregate_group_messages(df)  # Consolidate messages within groups
        df = self.standardize_formats(df)
        df = self.validate_data(df)
        self.store_cleaned_data(df)
        logger.info("Telegram data cleaning pipeline completed successfully")
