import os
import sys
import pandas as pd

# Setup logger for cleaning operations
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.cleaner import *
from scripts.utils.storage_interface import StorageInterface

logger = setup_logger("data_cleaning")

# ==========================================
# Data Cleaning Pipeline
# ==========================================

class TelegramDataCleaningPipeline:
    def __init__(self, storage):
        """
        Initialize the cleaning pipeline with a storage backend.

        Args:
            storage: The storage backend for saving cleaned data.
        """
        self.storage = storage

    async def load_raw_data(self) -> pd.DataFrame:
        """Load raw data from the storage backend."""
        try:
            raw_data = await self.storage.retrieve_data({})  # Fetch all data
            data = pd.DataFrame(raw_data)
            logger.info(f"Loaded {len(data)} raw Telegram messages from storage.")
            return data
        except Exception as e:
            logger.error(f"Error loading raw data: {e}")
            return pd.DataFrame()

    def clean_text_pipeline(self, text: str) -> str:
        """Apply a series of cleaning steps to a text message."""
        if not text:
            return ""

        # Apply cleaning steps
        text = normalize_amharic_text(text, AMHARIC_DIACRITICS_MAP)
        # text = remove_non_amharic_characters(text)
        text = remove_punctuation(text)
        text = remove_emojis(text)
        text = remove_repeated_characters(text)
        text = remove_urls(text)
        text = normalize_links(text)  # Normalize URLs
        text = normalize_spaces(text)

        return text

    def clean_dataframe(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the entire dataframe."""
        try:
            # Drop duplicates
            # data = data.drop_duplicates(subset=["Message IDs"]).copy()
            # logger.info("Duplicates removed from dataset.")

            # Standardize column names
            # data = data.rename(columns={
            #     "Channel": "channel",
            #     "Group ID": "group_id",
            #     "Message IDs": "message_ids",
            #     "Message": "message",
            #     "Text": "text",
            #     "Sender ID": "sender_id",
            #     "Date": "message_date",
            #     "Media Path": "media_path"
            # })

            # Extract additional features
            data['Emojis'] = data['Message'].apply(extract_emojis)
            data['Links'] = data['Message'].apply(extract_links)
            data['Message'] = data['Message'].apply(remove_urls)

            # Handle missing values
            data['Message'] = data['Message'].fillna("No Message")
            data['Media Path'] = data['Media Path'].fillna("No Media")
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce').fillna(pd.Timestamp.now())

            # Clean text columns
            data['Message'] = data['Message'].apply(self.clean_text_pipeline)
            data['Channel'] = data['Channel'].str.strip()

            logger.info("Data cleaning completed successfully.")

            return data
        
        except Exception as e:
            logger.error(f"Error cleaning dataframe: {e}")
            raise

    async def run(self, data=None):
        """Run the entire cleaning pipeline."""
        try:
            # Load raw data
            if data is None:
                data = await self.load_raw_data()
            if data.empty:
                logger.warning("No data to process.")
                return

            # Clean data
            cleaned_data = self.clean_dataframe(data)

            # Save cleaned data
            await self.storage.save_data(cleaned_data.to_dict('records'))
            logger.info("Cleaned data saved successfully.")

            return cleaned_data
        
        except Exception as e:
            logger.error(f"Error in cleaning pipeline: {e}")
            raise

async def main(storage_type):
    # Initialize storage backend
    storage = await StorageInterface.create_storage(storage_type)

    # Initialize and run the cleaning pipeline
    pipeline = TelegramDataCleaningPipeline(storage)
    await pipeline.run()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

