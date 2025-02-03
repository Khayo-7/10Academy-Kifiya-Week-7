import os
import sys
import csv
import json
import asyncio
import aiofiles
import pandas as pd
from typing import List, Dict, Any

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.utils.storage_interface import StorageInterface
from scripts.data_utils.cleaning_pipeline import TelegramDataCleaningPipeline

logger = setup_logger("preprocess")

# ==========================================
# Main Data Preprocessing Function
# ==========================================

async def preprocess_data(data: pd.DataFrame, storage_type: str = "postgres") -> pd.DataFrame:
    """
    Loads, preprocesses, and saves cleaned data.
    """
    # Initialize storage backend
    storage = await StorageInterface.create_storage(storage_type)

    # Initialize and run the cleaning pipeline
    pipeline = TelegramDataCleaningPipeline(storage)
    
    cleaned_data = await pipeline.run(data)

    return cleaned_data

async def merge_files(data_path: str):
    """Merge JSON and CSV data from all channels into a single file."""
        
    async def load_json_data(file_path: str, channel: str) -> List[Dict[str, Any]]:
        """Load data from a JSON file and add channel name."""
        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                data = json.loads(await f.read())
                for record in data:
                    record["Channel"] = channel  # Add channel name
                return data
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []

    def load_csv_data(file_path: str, channel: str) -> List[Dict[str, Any]]:
        """Load data from a CSV file using synchronous I/O (fixing splitlines issue)."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                return [{**row, "Channel": channel} for row in reader]
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []
        
    all_data = []
    
    # Iterate over all files in the directory
    for file in os.listdir(data_path):
        file_path = os.path.join(data_path, file)
        channel_name, ext = os.path.splitext(file)

        if ext == ".json":
            all_data.extend(await load_json_data(file_path, channel_name))
        elif ext == ".csv":
            all_data.extend(load_csv_data(file_path, channel_name))  # Synchronous function call

    data_path_json = os.path.join(data_path, "Messages.json")
    data_path_csv = os.path.join(data_path, "Messages.csv")

    # Save merged JSON
    try:
        async with aiofiles.open(data_path_json, "w", encoding="utf-8") as f:
            await f.write(json.dumps(all_data, indent=4, ensure_ascii=False))
        logger.info(f"Merged JSON saved to {data_path_json}")
    except Exception as e:
        logger.error(f"Error saving JSON file: {e}")

    # Save merged CSV
    if all_data:
        try:
            fieldnames = list(all_data[0].keys())  # Get headers from the first record
            with open(data_path_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_data)
            logger.info(f"Merged CSV saved to {data_path_csv}")
        except Exception as e:
            logger.error(f"Error saving CSV file: {e}")

if __name__ == "__main__":
    data_path = "../resources/data/raw"
    asyncio.run(merge_files(data_path))