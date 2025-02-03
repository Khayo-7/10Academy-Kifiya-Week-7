import logging
from telethon import TelegramClient
import csv
import os
import json
from dotenv import load_dotenv
import asyncio

# Set up logging
logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables once
load_dotenv('.env')
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone_number = os.getenv('PHONE_NUMBER')

# Function to read channels from a JSON file
def load_channels_from_json(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get('channels', []), data.get('comments', [])
    except Exception as e:
        logging.error(f"Error reading channels from JSON: {e}")
        return [], []

# Function to get last processed message ID
def get_last_processed_id(channel_username):
    try:
        with open(f"{channel_username}_last_id.json", 'r') as f:
            return json.load(f).get('last_id', 0)
    except FileNotFoundError:
        logging.warning(f"No last ID file found for {channel_username}. Starting from 0.")
        return 0

# Function to save last processed message ID
def save_last_processed_id(channel_username, last_id):
    with open(f"{channel_username}_last_id.json", 'w') as f:
        json.dump({'last_id': last_id}, f)
        logging.info(f"Saved last processed ID {last_id} for {channel_username}.")

# Function to scrape data from a single channel
async def scrape_channel(client, channel_username, writer, media_dir, num_messages, start_from_id=None):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title
        
        last_id = start_from_id if start_from_id is not None else get_last_processed_id(channel_username)
        
        message_count = 0
        async for message in client.iter_messages(entity):
            if message.id <= last_id:
                continue
            
            media_path = None
            if message.media:
                filename = f"{channel_username}_{message.id}.{message.media.document.mime_type.split('/')[-1]}" if hasattr(message.media, 'document') else f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)
                logging.info(f"Downloaded media for message ID {message.id}.")
            
            writer.writerow([channel_title, channel_username, message.id, message.message, message.date, media_path])
            logging.info(f"Processed message ID {message.id} from {channel_username}.")
            
            last_id = message.id
            message_count += 1
            
            # Stop after scraping the specified number of messages
            if message_count >= num_messages:
                break

        save_last_processed_id(channel_username, last_id)

        if message_count == 0:
            logging.info(f"No new messages found for {channel_username}.")

    except Exception as e:
        logging.error(f"Error while scraping {channel_username}: {e}")

# Initialize the client once with a session file
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    try:
        await client.start(phone_number)
        logging.info("Client started successfully.")
        
        media_dir = 'photos'
        os.makedirs(media_dir, exist_ok=True)

        # Load channels from JSON file
        channels, comments = load_channels_from_json('channels.json')
        
        num_messages_to_scrape = 20  # Specify the number of messages to scrape

        for channel in channels:
            # Create a CSV file named after the channel
            csv_filename = f"{channel[1:]}_data.csv"  # Remove '@' from channel name
            with open(csv_filename, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])
                
                # Check if a starting message ID is provided for this channel
                start_from_id = None  # You can modify this to accept a starting ID dynamically
                await scrape_channel(client, channel, writer, media_dir, num_messages_to_scrape, start_from_id)
                logging.info(f"Scraped data from {channel}.")

        # Log commented channels if needed
        if comments:
            logging.info(f"Commented channels: {', '.join(comments)}")

    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == "__main__":
    asyncio.run(main())