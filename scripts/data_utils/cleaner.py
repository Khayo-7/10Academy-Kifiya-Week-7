import re
import os
import sys
import emoji
import pandas as pd
from typing import Dict

# Setup logger for cleaning operations
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger

logger = setup_logger("cleaning")

# ==========================================
# Helper Functions for Cleaning Operations
# ==========================================

def normalize_amharic_text(text: str, diacritics_map: Dict[str, str]) -> str:
    """
    Replaces Amharic diacritics with their base forms based on the given map.
    """
    if not isinstance(text, str):
        logger.warning("Input text is not a string. Skipping normalization.")
        return text

    for diacritic, base_char in diacritics_map.items():
        text = text.replace(diacritic, base_char)
    
    logger.debug("Normalized Amharic diacritics.")
    return text

def remove_non_amharic_characters(text: str) -> str:
    """
    Removes characters that are not part of the Amharic Unicode block or spaces.
    """
    # pattern = re.compile(r'[^\u1200-\u137F\s]') # Retains Amharic script only
    pattern = re.compile(r'[^\u1200-\u137F0-9\s]')  # Retains Amharic script and numbers
    result = pattern.sub('', text)
    logger.debug("Removed non-Amharic characters.")
    return result

def remove_punctuation(text: str) -> str:
    """
    Removes Amharic punctuation and replaces it with a space.
    """
    pattern = re.compile(r'[፡።፣፤፥፦፧፨]+')
    result = pattern.sub(' ', text)
    logger.debug("Removed Amharic punctuation.")
    return result

def remove_emojis(text: str) -> str:
    """
    Removes emojis from the text.
    """
    emoji_pattern = re.compile(
        "[" 
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE
    )
    result = emoji_pattern.sub('', text)
    logger.debug("Removed emojis.")
    return result

def remove_repeated_characters(text: str) -> str:
    """
    Collapses consecutive repeated characters into a single instance.
    """
    result = re.sub(r'(.)\1+', r'\1', text)
    logger.debug("Removed repeated characters.")
    return result

def remove_numbers(text: str) -> str:
    """
    Removes numeric characters from the text.
    """
    result = re.sub(r'\d+', '', text)
    logger.debug("Removed numbers.")
    return result

def remove_urls(text: str) -> str:
    """
    Removes URLs from the text.
    """
    result = re.sub(r'http\S+|www\S+', '', text)
    logger.debug("Removed URLs.")
    return result

def normalize_spaces(text: str) -> str:
    """
    Normalize spaces in the text.
    """
    # Normalize Multiple whitespace characters and trim
    result = ' '.join(text.split()).strip()
    # result = re.sub(r'\s+', ' ', text).strip()

    logger.debug("Normalize spaces.")
    return result

def extract_emojis(text):
    """ Extract emojis from text, return 'No emoji' if none found. """
    emojis = ''.join(c for c in text if c in emoji.EMOJI_DATA)
    return emojis if emojis else "No emoji"

def remove_emojis(text):
    """ Remove emojis from the message text. """
    return ''.join(c for c in text if c not in emoji.EMOJI_DATA)

def extract_youtube_links(text):
    """ Extract YouTube links from text, return 'No YouTube link' if none found. """
    youtube_pattern = r"(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+)"
    links = re.findall(youtube_pattern, text)
    return ', '.join(links) if links else "No YouTube link"

def remove_youtube_links(text):
    """ Remove YouTube links from the message text. """
    youtube_pattern = r"https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+"
    return re.sub(youtube_pattern, '', text).strip()

def clean_text(text):
    """ Standardize text by removing newline characters and unnecessary spaces. """
    if pd.isna(text):
        return "No Message"
    return re.sub(r'\n+', ' ', text).strip()

def clean_text_pipeline(text: str) -> str:
    """
    Orchestrates the text cleaning process by applying multiple cleaning functions.
    """
    if not text:
        logger.info("Received empty input text.")
        return ""

    # Define the Amharic diacritics map
    amharic_diacritics_map = {
        'ኀ': 'ሀ', 'ኁ': 'ሁ', 'ኂ': 'ሂ', 'ኃ': 'ሀ', 'ኄ': 'ሄ', 'ኅ': 'ህ', 'ኆ': 'ሆ',
        'ሐ': 'ሀ', 'ሑ': 'ሁ', 'ሒ': 'ሂ', 'ሓ': 'ሀ', 'ሔ': 'ሄ', 'ሕ': 'ህ', 'ሖ': 'ሆ',
        'ሠ': 'ሰ', 'ሡ': 'ሱ', 'ሢ': 'ሲ', 'ሣ': 'ሳ', 'ሤ': 'ሴ', 'ሥ': 'ስ', 'ሦ': 'ሶ',
        'ዐ': 'አ', 'ዑ': 'ኡ', 'ዒ': 'ኢ', 'ዓ': 'አ', 'ዔ': 'ኤ', 'ዕ': 'እ', 'ዖ': 'ኦ', 'ኣ': 'አ'
    }

    # Apply cleaning steps
    text = normalize_amharic_text(text, amharic_diacritics_map)
    text = remove_non_amharic_characters(text)
    # text = remove_punctuation(text)
    text = remove_emojis(text)
    text = remove_repeated_characters(text)
    text = remove_urls(text)
    # text = remove_numbers(text)
    text = normalize_spaces(text)

    logger.debug("Final text cleaning completed.")
    return text


def clean_dataframe(df):
    """ Perform all cleaning and standardization steps while avoiding SettingWithCopyWarning. """
    try:
        df = df.drop_duplicates(subset=["ID"]).copy()  # Ensure a new copy
        logger.info("Duplicates removed from dataset.")

        # Convert Date to datetime format, replacing NaT with None
        df.loc[:, 'Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df.loc[:, 'Date'] = df['Date'].where(df['Date'].notna(), None)
        logger.info("Date column formatted to datetime.")

        # Convert 'ID' to integer for PostgreSQL BIGINT compatibility
        df.loc[:, 'ID'] = pd.to_numeric(df['ID'], errors="coerce").fillna(0).astype(int)

        # Fill missing values
        df.loc[:, 'Message'] = df['Message'].fillna("No Message")
        df.loc[:, 'Media Path'] = df['Media Path'].fillna("No Media")
        logger.info("Missing values filled.")

        # Standardize text columns
        df.loc[:, 'Channel Title'] = df['Channel Title'].str.strip()
        df.loc[:, 'Channel Username'] = df['Channel Username'].str.strip()
        df.loc[:, 'Message'] = df['Message'].apply(clean_text)
        df.loc[:, 'Media Path'] = df['Media Path'].str.strip()
        logger.info("Text columns standardized.")

        # Extract emojis and store them in a new column
        df.loc[:, 'emoji_used'] = df['Message'].apply(extract_emojis)
        logger.info("Emojis extracted and stored in 'emoji_used' column.")
        
        # Remove emojis from message text
        df.loc[:, 'Message'] = df['Message'].apply(remove_emojis)

        # Extract YouTube links into a separate column
        df.loc[:, 'youtube_links'] = df['Message'].apply(extract_youtube_links)
        logger.info("YouTube links extracted and stored in 'youtube_links' column.")

        # Remove YouTube links from message text
        df.loc[:, 'Message'] = df['Message'].apply(remove_youtube_links)

        # Rename columns to match PostgreSQL schema
        df = df.rename(columns={
            "Channel Title": "channel_title",
            "Channel Username": "channel_username",
            "ID": "message_id",
            "Message": "message",
            "Date": "message_date",
            "Media Path": "media_path",
            "emoji_used": "emoji_used",
            "youtube_links": "youtube_links"
        })

        logger.info("Data cleaning completed successfully.")
        return df
    except Exception as e:
        logger.error(f"Data cleaning error: {e}")
        raise

def save_cleaned_data(df, output_path):
    """ Save cleaned data to a new CSV file. """
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Cleaned data saved successfully to '{output_path}'.")
        print(f"Cleaned data saved successfully to '{output_path}'.")
    except Exception as e:
        logger.error(f"Error saving cleaned data: {e}")
        raise