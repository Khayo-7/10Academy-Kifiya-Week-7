import re
import os
import sys
import emoji
import pandas as pd
from typing import Dict, List

# Setup logger for cleaning operations
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger

logger = setup_logger("cleaning")

# ==========================================
# Helper Functions for Cleaning Operations
# ==========================================

# Amharic diacritics normalization map
AMHARIC_DIACRITICS_MAP = {
    'ኀ': 'ሀ', 'ኁ': 'ሁ', 'ኂ': 'ሂ', 'ኃ': 'ሀ', 'ኄ': 'ሄ', 'ኅ': 'ህ', 'ኆ': 'ሆ',
    'ሐ': 'ሀ', 'ሑ': 'ሁ', 'ሒ': 'ሂ', 'ሓ': 'ሀ', 'ሔ': 'ሄ', 'ሕ': 'ህ', 'ሖ': 'ሆ',
    'ሠ': 'ሰ', 'ሡ': 'ሱ', 'ሢ': 'ሲ', 'ሣ': 'ሳ', 'ሤ': 'ሴ', 'ሥ': 'ስ', 'ሦ': 'ሶ',
    'ዐ': 'አ', 'ዑ': 'ኡ', 'ዒ': 'ኢ', 'ዓ': 'አ', 'ዔ': 'ኤ', 'ዕ': 'እ', 'ዖ': 'ኦ', 'ኣ': 'አ'
}

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
    """Remove non-Amharic characters (retain Amharic script and numbers)."""

    if not text:
        return text
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

def extract_emojis(text: str) -> str:
    """Extract emojis from the text."""
    if not text:
        return "No Emoji"
    emojis = ''.join(c for c in text if c in emoji.EMOJI_DATA)
    return emojis if emojis else "No Emoji"

def remove_emojis(text):
    """Remove emojis from the text."""
    if not text:
        return text
    return ''.join(c for c in text if c not in emoji.EMOJI_DATA)

# def remove_emojis(text: str) -> str:
#     """
#     Removes emojis from the text.
#     """
#     emoji_pattern = re.compile(
#         "["
#         "\U0001F600-\U0001F64F"
#         "\U0001F300-\U0001F5FF"
#         "\U0001F680-\U0001F6FF"
#         "\U0001F700-\U0001F77F"
#         "\U0001F780-\U0001F7FF"
#         "\U0001F800-\U0001F8FF"
#         "\U0001F900-\U0001F9FF"
#         "\U0001FA00-\U0001FA6F"
#         "\U0001FA70-\U0001FAFF"
#         "\U00002702-\U000027B0"
#         "\U000024C2-\U0001F251"
#         "]+", flags=re.UNICODE
#     )
#     result = emoji_pattern.sub('', text)
#     logger.debug("Removed emojis.")
#     return result

def remove_repeated_characters(text: str) -> str:
    """Remove repeated characters."""
    if not text:
        return text
    return re.sub(r'(.)\1+', r'\1', text)

def remove_urls(text: str) -> str:
    """Remove all URLs from the text."""
    if not text:
        return text
    return re.sub(r'http\S+|www\S+', '', text)

def extract_links(text: str) -> List[str]:
    """Extract all links from the text."""
    if not text:
        return []
    url_pattern = r'(https?://\S+|www\.\S+)'
    # youtube_pattern = r"(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+)"
    return re.findall(url_pattern, text)

def normalize_links(text: str) -> str:
    """Normalize URLs in the text."""
    if not text:
        return text
    
    # Remove tracking parameters from URLs
    return re.sub(r'(https?://[^\s]+)\?[^\s]*', r'\1', text)

def normalize_spaces(text: str) -> str:
    """Normalize multiple spaces and trim the text."""
    if not text:
        return text
    return ' '.join(text.split()).strip()
    # return re.sub(r'\s+', ' ', text).strip()

def clean_text(text):
    """ Standardize text by removing newline characters and unnecessary spaces. """
    if pd.isna(text):
        return "No Message"
    return re.sub(r'\n+', ' ', text).strip()

def combine_group_messages(messages: List[str]) -> str:
    """Combine messages from a group into a single text."""
    return "\n".join(msg.strip() for msg in messages if msg.strip())