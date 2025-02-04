from datetime import date
import os
import sys
import joblib
import logging
import numpy as np
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

try:
    from scripts.utils.logger import setup_logger
except ImportError as e:
    logging(f"Import error: {e}. Please check the module path.")

# Setup logger for deployement
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
logger = setup_logger("deployement", log_dir)  

def preprocess_input(data: pd.DataFrame) -> np.ndarray:

    logger.info("Starting preprocessing of input data...")

    logger.info("Preprocessing complete.")
    return data

