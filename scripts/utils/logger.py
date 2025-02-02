import os
import logging

# Configure logging to write to file & display in Jupyter Notebook
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler("../logs/data_cleaning.log"),  # Log to file
#         logging.StreamHandler()  # Log to Jupyter Notebook output
#     ]
# )

def setup_logger(log_file_name, log_dir=None):  
    """
    Sets up a logger that writes different log levels to separate files.
    - INFO and higher go to an 'info.log' file.
    - WARNING and higher go to a 'warning.log' file.
    - ERROR and higher go to an 'error.log' file.
    """
    if not log_dir:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs')

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Define log format
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Create the logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Capture all levels
    # logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create handlers for different log levels
    info_handler = logging.FileHandler(os.path.join(log_dir, f"{log_file_name}_info.log"))
    info_handler.setLevel(logging.INFO)

    warning_handler = logging.FileHandler(os.path.join(log_dir, f"{log_file_name}_warning.log"))
    warning_handler.setLevel(logging.WARNING)

    error_handler = logging.FileHandler(os.path.join(log_dir, f"{log_file_name}_error.log"))
    error_handler.setLevel(logging.ERROR)

    # Console  handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)


    # Define formatter with emojis
    class EmojiFormatter(logging.Formatter):
        def format(self, record):
            if record.levelno == logging.DEBUG:
                record.levelname = "[DEBUG]"
                # record.levelname = "[🐛 DEBUG]"
            elif record.levelno == logging.INFO:
                record.levelname = "[INFO]"
                # record.levelname = "[✅ INFO]"
            elif record.levelno == logging.WARNING:
                record.levelname = "[WARNING]"
                # record.levelname = "[⚠️ WARNING]"
            elif record.levelno == logging.ERROR:
                record.levelname = "[ERROR]"
                # record.levelname = "[❌ ERROR]"
            elif record.levelno == logging.CRITICAL:
                record.levelname = "[CRITICAL]"
                # record.levelname = "[🚨 CRITICAL]"
            return super().format(record)
        
    # Define formatter and Apply formatter with emojis to handlers
    formatter = EmojiFormatter(fmt=log_format, datefmt=date_format)
    # formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

    # Apply formatter to handlers
    for handler in [info_handler, warning_handler, error_handler, console_handler]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger