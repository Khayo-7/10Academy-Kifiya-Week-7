import os
import sys
import psycopg2
# import pandas as pd
# from dotenv import load_dotenv
# from sqlalchemy import create_engine

# Setup logger for data_loader
sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
from scripts.utils.logger import setup_logger
from scripts.data_utils.loaders import load_json


logger = setup_logger("database")

# load_dotenv()

# # Database connection settings
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT")
# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")


CONFIG_PATH = os.path.join('..', 'resources', 'configs')
config_filepath = os.path.join(CONFIG_PATH, 'config.json')
config = load_json(config_filepath)

DB_NAME = config['DB_NAME']
DB_USER = config['DB_USER']
DB_PASSWORD = config['DB_PASSWORD']
DB_HOST = config['DB_HOST']
DB_PORT = config['DB_PORT']

# def execute_queries_sqlalchemy(query, database_type='postgres'):
#     """
#     Executes SQL queries using SQLAlchemy.
#     Args:
#         query (str): SQL query to execute.
#         database_type (str): Type of database to connect to. Defaults to 'postgres'.
#     Returns:
#         pd.DataFrame: Result of the query.
#     """
#     if database_type == 'postgres':
#         engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
#     elif database_type == 'mysql':
#         engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}")
#     else:
#         raise ValueError("Unsupported database type. Please use 'postgres' or 'mysql'.")
#     return pd.read_sql_query(query, engine)

# def export_to_database_sqlalchemy(data, table_name, database_type='postgres'):
#     """
#     Exports data to a database table using SQLAlchemy.
#     Args:
#         data (pd.DataFrame): DataFrame to export.
#         table_name (str): Name of the Database table.
#         database_type (str): Type of database to export to. Defaults to 'postgres'.
#     """
#     if database_type == 'mysql':
#         engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:3306/{DB_NAME}")
#     else:
#         engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
#     with engine.connect() as connection:
#         data.to_sql(table_name, con=connection, if_exists="replace", index=False)
#         logger.info(f"Data exported to {table_name} table in {DB_NAME} database using SQLAlchemy.")

# def create_table_sqlalchemy():
#     """
#     Creates a table in the database using SQLAlchemy if it doesn't exist.
#     """
#     query = '''
#                 CREATE TABLE IF NOT EXISTS messages (
#                     id SERIAL PRIMARY KEY,
#                     channel_username VARCHAR(255),
#                     message_id BIGINT,
#                     text TEXT,
#                     media_path TEXT,
#                     media_type VARCHAR(50),
#                     created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
#                 );
#             '''
    
#     execute_queries_sqlalchemy(query)
#     logger.info("Table created or already exists.")
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd


# Load environment variables
load_dotenv("../.env")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

def get_db_connection():
    """ Create and return database engine. """
    try:
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))  # Test connection
        logging.info("✅ Successfully connected to the PostgreSQL database.")
        return engine
    except Exception as e:
        logging.error(f"❌ Database connection failed: {e}")
        raise


def create_table(engine):
    """ Create telegram_messages table if it does not exist. """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS telegram_messages (
        id SERIAL PRIMARY KEY,
        channel_title TEXT,
        channel_username TEXT,
        message_id BIGINT UNIQUE,
        message TEXT,
        message_date TIMESTAMP,
        media_path TEXT,
        emoji_used TEXT,       -- New column for extracted emojis
        youtube_links TEXT     -- New column for extracted YouTube links
    );
    """
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(create_table_query))
        logging.info("✅ Table 'telegram_messages' created successfully.")
    except Exception as e:
        logging.error(f"❌ Error creating table: {e}")
        raise


def insert_data(engine, cleaned_df):
    """ Inserts cleaned Telegram data into PostgreSQL database. """
    try:
        # Convert NaT timestamps to None (NULL in SQL)
        cleaned_df["message_date"] = cleaned_df["message_date"].apply(lambda x: None if pd.isna(x) else str(x))

        insert_query = """
        INSERT INTO telegram_messages 
        (channel_title, channel_username, message_id, message, message_date, media_path, emoji_used, youtube_links) 
        VALUES (:channel_title, :channel_username, :message_id, :message, :message_date, :media_path, :emoji_used, :youtube_links)
        ON CONFLICT (message_id) DO NOTHING;
        """

        with engine.begin() as connection:  # ✅ Auto-commit enabled
            for _, row in cleaned_df.iterrows():
                # Debug log to ensure data is being inserted
                logging.info(f"Inserting: {row['message_id']} - {row['message_date']}")

                connection.execute(
                    text(insert_query),
                    {
                        "channel_title": row["channel_title"],
                        "channel_username": row["channel_username"],
                        "message_id": row["message_id"],
                        "message": row["message"],
                        "message_date": row["message_date"],  # ✅ No NaT values
                        "media_path": row["media_path"],
                        "emoji_used": row["emoji_used"],
                        "youtube_links": row["youtube_links"]
                    }
                )

        logging.info(f"✅ {len(cleaned_df)} records inserted into PostgreSQL database.")
    except Exception as e:
        logging.error(f"❌ Error inserting data: {e}")
        raise


def export_to_database(data, table_name):
    """
    Exports data to a database table using psycopg2.
    Args:
        data (pd.DataFrame): DataFrame to export.
        table_name (str): Name of the Database table.
    """
    conn = create_connection()
    cursor = conn.cursor()
    for index, row in data.iterrows():
        cursor.execute(f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(row))})", row)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info(f"Data exported to {table_name} table in {DB_NAME} database using psycopg2.")

def create_connection():
    """
    Creates a connection to the database using psycopg2.
    Returns:
        psycopg2.extensions.connection: Connection object.
    """
    try:
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    except psycopg2.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        raise

# Connect to PostgreSQL and execute
def execute_queries(query, params=None):
    """
    Executes SQL queries using psycopg2.
    Args:
        query (str): SQL query to execute.
        params (tuple): Parameters to pass to the query.
    """
    conn = create_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        logger.info("Query executed successfully.")
    except Exception as e:
        logger.error(f"Error while executing query: {e}")
    finally:
        cursor.close()
        conn.close()

# Connect to PostgreSQL and create table if it doesn't exist
def create_table():    
    # Create table if it doesn't exist
    query = '''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    channel_username VARCHAR(255),
                    message_id BIGINT,
                    text TEXT,
                    media_path TEXT,
                    media_type VARCHAR(50),
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            '''
    
    execute_queries(query)

    logger.info("Table created or already exists.")

# Save message to PostgreSQL database
def save_to_database(channel_username, message_id, text, media_path, media_type):
    """
    Saves a message to the PostgreSQL database.

    Args:
        channel_username (str): The username of the channel the message is from.
        message_id (int): The unique identifier of the message.
        text (str): The text content of the message.
        media_path (str): The path to the media file associated with the message.
        media_type (str): The type of media associated with the message.

    Raises:
        Exception: If an error occurs while saving the message to the database.
    """
    try:
        query = '''
            INSERT INTO messages (channel_username, message_id, text, media_path, media_type)
            VALUES (%s, %s, %s, %s, %s)
        '''
        params = (channel_username, message_id, text, media_path, media_type)
        execute_queries(query, params)
        logger.info(f"Saved message {message_id} from {channel_username} to the database.")
    except Exception as e:
        logger.error(f"Error saving message from {channel_username}: {e}")
