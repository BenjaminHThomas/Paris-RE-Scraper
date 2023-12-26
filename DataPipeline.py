# This script will be responsible for storing the scraped data in the database.
import mysql.connector
from dotenv import load_dotenv
import os
import logging
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

def save_to_sql(db_name:str, table_name:str, data_list:list, buy_or_rent:str) -> None:
    conn = mysql.connector.connect(
        host = os.getenv('DB_HOST'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
    )

    cur = conn.cursor() ## The cursor is used to execute commands

    try:
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    except mysql.connector.Error as err:
        raise err(f"Cannot connect to SQL database {db_name}, please check .env settings.")

    conn.database = db_name

    queries = {
        'buy':"""
        price DECIMAL,
        price_square_mtr DECIMAL
        """,
        'rent':"""
        monthly_rent DECIMAL
        """
    }

    ## Create the table if it doesn't exist
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
                id int NOT NULL auto_increment,
                {queries[buy_or_rent]},
                size DECIMAL,
                rooms DECIMAL,
                bedrooms DECIMAL,
                bathrooms DECIMAL,
                realtor VARCHAR(255),
                zip_code VARCHAR(255),
                url VARCHAR(255),
                property_id VARCHAR(255),
                timestamp TIMESTAMP,
                PRIMARY KEY (id)
    )
    """)

    columns = {
        'buy': ['price', 'price_square_mtr', 'size', 'rooms', 'bedrooms', 'bathrooms', 'realtor', 'zip_code', 'url', 'property_id', 'timestamp'],
        'rent':['monthly_rent', 'size', 'rooms', 'bedrooms', 'bathrooms', 'realtor', 'zip_code', 'url', 'property_id', 'timestamp']
    }

    values_placeholder = ', '.join(['%s'] * len(columns[buy_or_rent]))
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns[buy_or_rent])}) VALUES ({values_placeholder})"
    
    for data_dict in data_list:
        data_dict['timestamp'] = datetime.datetime.now()
        data_tuple = tuple(data_dict[col] for col in columns[buy_or_rent]) ## convert dictionary of key:value pairs into tuple of values
        cur.execute(insert_query, data_tuple)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

def get_existing_property_ids(db_name:str, table_name:str) -> list:
    conn = mysql.connector.connect(
        host = os.getenv('DB_HOST'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
    )
    cur = conn.cursor() ## The cursor is used to execute commands

    try:
        conn.database = db_name
    except mysql.connector.errors.ProgrammingError as err:
        logging.info(f"Cannot connect to database {db_name} to verify properties. If this is not your first time scraping, please verify .env settings for mysql connection. Returning empty list of pre-existing property id's...")
        return ['']

    try:
        cur.execute(f"SELECT property_id FROM {table_name}")
    except mysql.connector.errors.ProgrammingError:
        logging.info(f"Table '{table_name}' does not exist yet, returning empty list of pre-existing property id's...")
        return ['']
    
    existing_property_ids = [row[0] for row in cur.fetchall()]
    return existing_property_ids
