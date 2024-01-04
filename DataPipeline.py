# This script will be responsible for storing the scraped data in the database.
import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
import logging
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

host = os.getenv('DB_HOST')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

def connect_to_db(db_name:str):
    logger.info(f'Connecting to database...')
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
    )
    cur = conn.cursor() ## The cursor is used to execute commands

    try:
        cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    except mysql.connector.Error as err:
        raise err(f"Cannot connect to SQL database {db_name}, please check .env settings.")

    conn.database = db_name

    return cur, conn

def save_to_sql(table_name:str, data_list:list, buy_or_rent:str, cur, conn) -> None:
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
                floor INT UNSIGNED,
                realtor VARCHAR(255),
                zip_code VARCHAR(255),
                url VARCHAR(255),
                property_id VARCHAR(255),
                timestamp TIMESTAMP,
                removed BOOLEAN,
                updated TIMESTAMP,
                PRIMARY KEY (id)
    )
    """)

    columns = {
        'buy': ['price', 'price_square_mtr', 'size', 'rooms', 'bedrooms', 'bathrooms', 'floor', 'realtor', 'zip_code', 'url', 'property_id', 'timestamp','removed','updated'],
        'rent':['monthly_rent', 'size', 'rooms', 'bedrooms', 'bathrooms', 'floor', 'realtor', 'zip_code', 'url', 'property_id', 'timestamp','removed','updated']
    }

    values_placeholder = ', '.join(['%s'] * len(columns[buy_or_rent]))
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns[buy_or_rent])}) VALUES ({values_placeholder})"
    dup_count = 0 # Count of duplicate property_id's
    
    for data_dict in data_list:
        data_dict['timestamp'] = datetime.datetime.now()

        # check if property_id already exists in the table
        cur.execute(f"SELECT EXISTS(SELECT * FROM {table_name} WHERE property_id = %s)", (data_dict['property_id'],))
        exists = cur.fetchone()[0]

        if not exists: # only insert if property_id isn't already in table
            data_tuple = tuple(data_dict[col] for col in columns[buy_or_rent]) ## convert dictionary of key:value pairs into tuple of values
            cur.execute(insert_query, data_tuple)
        else:
            dup_count += 1
    
    logger.info(f"{dup_count} duplicates removed prior to insertion...") # I need to remove duplicates twice as sometimes the URL changes and they slip through the first check.
    conn.commit()

def get_existing_property_ids(table_name:str, cur, conn) -> list:
    try:
        cur.execute(f"SELECT property_id FROM {table_name}")
    except mysql.connector.errors.ProgrammingError:
        logger.info(f"Table '{table_name}' does not exist yet, returning empty list of pre-existing property id's...")
        return ['']
    
    existing_property_ids = [row[0] for row in cur.fetchall()]
    return existing_property_ids

def retrieve_table(db_name:str, table_name:str, cur, conn) -> pd.DataFrame:
    logger.info(f'Retrieving table "{table_name}" from {db_name}...')
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db_name}")
    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(query, engine)
    return df

def update_record(table_name, id, property_dict, buy_or_rent, cur, conn) -> None:
    columns = {
        'buy': ['price', 'price_square_mtr', 'size', 'rooms', 'bedrooms', 'bathrooms', 'floor', 'realtor', 'zip_code', 'url','removed'],
        'rent':['monthly_rent', 'size', 'rooms', 'bedrooms', 'bathrooms', 'floor', 'realtor', 'zip_code', 'url','removed']
    }
    # Generate the SET part of the UPDATE query using dictionary keys and placeholders
    set_clause = ', '.join([f"{column} = %({column})s" for column in columns.get(buy_or_rent)])
    # Update the specific row
    update_query = f"UPDATE {table_name} SET {set_clause} WHERE id = %(id)s"
    property_dict['id'] = id # add in id to the property details
    cur.execute(update_query, property_dict)
    conn.commit() # Commit the changes
    logger.info(f'Property {id} in {table_name} updated successfully...')

def flag_delisted(table_name, id, cur, conn) -> None:
    # Flags a property as delisted.
    update_query = f'UPDATE {table_name} SET removed = TRUE WHERE id = {id}'
    cur.execute(update_query)
    conn.commit()
    logger.info(f'Row with ID {id} in {table_name} flagged as delisted successfully...')

def timestamp_update(table_name, id, cur, conn) -> None:
    # records the time when the record was last checked for updates.
    update_query = f'UPDATE {table_name} SET updated = %s where id = %s'
    cur.execute(update_query, (datetime.datetime.now(), id))
    conn.commit()
    logger.info('Record update timestamped...\n')