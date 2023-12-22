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

def save_to_sql(buy_or_rent, data_list):
    conn = mysql.connector.connect(
        host = os.getenv('DB_HOST'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
    )

    cur = conn.cursor() ## The cursor is used to execute commands

    try:
        cur.execute('CREATE DATABASE IF NOT EXISTS paris_RE')
    except mysql.connector.Error as err:
        raise err("Cannot connect to SQL, please check .env settings.")

    conn.database = "paris_RE"

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
    CREATE TABLE IF NOT EXISTS {buy_or_rent}(
                id int NOT NULL auto_increment,
                {queries[buy_or_rent]},
                size DECIMAL,
                rooms DECIMAL,
                bedrooms DECIMAL,
                bathrooms DECIMAL,
                realtor VARCHAR(255),
                zip_code INTEGER,
                url VARCHAR(255),
                timestamp TIMESTAMP,
                PRIMARY KEY (id)
    )
    """)

    columns = {
        'buy': ['price', 'price_square_mtr', 'size', 'rooms', 'bedrooms', 'bathrooms', 'realtor', 'zip_code', 'url', 'timestamp'],
        'rent':['monthly_rent', 'size', 'rooms', 'bedrooms', 'bathrooms', 'realtor', 'zip_code', 'url', 'timestamp']
    }

    values_placeholder = ', '.join(['%s'] * len(columns[buy_or_rent]))
    insert_query = f"INSERT INTO {buy_or_rent} ({', '.join(columns[buy_or_rent])}) VALUES ({values_placeholder})"
    
    # Fetch all existing url's from the table:
    cur.execute(f'SELECT url FROM {buy_or_rent}')
    existing_urls = [row[0] for row in cur.fetchall()]

    for data_dict in data_list:
        if data_dict['url'] not in existing_urls: ## don't insert the data if the url is already present
            data_dict['timestamp'] = datetime.datetime.now()
            data_tuple = tuple(data_dict[col] for col in columns[buy_or_rent]) ## convert dictionary of key:value pairs into tuple of values
            cur.execute(insert_query, data_tuple)
        else: 
            logger.info(f'url already present in database, skipping: {data_dict['url']}')

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

