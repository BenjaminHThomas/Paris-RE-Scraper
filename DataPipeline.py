# This script will be responsible for storing the scraped data in the database.
import mysql.connector
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from the .env file
load_dotenv()

def save_to_sql(table_name):
    conn = mysql.connector.connect(
        host = os.getenv('DB_HOST'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
    )

    cur = conn.cursor() ## The cursor is used to execute commands
    try:
        cur.execute(f"USE paris_RE")
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            cur.execute(f"CREATE DATABASE paris_RE")
            logger.info("Created missing paris_RE database")
            conn.database = "paris_RE"

    cur.execute(f"DROP TABLE IF EXISTS {table_name}") ## Temporary, only to be used during testing.

    ## Create the database if it doesn't exist
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name}(
                id int NOT NULL auto_increment,
                price DECIMAL,
                price_square_mtr DECIMAL,
                size DECIMAL,
                rooms DECIMAL,
                bedrooms DECIMAL,
                sold_by VARCHAR(255),
                zip_code INTEGER,
                PRIMARY KEY (id)
    )
    """)
    
save_to_sql()