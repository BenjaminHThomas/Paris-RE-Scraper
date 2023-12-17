# This script will be responsible for storing the scraped data in the database.
import mysql.connector
from dotenv import load_dotenv
import os
import logging

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
        cur.execute(f"USE paris_RE")
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            cur.execute(f"CREATE DATABASE paris_RE")
            logger.info("Created missing paris_RE database")
            conn.database = "paris_RE"

    cur.execute(f"DROP TABLE IF EXISTS {buy_or_rent}") ## Temporary, only to be used during testing.

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
                PRIMARY KEY (id)
    )
    """)

    columns = {
        'buy': ['price', 'price_square_mtr', 'size', 'rooms', 'bedrooms', 'bathrooms', 'realtor', 'zip_code', 'url'],
        'rent':['monthly_rent', 'size', 'rooms', 'bedrooms', 'bathrooms', 'realtor', 'zip_code', 'url']
    }

    values_placeholder = ', '.join(['%s'] * len(columns[buy_or_rent]))
    insert_query = f"INSERT INTO {buy_or_rent} ({', '.join(columns[buy_or_rent])}) VALUES ({values_placeholder})"

    # Prepare the data as a list of tuples
    data_tuples = [tuple(data_dict[col] for col in columns[buy_or_rent]) for data_dict in data_list]
    
    # Execute the bulk insert
    cur.executemany(insert_query, data_tuples)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

# buy = [{'price': 1100000.0,
# 'price_square_mtr': 10000.0,
# 'monthly_rent': None,
# 'size': 105.0,
# 'rooms': 6.0,
# 'bedrooms': 4.0,
# 'bathrooms': 2.0,
# 'realtor': 'DE FERLA IMMOBILIER',
# 'zip_code': 75014,
# 'url':'aaaaa.com',
# }]
#
# rent = [{'price': None,
# 'price_square_mtr': None,
# 'monthly_rent': 1600,
# 'size': 105.0,
# 'rooms': 6.0,
# 'bedrooms': 4.0,
# 'bathrooms': 2.0,
# 'realtor': 'DE FERLA IMMOBILIER',
# 'zip_code': 75014,
# 'url':'aaaaa.com',
# }]
#
# save_to_sql('rent', rent)