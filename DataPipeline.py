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

def connect_to_db():
    logger.info(f'Connecting to database...')
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
    )
    cur = conn.cursor() ## The cursor is used to execute commands

    try:
        cur.execute(f'CREATE DATABASE IF NOT EXISTS paris_re')
    except mysql.connector.Error as err:
        raise err(f"Cannot connect to SQL database 'paris_re', please check .env settings.")

    conn.database = 'paris_re'

    return cur, conn

def save_to_sql(table_name:str, create_query:str, columns:list, property_dict_list:list, uid_column:str, cur, conn) -> None:
    '''
    inputs:
        table_name: name of table in MySQL,
        create_query: a query string that will create the SQL table if it doesn't exist
        columns: a list containing the columns that will have data entered into them.
        property_dict_list: a list of dictionaries, each containing the info of scraped properties
        uid_column: the column that acts as the unique id for entries
        cur = sql cursor
        conn = sql connection
    '''
    cur.execute(create_query)


    values_placeholder = ', '.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_placeholder})"
    dup_count = 0 # Count of duplicate property_id's
    
    for data_dict in property_dict_list:
        data_dict['timestamp'] = datetime.datetime.now()
        if 'removed' not in data_dict:
            data_dict['removed'] = False
        if 'updated' not in data_dict:
            data_dict['updated'] = None
            
        # check if unique id already exists in the table
        exists = check_duplicate(cur, table_name, uid_column, data_dict[uid_column])

        if not exists: # only insert if unique id isn't already in table
            data_tuple = tuple(data_dict[col] for col in columns) ## convert dictionary of key:value pairs into tuple of values
            cur.execute(insert_query, data_tuple)
        else:
            dup_count += 1
    
    logger.info(f"{dup_count} duplicates removed prior to insertion...") # I need to remove duplicates twice as sometimes the URL changes and they slip through the first check.
    conn.commit()

def check_duplicate(cur, table_name:str, uid_column:str, uid_value:str) -> bool:
    '''
    inputs:
        cur: sql cursor
        table_name: sql table name
        uid_column: name of column containing the unique id
        uid_value: the unique value being checked to see if it's already stored
    '''
    cur.execute(f"SELECT EXISTS(SELECT * FROM {table_name} WHERE {uid_column} = %s)", (uid_value,))
    if cur.fetchone()[0]:
        return True
    return False
    

#def get_existing_property_ids(table_name:str, cur, conn) -> list:
def get_field_as_list(table_name:str, column_name:str, cur) -> list:
    try:
        cur.execute(f"SELECT {column_name} FROM {table_name}")
    except mysql.connector.errors.ProgrammingError:
        logger.info(f"Table '{table_name}' does not exist yet, returning empty list of pre-existing property id's...")
        return ['']
    
    existing_property_ids = [row[0] for row in cur.fetchall()]
    return existing_property_ids

def retrieve_table(table_name:str) -> pd.DataFrame:
    logger.info(f'Retrieving table "{table_name}" from paris_re...')
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/paris_re")
    query = f'SELECT * FROM {table_name}'
    df = pd.read_sql(query, engine)
    return df

def update_record(table_name, id, property_dict, columns, cur, conn) -> None:
    # Generate the SET part of the UPDATE query using dictionary keys and placeholders
    columns = [x for x in columns if x not in ('removed','updated','timestamp')]
    set_clause = ', '.join([f"{column} = %({column})s" for column in columns])
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
    logger.info('Property update timestamped...\n')