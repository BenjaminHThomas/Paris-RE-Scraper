from seleniumbase import SB
import random
random.seed(1)
import logging
import re
from typing import Callable
from DataPipeline import update_record, retrieve_table, flag_delisted, timestamp_update, connect_to_db
import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class _baseScraper():
    def __init__(self, buy_or_rent:str) -> None:
        self.buy_or_rent = self._choose_table(buy_or_rent)
        self.db_name = 'paris_re'
        self.property_links = [] # list of properties to scrape
        self.cleaned_data_list = [] # list of dictionaries containing cleaned property details
        self.property_features = [] # list of features being scraped: e.g., rooms, bedrooms, size etc.
        self.table_name = '' # sql table name
        self.conn = '' # sql connection
        self.cur = '' # sql cursor
    
    def _choose_table(self, buy_or_rent:str) -> str:
        # sets instance variable to either 'buy' or 'rent' which later determines the sql table name
        if buy_or_rent not in ('rent', 'buy'):
            raise ValueError("Invalid input. Please provide either 'rent' or 'buy'")
        return buy_or_rent 
    
    def _clean_numeric(self, value:str) -> float:
        value = value.replace("\xa0", "")
        value = re.sub(r"[^\d.]", "", value)
        if value.replace('.','').isnumeric():
            return float(value) if value else None
        else:
            return None

    def _extract_zip_code(self, zip_code_str:str):
        match = re.search(r'\b75\d{3}\b', zip_code_str)
        return match.group() if match else None
    
    def _clean_price_range(self, price_str:str):
        ## To be used when a price range is given for a property.
        ## For example: "495 000 à 2 100 000 €" or "500 - 1000"
        cleaned_prices = re.split('à|-', price_str) # à is French for "to". à and - suggest a range of prices.
        cleaned_prices = [re.sub(r"[^\d.]", "", num) for num in cleaned_prices] # remove non-digits
        cleaned_prices = [float(num) for num in cleaned_prices if num and float(num) > 0] # convert digits to floats
        average_price = sum(cleaned_prices) / len(cleaned_prices) 
        return average_price
    
    def _print_results(self, results_dict:dict) -> None:
        logger.info("Formatted scraping results:")
        for key, value in results_dict.items():
            logger.info(f"{key}: {value}")

    def _validate_limit(self, url_string:str, page_num:int) -> bool:
        ## If there's only 50 pages and you enter page 100 into the url it will go to page 50
        ## this functions checks if you've run out of pages to scrape.
        url_string = re.sub(r'[^a-zA-Z0-9]', '', url_string)
        url_page_num = url_string[-len(str(page_num)):]
        return url_page_num == str(page_num)

    def _update_row(self, row, cleaned_data:dict):
        ## If a value has changed, update the record in MySQL
        if cleaned_data.get('removed') == True:
            # Don't update all values because they may now be null & I want to preserve the data.
            logger.info(f'removed property found...')
            flag_delisted(self.table_name, row["id"],
                          cur = self.cur, conn = self.conn)
            return
        
        for column in self.property_features:
            if cleaned_data.get(column) != row[column] and cleaned_data.get(column) is not None: # If a value has changed, update the row.
                logger.info(f'New value found:\n url:{row["url"]} \nold {column}: {row[column]}\nnew {column}: {cleaned_data.get(column)}')
                update_record(table_name = self.table_name,
                              id = row['id'],
                              property_dict = cleaned_data,
                              columns = self.property_features,
                              cur = self.cur, conn = self.conn)

    def update_table(self, exctract_func:Callable, clean_func:Callable) -> None:
        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db()
            df = retrieve_table(table_name = self.table_name)
            if len(df):
                df = df[df['removed'] == 0].sort_values(by=['updated','timestamp'], ascending=True)
            else:
                logger.info(f'{self.table_name} not found or is empty...')
                return

            for _, row in df.iterrows():
                property_dict = exctract_func(property_link = None, sb = sb, target_url=row['url'])
                if not property_dict: # If a URL is no longer valid and there's no delisted message, mark the property as delisted.
                    flag_delisted(self.table_name, row['id'], self.cur, self.conn)
                else:
                    cleaned_data = clean_func(property_dict, update=True)
                    self._update_row(row, cleaned_data)
                    timestamp_update(table_name = self.table_name,
                                 id = row['id'],
                                 cur = self.cur, conn = self.conn)
                
                

            
            