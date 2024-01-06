from seleniumbase import SB
from seleniumbase.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re
from typing import Callable
from DataPipeline import save_to_sql, get_existing_property_ids, update_record, retrieve_table, flag_delisted, timestamp_update, connect_to_db
import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class _baseScraper():
    def __init__(self, buy_or_rent:str) -> None:
        self.buy_or_rent = self._choose_table(buy_or_rent)
        self.property_links = []
        self.cleaned_data_list = []
        self.property_features = []
        self.db_name = ''
        self.table_name = ''
        self.conn = '' 
        self.cur = ''
    
    def _choose_table(self, buy_or_rent:str) -> str:
        # sets instance variable to either 'buy' or 'rent' which later determines the sql table name
        if buy_or_rent not in ('rent', 'buy'):
            raise ValueError("Invalid input. Please provide either 'rent' or 'buy' to the scrape function")
        return buy_or_rent 

    def _check_driver(self, url:str, sb:Callable, element:str) -> None:
        """
        url: a url string
        sb: the web browser SB from seleniumbase
        element: a string representing an HTML element (class or id)
        """
        try:
            sb.wait_for_element_present(element, timeout=10)
        except NoSuchElementException:
            for _ in range(settings.max_retry+1):
                if sb.is_element_present(element):
                    break
                logger.warning("Retrying with new driver...")
                #sb.close() ## deprecated, need to find way to close previous browser
                sb.get_new_driver(undetectable = True)
                sb.get(url)
                sb.sleep(3 + random.random())
            if not sb.is_element_present(element):
                raise ConnectionError(f"Error: Unable to find element '{element}'. Please check proxy settings...")
    
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

    def _validate_limit(self, url_string, page_num) -> bool:
        ## If there's only 50 pages and you enter page 100 into the url it will go to page 50
        ## this functions checks if you've run out of pages to scrape.
        url_page_num = url_string[-len(str(page_num)):]
        return url_page_num == str(page_num)

    def _update_row(self, row, cleaned_data:dict):
        ## If a value has changed, update the record in MySQL
        if cleaned_data.get('removed') == True:
            # Don't update all values because they may now be null & I want to preserve the data.
            logger.info(f'removed property found:\n{row["url"]}')
            flag_delisted(self.table_name, row["id"],
                          cur = self.cur, conn = self.conn)
            return
        
        for column in self.property_features:
            if cleaned_data.get(column) != row[column] and cleaned_data.get(column) is not None: # If a value has changed, update the row.
                logger.info(f'New value found:\n url:{row["url"]} \nold {column}: {row[column]}\nnew {column}: {cleaned_data.get(column)}')
                update_record(table_name = self.table_name,
                              id = row['id'],
                              property_dict = cleaned_data,
                              buy_or_rent = self.buy_or_rent,
                              cur = self.cur, conn = self.conn)

    def update_table(self, exctract_func:Callable, clean_func:Callable) -> None:
        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db(self.db_name)
            df = retrieve_table(db_name = self.db_name,
                                table_name = self.table_name,
                                 cur = self.cur, conn = self.conn)
            df = df[df['removed'] == 0].sort_values(by=['updated','timestamp'], ascending=True)

            for _, row in df.iterrows():
                property_dict = exctract_func(None, sb, target_url=row['url'])
                cleaned_data = clean_func(property_dict, update=True)
                self._update_row(row, cleaned_data)
                timestamp_update(table_name = self.table_name,
                                 id = row['id'],
                                 cur = self.cur, conn = self.conn)
                
                

            
            