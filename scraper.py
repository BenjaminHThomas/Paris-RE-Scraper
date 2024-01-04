# This file will contain all of the scrapers for different websites.self.property_links

from seleniumbase import SB
from seleniumbase.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re

from DataPipeline import save_to_sql, get_existing_property_ids, update_record, retrieve_table, flag_delisted, timestamp_update, connect_to_db
import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BieniciScraper():
    def __init__(self) -> None:
        self.db_name = 'paris_RE'
        self.property_links = [] 
        self.base_url = "https://www.bienici.com"
        self.url_extensions = { # 
            'rent':"/recherche/location/paris-75000?page=",
            'buy':"/recherche/achat/paris-75000?page="
        }
        self.tile_selector = "a.detailedSheetLink"
        self.price_header_selector = 'ad-price__the-price'
        self.price_square_mtr_selector = "ad-price__price-per-square-meter"
        self.monthly_rent_selector = 'ad-price__the-price'
        self.details_table_selector = 'allDetails'
        self.section_title_selector = 'section-title'
        self.realtor_selector = 'agency-overview__info-name'
        self.zip_code_selector = 'fullAddress'
        self.cleaned_data_list = []
        self.conn = '' # Will be updated once connection is established
        self.cur = '' # Will be updated once connection is established
        self.buy_or_rent = '' # Will be updated when scrape or update is called

    def choose_table(self, buy_or_rent):
        # sets instance variable to either 'buy' or 'rent' which later determines the sql table name
        if buy_or_rent not in ('rent', 'buy'):
            raise ValueError("Invalid input. Please provide either 'rent' or 'buy' to the scrape function")
        self.buy_or_rent = buy_or_rent 

    def check_driver(self, url, sb, element) -> None:
        ## If the element is not present, resets the chrome driver.
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
    
    def populate_property_list(self, page, sb) -> None:
        target_url = self.base_url + self.url_extensions[self.buy_or_rent] + str(page)
        sb.get(target_url)
        self.check_driver(target_url, sb, self.tile_selector)
        soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
        self.property_links.extend([link.get('href') for link in soup.select(self.tile_selector)])

    def extract_property_id(self, url):
        # Extracts the unique id from the url between '/' and 'q='
        result = re.search(r'/([^/]+?q=)', url)
        if result:
            return result[0]
        else: return None

    def purge_duplicates(self) -> None:
        # Checks whether property id already exists in SQL & removes from to-scrape list (property_links)
        existing_property_ids = get_existing_property_ids(table_name=f'bien_ici_{self.buy_or_rent}',
                                                          cur = self.cur,
                                                          conn =self.conn)
        initial_len = len(self.property_links)
        self.property_links = [x for x in self.property_links if self.extract_property_id(x) not in existing_property_ids]
        new_len = len(self.property_links)
        logger.info(f"{initial_len-new_len} duplicates removed, proceeding...")

    def extract_details_rental(self, soup) -> str:
        monthly_rent = soup.find('span', class_=self.monthly_rent_selector)
        monthly_rent = monthly_rent.get_text(strip=True) if monthly_rent else ''
        return monthly_rent

    def extract_details_buy(self, soup) -> str:
        price = soup.find(class_=self.price_header_selector)
        price = price.get_text(strip = True) if price else ''
        price_square_mtr = soup.find(class_=self.price_square_mtr_selector)
        price_square_mtr = price_square_mtr.get_text(strip = True) if price_square_mtr else ''
        return price, price_square_mtr

    def extract_property_details(self, property_link, sb, target_url=False) -> dict:
        if not target_url: # If no target url is supplied, it will be taken from the property_link
            target_url = self.base_url+property_link
        logger.info(f"Starting next url...\n{target_url}")

        try:
            sb.get(target_url)
        except TimeoutException:
            self.check_driver(target_url,sb,'.'+self.price_header_selector)

        self.check_driver(target_url, sb, '.'+self.price_header_selector)
        page_source = sb.get_page_source() 
        soup = BeautifulSoup(page_source, 'html.parser')

        ## Property details table
        all_details_div = soup.find('div', class_=self.details_table_selector)
        size = all_details_div.find('div', string=lambda t: 'm²' in t if t else False)
        size = size.get_text(strip=True).replace(",",".") if size else ''
        rooms = all_details_div.find('div', string=lambda t: 'pièce' in t if t else False)
        rooms = rooms.get_text(strip=True) if rooms else ''
        bedrooms = all_details_div.find('div', string=lambda t: 'chambre' in t if t else False)
        bedrooms = bedrooms.get_text(strip=True) if bedrooms else ''
        realtor = soup.find('div', class_=self.realtor_selector)
        realtor = realtor.get_text(strip=True) if realtor else ''
        zip_code = soup.find('span', class_=self.zip_code_selector)
        zip_code = zip_code.get_text(strip=True) if zip_code else ''
        bathrooms = all_details_div.find('div', string=lambda t: (' WC' in t or 'salle de bain' in t or "salle d'eau" in t) if t else False)
        bathrooms = bathrooms.get_text(strip=True) if bathrooms else '' 
        floor = all_details_div.find('div', string=lambda t: 'étage' in t if t else False)
        floor = floor.get_text(strip=True) if floor else ''
        removed = soup.find('div', class_=self.section_title_selector)
        removed = removed.get_text(strip=True).replace('’', '') == 'Cette annonce nest plus disponible.' if removed else False # this header explains that the listing is no longer available.

        if self.buy_or_rent == 'buy':
            price, price_square_mtr = self.extract_details_buy(soup)
            monthly_rent = ''
        elif self.buy_or_rent == 'rent':
            monthly_rent = self.extract_details_rental(soup)
            price, price_square_mtr = '',''

        return {
            'price': price,
            'price_square_mtr': price_square_mtr,
            'monthly_rent': monthly_rent,
            'size': size,
            'rooms': rooms,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'floor': floor,
            'removed': removed,
            'realtor': realtor,
            'zip_code': zip_code,
            'url': target_url
        }
    
    def extract_zip_code(self, zip_code) -> str:
        match = re.search(r'\b75\d{3}\b', zip_code)
        return match.group() if match else None
    
    def clean_price_range(self, price_str) -> float:
        ## To be used when a price range is given for a property.
        ## For example: "495 000 à 2 100 000 €" or "500 - 1000"
        cleaned_prices = re.split('à|-', price_str) # à is French for "to". à and - suggest a range of prices.
        cleaned_prices = [re.sub(r"[^\d.]", "", num) for num in cleaned_prices] # remove non-digits
        cleaned_prices = [float(num) for num in cleaned_prices if num and float(num) > 0] # convert digits to floats
        average_price = sum(cleaned_prices) / len(cleaned_prices) 
        return average_price
    
    def clean_numeric(self, value) -> float:
        value = value.replace("\xa0", "")
        return float(re.sub(r"[^\d.]", "", value)) if value else None
    
    def extract_floor_number(self, floor_string) -> int:
        # Extracts first number that has an "e" attached to it from string. e.g., "3e étage (sur 6)" would extract 3.
        pattern = r'\b(\d+)e\b'
        match = re.search(pattern, floor_string)
        if match:
            return int(match.group(1))
        else:
            return None

    def clean_data(self, property_details_dict, update = False) -> dict:
        price_square_mtr = self.clean_numeric(property_details_dict.get('price_square_mtr','').replace(",", "."))
        if "k" in property_details_dict['price_square_mtr']:
            price_square_mtr *= 1000
        zip_code = self.extract_zip_code(property_details_dict.get('zip_code',''))

        price = property_details_dict.get('price','')
        if any(char in price for char in ['à','-']): # if a range of prices is given
            price = self.clean_price_range(price) # will return the average price of the range
        else:
            price = self.clean_numeric(price)

        cleaned_data = {
                'price': price,
                'price_square_mtr': price_square_mtr,
                'monthly_rent': self.clean_numeric(property_details_dict.get('monthly_rent','')),
                'size': self.clean_numeric(property_details_dict.get('size','')),
                'rooms': self.clean_numeric(property_details_dict.get('rooms','')),
                'bedrooms': self.clean_numeric(property_details_dict.get('bedrooms','')),
                'bathrooms': self.clean_numeric(property_details_dict.get('bathrooms','')),
                'floor': self.extract_floor_number(property_details_dict.get('floor',None)),
                'removed': property_details_dict.get('removed', False),
                'realtor': property_details_dict.get('realtor',''),
                'zip_code': str(zip_code) if zip_code else None,
                'url':property_details_dict.get('url'),
                'property_id':self.extract_property_id(property_details_dict.get('url'))
            }
        
        if not update:
            self.cleaned_data_list.append(cleaned_data)
        else:
            return cleaned_data

    def print_results(self) -> None:
        logger.info("Formatted scraping results:")
        for key, value in self.cleaned_data_list[-1].items():
            logger.info(f"{key}: {value}")

    def process_data(self) -> None:
        # Saves the scraped data in SQL
        save_to_sql(table_name= f'bien_ici_{self.buy_or_rent}', 
                    data_list= self.cleaned_data_list, 
                    buy_or_rent= self.buy_or_rent, 
                    cur = self.cur, 
                    conn = self.conn)
        self.cleaned_data_list = [] 

    def validate_limit(self, url_string, page_num) -> bool:
        ## If there's only 50 pages and you enter page 100 into the url it will go to page 50
        ## this functions checks if you've run out of pages to scrape.
        url_page_num = url_string[-len(str(page_num)):]
        return url_page_num == str(page_num)

    def scrape(self, buy_or_rent) -> None:
        self.choose_table(buy_or_rent)

        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db(self.db_name)
            ## Populate list of property url's
            for x in range(1,settings.property_page_limit + 1):
                logger.info(f"Scraping property to {self.buy_or_rent} listings from page {x} of BienIci...")
                self.populate_property_list(x, sb)
                current_url = sb.get_current_url()
                if not self.validate_limit(current_url, x):
                    break

            ## Remove pre-existing properties from property list before commencing scraping
            self.purge_duplicates()    
            
            ## Loop through property urls and extract details of each one
            logger.info(f"Commencing the scraping of {buy_or_rent} properties...")
            for x in range(len(self.property_links)):
                property_details_dict = self.extract_property_details(self.property_links[x], sb)
                self.clean_data(property_details_dict)
                if settings.print_results:
                    self.print_results()
                ## Save results to database every 5 properties
                if x % 5 == 0 and x > 0:
                    self.process_data()

        if self.cleaned_data_list: # if there's any remaining results at the end, insert them into the table
            self.process_data()

        self.property_links = [] # remove properties that have been logged
        logger.info("BienIci scraper finished.")
        self.cur.close()
        self.conn.close()

    def update_row(self, row, property_dict) -> None:
        ## If a value has changed, update the record in MySQL
        if property_dict.get('removed') == True:
            # Don't update all values because they may now be null & I want to preserve the data.
            logger.info(f'removed property found:\n{row["url"]}')
            flag_delisted(f'bien_ici_{self.buy_or_rent}', row["id"],
                          cur = self.cur, conn = self.conn)
            return

        column_dict = {
            'buy':['price','price_square_mtr'],
            'rent':['monthly_rent']
        }
        cols_to_check = column_dict.get(self.buy_or_rent)
        cols_to_check += ['size','rooms','bedrooms','bathrooms','floor','removed']
        for col in cols_to_check:
            if property_dict.get(col) != row[col] and property_dict.get(col) is not None: # If a value has changed, update the row.
                logger.info(f'New value found:\n url:{row["url"]} \nold {col}: {row[col]}\nnew {col}: {property_dict.get(col)}')
                update_record(f'bien_ici_{self.buy_or_rent}', row["id"], property_dict, self.buy_or_rent,
                              cur = self.cur, conn = self.conn)

    def update_table(self, buy_or_rent) -> None:
        # Goes through existing records to see if they've been updated or delisted.
        self.choose_table(buy_or_rent) # assign instance variable.

        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db(self.db_name)
            df = retrieve_table(db_name=self.db_name,
                                table_name=f'bien_ici_{self.buy_or_rent}',
                                cur = self.cur, conn = self.conn)
            df = df[df['removed'] == 0].sort_values(by=['updated','timestamp'], ascending=True)

            for _, row in df.iterrows():
                property_dict = self.extract_property_details(property_link=None,
                                                                    sb = sb,
                                                                    target_url=row['url']
                                                                )
                cleaned_property_dict = self.clean_data(property_dict, update=True)
                self.update_row(row, cleaned_property_dict)
                timestamp_update(f'bien_ici_{buy_or_rent}', row["id"], self.cur, self.conn)
        self.cur.close()
        self.conn.close()
