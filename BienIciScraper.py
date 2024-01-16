from seleniumbase import SB
from seleniumbase.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re
from typing import Callable # type hinting functions as inputs
import settings 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from DataPipeline import save_to_sql, get_field_as_list, connect_to_db
import BaseScraper

class _BaseBienIci(BaseScraper._baseScraper):
    def __init__(self, buy_or_rent: str) -> None:
        super().__init__(buy_or_rent)
        self.base_url = "https://www.bienici.com"
        self.property_features = ['size', 'rooms', 'bedrooms', 'bathrooms', 'floor', 'realtor', 'zip_code', 'url', 'property_id', 'timestamp','removed']
        self.tile_selector = "a.detailedSheetLink"
        self.details_table_selector = 'allDetails'
        self.section_title_selector = 'section-title'
        self.realtor_selector = 'agency-overview__info-name'
        self.zip_code_selector = 'fullAddress'

    def _check_driver(self, url:str, sb:Callable, element:str) -> bool:
        """
        url: a url string
        sb: the web browser SB from seleniumbase
        element: a string representing an HTML element (class or id)
        """
        try:
            sb.wait_for_element_present(element, timeout=10)
        except NoSuchElementException:
            # Check whether url still exists or if it's a dead link. Checks whether unique id is still present to determine this.
            actual_url = sb.get_current_url()
            if self._extract_property_id(url) and self._extract_property_id(url) not in actual_url:
                logger.info(f'URL is no longer valid, skipping...')
                return False

            for _ in range(settings.max_retry+1):
                if sb.is_element_present(element):
                    break
                logger.warning("Retrying with new driver...")
                #sb.close() ## deprecated, need to find way to close previous browser
                #sb.get_new_driver(undetectable = True)
                sb.get(url)
                sb.sleep(3 + random.random())
            if not sb.is_element_present(element):
                raise ConnectionError(f"Error: Unable to find element '{element}'. Please check proxy settings...")
        return True

    def _populate_property_list(self, page:int, sb:Callable) -> None:
        target_url = self.base_url + self.url_extension + str(page)
        sb.get(target_url)
        if not self._check_driver(target_url, sb, self.tile_selector):
            return
        soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
        self.property_links.extend([link.get('href') for link in soup.select(self.tile_selector)])

    def _extract_property_id(self, url:str) -> str:
        # Extracts the unique id from the url between '/' and 'q='
        result = re.search(r'/([^/]+?q=)', url)
        if result:
            return result[0]
        else: return ''

    def _purge_duplicates(self) -> None:
        # Checks whether property id already exists in SQL & removes from to-scrape list (property_links)
        existing_property_ids = get_field_as_list(table_name=self.table_name,
                                                        column_name='property_id',
                                                          cur = self.cur)
        initial_len = len(self.property_links)
        self.property_links = [x for x in self.property_links if self._extract_property_id(x) not in existing_property_ids]
        new_len = len(self.property_links)
        logger.info(f"{initial_len - new_len} duplicates removed, proceeding...")
    
    def _extract_floor_number(self, floor_string:str) -> int:
        # Extracts first number that has an "e" attached to it from string. e.g., "3e étage (sur 6)" would extract 3.
        pattern = r'\b(\d+)e\b'
        match = re.search(pattern, floor_string)
        if match:
            return int(match.group(1))
        else:
            return None

    def _extract_property_details(self, property_link:str, sb:Callable, target_url=False) -> dict:
        if not target_url:
            target_url = self.base_url+property_link
        logger.info(f"\n\nStarting next url...\n{target_url}")

        try:
            sb.get(target_url)
        except TimeoutException:
            logger.info('Target url timed out, trying again...')

        if not self._check_driver(target_url, sb, '.'+self.details_table_selector):
            return None, None
        
        page_source = sb.get_page_source() 
        soup = BeautifulSoup(page_source, 'html.parser')

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

        return {
            'size': size,
            'rooms': rooms,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'floor': floor,
            'removed': removed,
            'realtor': realtor,
            'zip_code': zip_code,
            'url': target_url
        }, soup
    
    def _clean_data(self, property_details_dict: dict, update:bool) -> dict:
        zip_code = self._extract_zip_code(property_details_dict.get('zip_code',''))
        cleaned_data = {
                'size': self._clean_numeric(property_details_dict.get('size','')),
                'rooms': self._clean_numeric(property_details_dict.get('rooms','')),
                'bedrooms': self._clean_numeric(property_details_dict.get('bedrooms','')),
                'bathrooms': self._clean_numeric(property_details_dict.get('bathrooms','')),
                'floor': self._extract_floor_number(property_details_dict.get('floor',None)),
                'removed': property_details_dict.get('removed', False),
                'realtor': property_details_dict.get('realtor',''),
                'zip_code': str(zip_code) if zip_code else None,
                'url':property_details_dict.get('url'),
                'property_id':self._extract_property_id(property_details_dict.get('url'))
            }
        return cleaned_data
    
    def _process_data(self) -> None:
        query_insert = {
        'buy':"""
        price DECIMAL,
        price_square_mtr DECIMAL
        """,
        'rent':"""
        monthly_rent DECIMAL
        """}.get(self.buy_or_rent)
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}(
                id int NOT NULL auto_increment,
                {query_insert},
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
            """

        # Saves the scraped data in SQL
        save_to_sql(table_name= self.table_name, 
                    create_query = create_query,
                    columns = self.property_features,
                    property_dict_list= self.cleaned_data_list,  
                    uid_column='property_id',
                    cur = self.cur, 
                    conn = self.conn)
        self.cleaned_data_list = [] 
    
    def scrape(self) -> None:
        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db()
            ## Populate list of property url's
            for x in range(1,settings.property_page_limit + 1):
                keyword = 'sale' if self.buy_or_rent == 'buy' else 'rent'
                logger.info(f"Scraping properties for {keyword} from page {x} of BienIci...")
                self._populate_property_list(x, sb)
                current_url = sb.get_current_url()
                # Checks whether the current page number is below what is should be, indicating that we've run out of pages to scrape.
                if not super()._validate_limit(current_url, x):
                    break
            
            ## Remove pre-existing properties from property list before commencing scraping
            self._purge_duplicates()

            ## Loop through property urls and extract details of each one
            keyword = 'sale' if self.buy_or_rent == 'buy' else 'rent'
            logger.info(f"Commencing the scraping of properties for {keyword}...")
            for x in range(len(self.property_links)):
                property_details_dict = self._extract_property_details(self.property_links[x], sb)
                self._clean_data(property_details_dict, update = False)
                if settings.print_results:
                    self._print_results(property_details_dict)
                ## Save results to database every 5 properties
                if x % 5 == 0 and x > 0:
                    self._process_data()

        if self.cleaned_data_list: # if there's any remaining results at the end, insert them into the table
            self._process_data()

        self.property_links = [] # remove properties that have been logged
        logger.info("BienIci scraper finished.")
        self.cur.close()
        self.conn.close()


class BienIciRent(_BaseBienIci):
    def __init__(self) -> None:
        super().__init__(buy_or_rent='rent')
        self.property_features.insert(0,'monthly_rent')
        self.monthly_rent_selector = 'ad-price__the-price'
        self.url_extension = "/recherche/location/paris-75000?page="
        self.table_name = 'bien_ici_rent'

    def _extract_property_details(self, property_link: str, sb: Callable, target_url=False) -> dict:
        property_dict, soup = super()._extract_property_details(property_link = property_link, sb = sb, target_url = target_url)
        if not property_dict: # if url is invalid and details can't be extracted, return nothing
            return {}
        monthly_rent = soup.find('span', class_=self.monthly_rent_selector)
        monthly_rent = monthly_rent.get_text(strip=True) if monthly_rent else ''
        property_dict['monthly_rent'] = monthly_rent
        return property_dict

    def _clean_data(self, property_details_dict:dict, update:bool) -> dict:
        cleaned_data = super()._clean_data(property_details_dict, update)
        cleaned_data['monthly_rent'] = super()._clean_numeric(property_details_dict.get('monthly_rent',''))
        if not update:
            self.cleaned_data_list.append(cleaned_data)
        else:
            return cleaned_data
    
    def update_table(self) -> None:
        return super().update_table(exctract_func = self._extract_property_details, clean_func = self._clean_data)


class BienIciBuy(_BaseBienIci):
    def __init__(self) -> None:
        super().__init__(buy_or_rent='buy')
        self.property_features[:0] = ['price','price_square_mtr']
        self.price_header_selector = 'ad-price__the-price'
        self.price_square_mtr_selector = "ad-price__price-per-square-meter"
        self.url_extension = "/recherche/achat/paris-75000?page="
        self.table_name = 'bien_ici_buy'

    def _extract_property_details(self, property_link: str, sb: Callable, target_url=False) -> dict:
        property_dict, soup = super()._extract_property_details(property_link, sb, target_url)
        if not property_dict: # if url is invalid and details can't be extracted, return nothing
            return None
        price = soup.find(class_=self.price_header_selector)
        price = price.get_text(strip = True) if price else ''
        price_square_mtr = soup.find(class_=self.price_square_mtr_selector)
        price_square_mtr = price_square_mtr.get_text(strip = True) if price_square_mtr else ''
        property_dict['price'] = price
        property_dict['price_square_mtr'] = price_square_mtr
        return property_dict

    def _clean_data(self, property_details_dict: dict, update: bool) -> dict:
        cleaned_data = super()._clean_data(property_details_dict, update)

        price = property_details_dict.get('price','')
        if any(char in price for char in ['à','-']):
            price = super()._clean_price_range(price)
        else:
            price = self._clean_numeric(price)

        price_square_mtr = property_details_dict.get('price_square_mtr','').replace(",", ".") # remove annoying use of ',' rather than '.' by RE agents before proceeding
        price_square_mtr = self._clean_numeric(price_square_mtr)
        if "k" in property_details_dict.get('price_square_mtr',''):
            price_square_mtr *= 1000

        cleaned_data['price'] = price
        cleaned_data['price_square_mtr'] = price_square_mtr
        
        if not update:
            self.cleaned_data_list.append(cleaned_data)
        else:
            return cleaned_data
    
    def update_table(self) -> None:
        return super().update_table(exctract_func = self._extract_property_details, clean_func = self._clean_data)


    
    
    

