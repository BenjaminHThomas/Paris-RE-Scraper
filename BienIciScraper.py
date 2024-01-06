from seleniumbase import SB
from seleniumbase.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re
from typing import Callable # type hinting functions as inputsr
import settings 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from DataPipeline import save_to_sql, get_existing_property_ids, update_record, retrieve_table, flag_delisted, timestamp_update, connect_to_db
import BaseScraper

class _BaseBienIci(BaseScraper._baseScraper):
    def __init__(self, buy_or_rent: str) -> None:
        super().__init__(buy_or_rent)
        self.property_features = ['size','rooms','bedrooms','bathrooms','floor','removed']
        self.db_name = 'paris_re'
        self.base_url = "https://www.bienici.com"
        self.url_extension = {'rent':"/recherche/location/paris-75000?page=",'buy':"/recherche/achat/paris-75000?page="}.get(buy_or_rent)
        self.tile_selector = "a.detailedSheetLink"
        self.details_table_selector = 'allDetails'
        self.section_title_selector = 'section-title'
        self.realtor_selector = 'agency-overview__info-name'
        self.zip_code_selector = 'fullAddress'

    def _check_driver(self, url: str, sb: Callable, element: str) -> None:
        return super()._check_driver(url, sb, element)

    def _populate_property_list(self, page:int, sb:Callable) -> None:
        target_url = self.base_url + self.url_extension + str(page)
        sb.get(target_url)
        self._check_driver(target_url, sb, self.tile_selector)
        soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
        self.property_links.extend([link.get('href') for link in soup.select(self.tile_selector)])

    def _extract_property_id(self, url:str) -> str:
        # Extracts the unique id from the url between '/' and 'q='
        result = re.search(r'/([^/]+?q=)', url)
        if result:
            return result[0]
        else: return None

    def _purge_duplicates(self, table_name:str) -> None:
        # Checks whether property id already exists in SQL & removes from to-scrape list (property_links)
        existing_property_ids = get_existing_property_ids(table_name=table_name,
                                                          cur = self.cur,
                                                          conn =self.conn)
        initial_len = len(self.property_links)
        self.property_links = [x for x in self.property_links if self._extract_property_id(x) not in existing_property_ids]
        new_len = len(self.property_links)
        logger.info(f"{initial_len - new_len} duplicates removed, proceeding...")

    def _clean_numeric(self, value:str) -> float:
        value = value.replace("\xa0", "")
        return float(re.sub(r"[^\d.]", "", value)) if value else None
    
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
        logger.info(f"Starting next url...\n{target_url}")

        try:
            sb.get(target_url)
        except TimeoutException:
            self._check_driver(target_url,sb,'.'+self.details_table_selector)

        self._check_driver(target_url, sb, '.'+self.details_table_selector)
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
    
    def _process_data(self, table_name:str) -> None:
        # Saves the scraped data in SQL
        save_to_sql(table_name= table_name, 
                    data_list= self.cleaned_data_list, 
                    buy_or_rent= self.buy_or_rent, 
                    cur = self.cur, 
                    conn = self.conn)
        self.cleaned_data_list = [] 
    
    def scrape(self, table_name:str) -> None:
        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db(self.db_name)
            ## Populate list of property url's
            for x in range(1,settings.property_page_limit + 1):
                logger.info(f"Scraping {self.buy_or_rent} property listings from page {x} of BienIci...")
                self._populate_property_list(x, sb)
                current_url = sb.get_current_url()
                if not super()._validate_limit(current_url, x):
                    break
            
            ## Remove pre-existing properties from property list before commencing scraping
            self._purge_duplicates(table_name)

            ## Loop through property urls and extract details of each one
            logger.info(f"Commencing the scraping of {self.buy_or_rent} properties...")
            for x in range(len(self.property_links)):
                property_details_dict = self._extract_property_details(self.property_links[x], sb)
                self._clean_data(property_details_dict, update = False)
                if settings.print_results:
                    self._print_results(property_details_dict)
                ## Save results to database every 5 properties
                if x % 5 == 0 and x > 0:
                    self._process_data(table_name)

        if self.cleaned_data_list: # if there's any remaining results at the end, insert them into the table
            self._process_data(table_name)

        self.property_links = [] # remove properties that have been logged
        logger.info("BienIci scraper finished.")
        self.cur.close()
        self.conn.close()


class BienIciRent(_BaseBienIci):
    def __init__(self) -> None:
        super().__init__(buy_or_rent='rent')
        self.property_features.append('monthly_rent')
        self.monthly_rent_selector = 'ad-price__the-price'
        self.table_name = 'bien_ici_rent'

    def _extract_property_details(self, property_link: str, sb: Callable, target_url=False) -> dict:
        property_dict, soup = super()._extract_property_details(property_link, sb, target_url)
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
        
    def scrape(self) -> None:
        return super().scrape(table_name = self.table_name)
    
    def update_table(self) -> None:
        return super().update_table(exctract_func = self._extract_property_details, clean_func = self._clean_data)

class BienIciBuy(_BaseBienIci):
    def __init__(self) -> None:
        super().__init__(buy_or_rent='buy')
        self.property_features += ['price','price_square_mtr']
        self.price_header_selector = 'ad-price__the-price'
        self.price_square_mtr_selector = "ad-price__price-per-square-meter"
        self.table_name = 'bien_ici_buy'

    def _extract_property_details(self, property_link: str, sb: Callable, target_url=False) -> dict:
        property_dict, soup = super()._extract_property_details(property_link, sb, target_url)
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
        
    def scrape(self) -> None:
        return super().scrape(table_name = self.table_name)
    
    def update_table(self) -> None:
        return super().update_table(exctract_func = self._extract_property_details, clean_func = self._clean_data)


    
    
    

