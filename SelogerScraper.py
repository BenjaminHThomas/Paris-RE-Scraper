from seleniumbase import SB
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re
from typing import Callable
from DataPipeline import update_record, retrieve_table, flag_delisted, timestamp_update, save_to_sql, connect_to_db, get_field_as_list
from unidecode import unidecode
import settings
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from BaseScraper import _baseScraper

class _BaseSeloger(_baseScraper):
    def __init__(self, buy_or_rent: str) -> None:
        super().__init__(buy_or_rent)
        self.property_features = ['property_type','size','rooms','bedrooms','floor','balcony','elevator','parking','zip_code','url','timestamp','removed']
        self.tile_link_selector = 'a.sc-bJHhxl.ceSuox'
        self.tile_selector = '.sc-bvTASY.byzQLE'
        self.tile_list = [] # a list containing the tiles for each property on the page
        self.property_type_selector = 'jxkWqO'
        self.details_selector = 'ul' # a ul containing li for each property feature
        self.zip_code_selector = 'eqIQiZ'
        self.property_details = [] # a list of dictionaries containing the property features, values

    def _check_captcha(self, sb) -> bool:
        soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
        captcha_frame = soup.find('iframe', src=lambda x: x and 'captcha' in x)
        if captcha_frame:
            input('Please complete the captcha and type any key in the terminal to continue...')
    
    def _classify_list_items(self, li_text:str):
        match li_text:
            case _ if 'piece' in unidecode(li_text.lower()):
                return super()._clean_numeric(li_text), 'rooms'
            case _ if 'chambre' in li_text.lower():
                return super()._clean_numeric(li_text), 'bedrooms'
            case _ if 'm²' in li_text:
                return super()._clean_numeric(li_text), 'size'
            case _ if 'etage' in unidecode(li_text.lower()):
                return self._extract_floor_number(li_text), 'floor'
            case _ if any(keyword in unidecode(li_text.lower()) for keyword in ('balcon', 'terrasse')):
                return True, 'balcony'
            case _ if 'parking' in unidecode(li_text.lower()):
                return True, 'parking'
            case _ if 'ascenseur' in unidecode(li_text.lower()):
                return True, 'elevator'
            case _:
                return None, None
    
    def _check_driver(self, element, sb, target_url) -> None:
        for _ in range(settings.max_retry):
            try:
                sb.wait_for_element_present(element, timeout=12)
                return
            except:
                logger.info(f'{element} was not present, trying again...')
                sb.get(target_url)

    def _extract_floor_number(self, floor_string:str) -> int:
        ## extracts the first number in a string and returns an int
        match = re.search(r'\d+', floor_string)
        if match: return int(match.group())
        else: return None

    def _process_html_ul(self, ul:str) -> dict:
        property_details_dict = {
            'rooms':None,
            'bedrooms':None,
            'size':None,
            'floor':None,
            'balcony':False,
            'parking':False,
            'elevator':False,
        }
        for li in ul.findAll('li'):
            val, key = self._classify_list_items(li.get_text())
            if val:
                property_details_dict[key] = val
        return property_details_dict

    def _process_data(self):
        query_insert = {
        'buy':"""
        price DECIMAL,
        price_square_mtr DECIMAL,
        """,
        'rent':"""
        monthly_rent DECIMAL,
        """}.get(self.buy_or_rent)

        create_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}(
                id int NOT NULL auto_increment,
                property_type VARCHAR(255),
                {query_insert}
                size DECIMAL,
                rooms DECIMAL,
                bedrooms DECIMAL,
                floor INT UNSIGNED,
                balcony BOOLEAN,
                elevator BOOLEAN,
                parking BOOLEAN,
                zip_code VARCHAR(255),
                url VARCHAR(255),
                timestamp TIMESTAMP,
                removed BOOLEAN,
                updated TIMESTAMP,
                PRIMARY KEY (id)
            )
            """
        save_to_sql(table_name=self.table_name,
                    create_query=create_query,
                    columns=self.property_features,
                    property_dict_list=self.property_details,
                    uid_column='url',
                    cur=self.cur,
                    conn = self.conn
                    )
        if settings.print_results:
                    for prop_dict in self.property_details:
                        super()._print_results(prop_dict)
                        print('\n')
        self.property_details = []


    def _scrape_page(self, page:int, sb:Callable):
        logger.info(f'Scraping page {page} of Seloger...')
        dups = 0 # for counting duplicate pages
        existing_urls = get_field_as_list(table_name = self.table_name,
                                            column_name = 'url',
                                            cur = self.cur)
        
        target_url = self.base_url + str(page)
        sb.get(target_url)
        current_url = sb.get_current_url()
        current_page = re.search(r'pg=(\d+)', current_url).group(1)
        if str(current_page) != str(page):
            logger.info(f'Ran out of valid pages, attempted to scrape page {page}, but connected to page {current_page}\nurl:{current_url}')
            return
        self._check_captcha(sb)
        self._check_driver(self.tile_selector, sb, target_url)
        soup = BeautifulSoup(sb.get_page_source(), 'html.parser')

        property_links = [link.get('href') for link in soup.select(self.tile_link_selector)]
        property_links = ['https://www.seloger.com' + x if not x.startswith('https:') else x for x in property_links]
        
        self.tile_list = [x for x in soup.select(self.tile_selector)]

        for x in range(len(self.tile_list)):
            link = property_links[x]
            if link in existing_urls:
                dups += 1
                pass
    
            property_type = self.tile_list[x].find('div', class_ = self.property_type_selector)
            if property_type:
                property_type = property_type.get_text(strip = True)
                
            zip_code = self.tile_list[x].find('div', class_ = self.zip_code_selector)
            if zip_code:
                zip_code = super()._extract_zip_code(zip_code.get_text())

            property_ul = self.tile_list[x].find(self.details_selector)
            if property_ul:
                prop_dict = self._process_html_ul(property_ul)
                prop_dict['url'] = link
                prop_dict['zip_code'] = zip_code
                prop_dict['property_type'] = property_type
                self.property_details.append(prop_dict)
            else:
                logger.info('No detail list found...')

        logger.info(f'{dups} duplicate properties skipped. {(dups/len(property_links))*100}% of total.')


class SelogerRent(_BaseSeloger):
    def __init__(self) -> None:
        super().__init__(buy_or_rent = 'rent')
        self.property_features.insert(1, 'monthly_rent')
        self.monthly_rent_selector = 'ccntto'
        self.base_url = 'https://www.seloger.com/immobilier/achat/75/?projects=1&places=[{%22subDivisions%22%3A[%2275%22]}]&mandatorycommodities=0&enterprise=0&qsVersion=1.0&LISTING-LISTpg='
        self.table_name = 'seloger_rent'
        
    def scrape(self):
        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db()
            for x in range(1,settings.property_page_limit):
                self._scrape_page(x,sb)
                # if not self.tile_list:
                #     break # If there's no more properties to scrape, finish the script.
                for x in range(len(self.tile_list)):
                    div = self.tile_list[x]
                    rent = div.find('div', class_ = self.monthly_rent_selector)
                    rent = super()._clean_numeric(rent.get_text())
                    self.property_details[x]['monthly_rent'] = rent
                self._process_data()
            logger.info(f'Seloger scraper finished :^)')


class SelogerBuy(_BaseSeloger):
    def __init__(self) -> None:
        super().__init__(buy_or_rent = 'buy')
        self.property_features.insert(1,'price')
        self.property_features.insert(2,'price_square_mtr')
        self.base_url = 'https://www.seloger.com/immobilier/achat/75/?LISTING-LISTpg='
        self.table_name = 'seloger_buy'
        self.price_selector = 'ccntto'
        self.price_square_mtr_selector = 'eyLVpC'

    def _get_prices(self, div) -> float:
        assert div is not None, "Div is None"
        assert len(div) > 0, "Div is Empty"
        price_str = div.find('div', class_=self.price_selector)
        price_str = price_str.get_text()
        if any(char in price_str for char in ['à','-']):
            price = super()._clean_price_range(price_str)
        else:
            price = super()._clean_numeric(price_str)
        return price
    
    def _get_price_per_metre(self, div) -> float:
        price_mtr_str = div.find('div', class_ = self.price_square_mtr_selector)
        if price_mtr_str:
            price_mtr_str = price_mtr_str.get_text()
        else:
            return None
        
        if 'k' in price_mtr_str:
            price_mtr = super()._clean_numeric(price_mtr_str) * 1000
        else:
            price_mtr = super()._clean_numeric(price_mtr_str) 
        return price_mtr
    
    def scrape(self):
        with SB(uc=True, headless=settings.headless, demo=settings.demo_mode) as sb:
            self.cur, self.conn = connect_to_db()
            for x in range(1,settings.property_page_limit):
                self._scrape_page(x,sb)
                # if not self.tile_list:
                #     break # If there's no more properties to scrape, finish the script.
                for x in range(len(self.tile_list)):
                    price = self._get_prices(self.tile_list[x])
                    price_mtr = self._get_price_per_metre(self.tile_list[x])
                    self.property_details[x]['price'] = price
                    self.property_details[x]['price_square_mtr'] = price_mtr
                self._process_data()
            logger.info(f'Seloger scraper finished :^)')


           