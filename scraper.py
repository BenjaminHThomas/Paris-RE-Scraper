# This file will contain all of the scrapers for different websites.

from seleniumbase import SB
from seleniumbase.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BieniciScraper():
    def __init__(self) -> None:
        self.tiles = []
        self.base_url = "https://www.bienici.com"
        self.url_ext = {
            'rent':"/recherche/location/paris-75000?page=",
            'buy':"/recherche/achat/paris-75000?page="
        }
        self.tile_selector = "a.detailedSheetLink"
        self.price_header_selector = 'ad-price__the-price'
        self.price_square_mtr_selector = "ad-price__price-per-square-meter"
        self.monthly_rent_selector = 'ad-price__the-price'
        self.details_table_selector = 'allDetails'
        self.realtor_selector = 'agency-overview__info-name'
        self.zip_code_selector = 'fullAddress'
        self.cleaned_data_list = []
    
    def check_driver(self, url, sb, element) -> None:
        ## If the elements are not present, resets the chrome driver. Maximum 5 times.
        try:
            sb.wait_for_element_present(element, timeout=10)
        except NoSuchElementException:
            for _ in range(6):
                if sb.is_element_present(element):
                    break
                logging.warning("Retrying with new driver...")
                #sb.close() ## deprecated, need to find way to close previous browser
                sb.get_new_driver(undetectable = True)
                sb.get(url)
                sb.sleep(3 + random.random)
            if not sb.is_element_present(element):
                logging.warning(f"Error: Unable to find element '{element}'. Please check proxy settings...")
    
    def populate_property_list(self, page, sb) -> None:
        target_url = self.base_url + self.url_ext[self.buy_or_rent] + str(page)
        sb.get(target_url)
        self.check_driver(target_url, sb, self.tile_selector)
        soup = BeautifulSoup(sb.get_page_source(), 'html.parser')
        self.tiles.extend([link.get('href') for link in soup.select(self.tile_selector)])

    def extract_details_rental(self, soup):
        monthly_rent = soup.find('span', class_=self.monthly_rent_selector)
        monthly_rent = monthly_rent.get_text(strip=True) if monthly_rent else ''
        return monthly_rent

    def extract_details_buy(self, soup):
        ## Header details
        price = soup.find(class_=self.price_header_selector).text 
        price_square_mtr = soup.find(class_=self.price_square_mtr_selector).text
        return price, price_square_mtr

    def extract_property_details(self, property_link, sb) -> dict:
        target_url = self.base_url+property_link
        logger.info(f'Scraping: {target_url}')
        sb.get(target_url)
        self.check_driver(target_url, sb, '.'+self.price_header_selector)
        page_source = sb.get_page_source() 
        soup = BeautifulSoup(page_source, 'html.parser')

        ## Property details table
        all_details_div = soup.find('div', class_=self.details_table_selector)
        size = all_details_div.find('div', string=lambda t: 'm²' in t if t else False)
        size = size.get_text(strip=True) if size else ''
        rooms = all_details_div.find('div', string=lambda t: 'pièce' in t if t else False)
        rooms = rooms.get_text(strip=True) if rooms else ''
        bedrooms = all_details_div.find('div', string=lambda t: 'chambre' in t if t else False)
        bedrooms = bedrooms.get_text(strip=True) if bedrooms else ''
        realtor = soup.find('div', class_=self.realtor_selector)
        realtor = realtor.get_text(strip=True) if realtor else ''
        zip_code = soup.find('span', class_=self.zip_code_selector)
        zip_code = zip_code.get_text(strip=True) if zip_code else ''
        bathrooms = all_details_div.find('div', string=lambda t: ' WC' in t if t else False)
        bathrooms = bathrooms.get_text(strip=True) if bathrooms else '1' # safe to assume that unless listed, the property has 1 bathroom

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
            'realtor': realtor,
            'zip_code': zip_code,
            'url': target_url,
        }
    
    def extract_zip_code(self, zip_code) -> str:
        match = re.search(r'\b75\d{3}\b', zip_code)
        return match.group() if match else None
    
    def clean_numeric(self, value) -> float:
        value = value.replace("\xa0", "")
        return float(re.sub(r"[^\d.]", "", value)) if value else None

    def clean_data(self, property_details_dict) -> None:
        price_square_mtr = self.clean_numeric(property_details_dict.get('price_square_mtr','').replace(",", "."))
        if "k" in property_details_dict['price_square_mtr']:
            price_square_mtr *= 1000
        zip_code = self.extract_zip_code(property_details_dict.get('zip_code',''))

        # self.tiles.extend([link.get('href') for link in soup.select(self.tile_selector)])
        self.cleaned_data_list.extend({
            'price': self.clean_numeric(property_details_dict.get('price','').replace(" ", "")[:-1]),
            'price_square_mtr': price_square_mtr,
            'monthly_rent': self.clean_numeric(property_details_dict.get('monthly_rent','')),
            'size': self.clean_numeric(property_details_dict.get('size','')),
            'rooms': self.clean_numeric(property_details_dict.get('rooms','')),
            'bedrooms': self.clean_numeric(property_details_dict.get('bedrooms','')),
            'bathrooms': self.clean_numeric(property_details_dict.get('bathrooms','')),
            'realtor': property_details_dict.get('realtor',''),
            'zip_code': int(zip_code) if zip_code is not None else None,
            'url':property_details_dict.get('url'),
        })

    def print_results(self):
        logger.info("Formatted scraping results:")
        for key, value in self.cleaned_data_list[-1].items():
            logger.info(f"{key}: {value}")

    def scrape(self, buy_or_rent) -> None:
        if buy_or_rent not in ('rent', 'buy'):
            raise ValueError("Invalid input. Please provide either 'rent' or 'buy' to the scrape function")
        self.buy_or_rent = buy_or_rent # might reconsider this line, defining instance variables outside init isn't best practice

        with SB(uc=True, headless=True, demo=False) as sb:
            for x in range(1,2):
                ## There's 100 pages in BienIci
                self.populate_property_list(x, sb)
            for property_link in self.tiles:
                property_details_dict = self.extract_property_details(property_link, sb)
                self.clean_data(property_details_dict)
                self.print_results()

myinstance = BieniciScraper()
myinstance.scrape('buy')
