# This file will contain all of the scrapers for different websites.

from seleniumbase import SB
from seleniumbase.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import random
random.seed(1)
import logging
import re

from DataPipeline import save_to_sql
import settings

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
        ## If the element is not present, resets the chrome driver.
        try:
            sb.wait_for_element_present(element, timeout=10)
        except NoSuchElementException:
            for _ in range(settings.max_retry+1):
                if sb.is_element_present(element):
                    break
                logging.warning("Retrying with new driver...")
                #sb.close() ## deprecated, need to find way to close previous browser
                sb.get_new_driver(undetectable = True)
                sb.get(url)
                sb.sleep(3 + random.random())
            if not sb.is_element_present(element):
                raise ConnectionError(f"Error: Unable to find element '{element}'. Please check proxy settings...")
    
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
        price = soup.find(class_=self.price_header_selector)
        price = price.get_text(strip = True) if price else ''
        price_square_mtr = soup.find(class_=self.price_square_mtr_selector)
        price_square_mtr = price_square_mtr.get_text(strip = True) if price_square_mtr else ''
        return price, price_square_mtr

    def extract_property_details(self, property_link, sb) -> dict:
        target_url = self.base_url+property_link
        logger.info(f"Starting next url...\n{target_url}\n")
        sb.get(target_url)
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
        return int(match.group()) if match else None
    
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

    def clean_data(self, property_details_dict) -> None:
        price_square_mtr = self.clean_numeric(property_details_dict.get('price_square_mtr','').replace(",", "."))
        if "k" in property_details_dict['price_square_mtr']:
            price_square_mtr *= 1000
        zip_code = self.extract_zip_code(property_details_dict.get('zip_code',''))

        price = property_details_dict.get('price','')
        if any(char in price for char in ['à','-']): # if a range of prices is given
            price = self.clean_price_range(price) # will return the average price of the range
        else:
            price = self.clean_numeric(price)

        # self.tiles.extend([link.get('href') for link in soup.select(self.tile_selector)])
        self.cleaned_data_list.append({
            'price': price,
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

    def print_results(self) -> None:
        logger.info("Formatted scraping results:")
        for key, value in self.cleaned_data_list[-1].items():
            logger.info(f"{key}: {value}")

    def process_data(self) -> None:
        # Saves the scraped data in SQL
        logger.info(f"Savings {len(self.cleaned_data_list)} results to database...")
        save_to_sql(self.buy_or_rent, self.cleaned_data_list)
        self.cleaned_data_list = [] 

    def validate_url(self, url_string, page_num) -> bool:
        ## If there's only 50 pages and you enter page 100 into the url it will go to page 50
        ## this functions checks if you've run out of pages to scrape.
        url_page_num = url_string[-len(str(page_num)):]
        return url_page_num == str(page_num)

    def scrape(self, buy_or_rent) -> None:
        if buy_or_rent not in ('rent', 'buy'):
            raise ValueError("Invalid input. Please provide either 'rent' or 'buy' to the scrape function")
        self.buy_or_rent = buy_or_rent # might reconsider this line, defining instance variables outside init isn't best practice

        with SB(uc=True, headless=settings.headless, demo=False) as sb:
            ## Populate list of property url's
            for x in range(1,settings.property_page_limit + 1):
                logger.info(f"Scraping property listings from page {x} of BienIci...")
                self.populate_property_list(x, sb)
                current_url = sb.get_current_url()
                if not self.validate_url(current_url, x):
                    break

            ## Loop through property urls and extract details of each one
            for x in range(len(self.tiles)):
                property_details_dict = self.extract_property_details(self.tiles[x], sb)
                self.clean_data(property_details_dict)
                self.print_results()
                ## Save results to database every 5 properties
                if x % 5 == 0 and x > 0:
                    self.process_data()

        if len(self.cleaned_data_list):
            self.process_data()
        self.tiles = [] # remove properties that have been logged
        logger.info("BienIci scraper finished.")