# This file will contain all of the scrapers for different websites.

from seleniumbase import SB
from random import random
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class bieniciScraper():
    def __init__(self) -> None:
        self.tiles = []
        self.tileSelector = "a.detailedSheetLink"
        self.priceHeader = '.ad-price__the-price'
        self.priceSquareMtrSelector = ".ad-price__price--big .ad-price__price-per-square-meter"

    def random_sleep(self):
        ## Assists with reducing bot visibility
        return 1 + random()
    
    def refresh_driver(self, url, sb):
        logging.warning("Retrying with new driver...")
        sb.get_new_driver(undetectable = True)
        sb.get(url)
        sb.sleep(self.random_sleep())
    
    def populate_property_list(self, page, sb):
        base_url = "https://www.bienici.com/recherche/achat/paris-75000?page="
        target_url = base_url + str(page)
        sb.get(target_url)
        sb.sleep(self.random_sleep())

        try:
            ## Checks whether html divs (tiles) containing property info are present
            sb.assert_element_present(self.tileSelector)
        except AssertionError:
            for x in range(6):
                if sb.is_element_present(self.tileSelector):
                    break
                self.refresh_driver(target_url, sb)
        
        soup = sb.get_beautiful_soup()
        self.tiles.extend([link.get('href') for link in soup.select(self.tileSelector)])

    def extract_property_details(self, property_link,sb):
        base_url = "https://www.bienici.com"
        target_url = base_url+property_link
        sb.get(target_url)

        try:
            sb.assert_element_present(self.priceHeader)
        except AssertionError:
            for x in range(6):
                if sb.is_element_present(self.priceHeader):
                    break
                self.refresh_driver(target_url, sb)
        
        soup = sb.get_beautiful_soup()
        propertyTable = soup.select(".allDetails")[0]

        price = soup.select(self.priceHeader)[0].text # soup.find is simpler but appears to be missing from sb
        priceSquareMtr = soup.select(self.priceSquareMtrSelector)[0].text

        size = propertyTable.select('.labelInfo')[3]
        size = size.select('span')[0].text

        rooms = propertyTable.select('.labelInfo')[4]
        rooms = rooms.select('span')[0].text

        bedrooms = propertyTable.select('.labelInfo')[5]
        bedrooms = bedrooms.select('span')[0].text

        soldby = soup.select('.agency-overview__info-name')[0].text
        address = soup.select('.fullAddress')[0].text
        return price, priceSquareMtr, size, rooms, bedrooms, soldby, address
    
    def extract_zip_code(self, address):
        match = re.search(r'\b75\d{3}\b', address)
        return match.group() if match else None
    
    def clean_numeric(self, value, data_type):
        return data_type(re.sub(r"[^\d.]", "", value))

    def cleanData(self, price, priceSquareMtr, size, rooms, bedrooms, soldby, address):
        return {
            'price': self.clean_numeric(price.replace("\xa0", "").replace(" ", "")[:-1], int),
            'priceSquareMtr': self.clean_numeric(priceSquareMtr.replace(",", "."), float) * 1000,
            'size': self.clean_numeric(size, float),
            'rooms': self.clean_numeric(rooms, float),
            'bedrooms': self.clean_numeric(bedrooms, float),
            'soldby': soldby,
            'address': self.extract_zip_code(address) if self.extract_zip_code(address) else None
        }

    def scrape(self):
        with SB(uc=True, headless=False, demo=True) as sb:
            for x in range(1,101):
                #There's 100 pages in BienIci
                self.populate_property_list(x,sb)
            print(self.tiles)
            for property_link in self.tiles:
                price, priceSquareMtr, size, rooms, bedrooms, soldby, address = self.extract_property_details(property_link,sb)
                print(price, priceSquareMtr, size, rooms, bedrooms, soldby, address)
                propertyDict = self.cleanData(price, priceSquareMtr, size, rooms, bedrooms, soldby, address)
                print(propertyDict)

#myinstance = bieniciScraper()
#myinstance.scrape()