import scraper
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
Currently there's only one scraper, which is for BienIci.com.
The only 2 valid inputs for the scrape function are 'buy' and 'rent'. 

It isn't mandatory to use both 'buy' and 'rent' as I've done below,
but the intention is for the rent data to supplement the buy data.
The data's intended purpose is to answer questions such as:
    - "If I buy an apartment in neighbourhood x, with y characteristics, what rental yield (ROI) can I reasonably expect?"
    - "I currently own an apartment with x characterisitcs in neighbourhood y, what could I reasonably sell or rent it for?"
    - "How are rent / property prices changing over time in each neighbourhood?"

before running the script, please ensure your .env file is set up with your mysql details. For example:
DB_HOST=localhost
DB_USER=username
DB_PASSWORD=password
Also please tweak the settings in the settings.py file.

If the script fails to scrape it's likely one of two things:
- The website has changed and class names are different
- Your IP has been flagged as a bot and you're now banned.

If you can access the website manually, it's likely the first reason. 
You can validate this by inspecting the webpage and searching for the missing element.
"""

if __name__ == '__main__':
    myinstance = scraper.BieniciScraper()
    logger.info("Commencing the scraping of properties for sale...")
    myinstance.scrape('buy')
    logger.info("Commencing the scraping of the properties for rent...")
    myinstance.scrape('rent')