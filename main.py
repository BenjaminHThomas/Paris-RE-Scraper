import scraper
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
The 'main' script is mainly for convenience and showing the intended syntax.
You could just as easily run the below commands in the terminal.

Currently there's only one scraper, which is for BienIci.com.
The only 2 valid inputs for the scrape function are 'buy' and 'rent'. 

It isn't mandatory to use both as I've done below,
but the intention is for the rent data to supplement the buy data.
The data's intended purpose is to answer questions such as:
    - "If I buy an apartment in neighbourhood x, with y characteristics, what rental yield (ROI) can I reasonably expect?"
    - "I currently own an apartment with x characterisitcs in neighbourhood y, what could I reasonably sell or rent it for?"
    - "How are rent / property prices changing over time in each neighbourhood?"
"""

if __name__ == '__main__':
    myinstance = scraper.BieniciScraper()
    logger.info("Commencing the scraping of properties for sale...")
    myinstance.scrape('buy')
    logger.info("Commencing the scraping of the properties for rent...")
    myinstance.scrape('rent')