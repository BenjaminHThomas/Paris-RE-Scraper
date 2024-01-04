import scraper

"""
Currently there's only one scraper, which is for BienIci.com.
The only 2 valid inputs for the scrape function are 'buy' and 'rent'. 
    - 'buy' extracts details from properties for sale. (price, price per metre squared, etc.)
    - 'rent' extracts details from properties for rent. (monthly rent, etc.)

before running the script, please ensure your .env file is set up with your mysql details. For example:
DB_HOST=localhost
DB_USER=username
DB_PASSWORD=password
Also, please tweak the settings in the settings.py file.

If the script fails to scrape it's likely one of two things:
- The website has changed, or;
- Your IP has been flagged as a bot and you're now banned.

If you can access the website manually, it's likely the first reason. 
You can validate this by inspecting the webpage and searching for the missing element.
"""

if __name__ == '__main__':
    myinstance = scraper.BieniciScraper()
    myinstance.scrape('buy')
    myinstance.scrape('rent')
    #myinstance.update_table('buy')
    #myinstance.update_table('rent')

