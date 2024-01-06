import BienIciScraper

"""
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
    rent_scraper = BienIciScraper.BienIciRent()
    rent_scraper.scrape()
    #rent_scraper.update_table()

    buy_scraper = BienIciScraper.BienIciBuy()
    buy_scraper.scrape()
    #buy_scraper.update_table()