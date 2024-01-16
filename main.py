from BienIciScraper import BienIciBuy, BienIciRent
from SelogerScraper import SelogerBuy, SelogerRent

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
    # Seloger
    # seloger_rent = SelogerRent()
    # seloger_rent.scrape()

    # seloger_buy = SelogerBuy()
    # seloger_buy.scrape()


    ## BienIci
    # bienici_buy = BienIciBuy()
    # bienici_buy.scrape()
    # bienici_buy.update_table()

    # bienici_rent = BienIciRent()
    # bienici_rent.scrape()
    # bienici_rent.update_table()
    pass



## Use the below to check on the bot output

# def get_table(table_name:str):
#     import os
#     import MySQLdb
#     import pandas.io.sql as psql
#     db=MySQLdb.connect(host=os.getenv('DB_HOST'), 
#                    user=os.getenv('DB_USER'), 
#                    passwd=os.getenv('DB_PASSWORD'), 
#                    db='paris_RE')
#     # create the query
#     query1 = f"select * from {table_name}"

#     # execute the query and assign it to a pandas dataframe
#     df = psql.read_sql(query1, con=db)
#     return df

# rent = get_table('bien_ici_rent')
# print(rent.tail())