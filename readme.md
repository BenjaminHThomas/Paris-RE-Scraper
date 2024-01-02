# Paris Real Estate Scraper

![License](https://img.shields.io/badge/license-MIT-blue)

## Overview
The paris-RE-Scraper is a Python-based web scraper designed to extract Paris real estate information from various websites and store it in a MySQL database. At this stage in development it's only set up to extract information from bienici.com. 

## Pre-requisites:
You need to have MySQL installed prior to running the code. You can find the installation guide [here.](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/)

## Getting Started

1. Clone the repository:
```bash
git clone https://github.com/BenjaminHThomas/Paris-RE-Scraper.git
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```
4. Add your .env file with your MySQL credentials. Example:
```
DB_HOST=localhost
DB_USER=username
DB_PASSWORD=password
```

3. Adjust the settings in settings.py
```
# This variable determines the number of home pages that are scraped for properties. The home page contains all the individual properties.
## There's a 100 page limit on BienIci
property_page_limit = 100

## Headless determines whether or not the browser window is invisible. (useful for testing)
headless = True

## Number of times the script tries to get a new driver when there's issues getting the desired page/element
max_retry = 5

## Change to true to print details for each property
print_results = False

## Demo mode slows the script down for you to inspect. Ensure headless = False if using demo_mode.
demo_mode = False
```

4. Adjust main.py as needed:
```
import scraper
if __name__ == '__main__':
    myinstance = scraper.BieniciScraper()
    myinstance.scrape('buy')
    myinstance.scrape('rent')
``` 

5. Run the scraper:
```bash
python main.py
```

## Usage
Once the scraper is finished you can access the data in MySQL.
<br/>
You can do this in the terminal:
```
mysql -u username -p password
mysql USE paris_re;
mysql SHOW TABLES;
mysql SELECT * FROM bien_ici_buy limit 5;
```
<br/>

Or directly in python:
```python
import pandas as pd, os
import MySQLdb
import pandas.io.sql as psql
from dotenv import load_dotenv
load_dotenv()

# setup the database connection.  
db=MySQLdb.connect(host=os.getenv('DB_HOST'), 
                   user=os.getenv('DB_USER'), 
                   passwd=os.getenv('DB_PASSWORD'), 
                   db='paris_RE')
# create the query
query1 = "select * from bien_ici_buy"
query2 = "select * from bien_ici_rent"
# execute the query and assign it to a pandas dataframe
buy = psql.read_sql(query1, con=db)
rent = psql.read_sql(query2, con=db)
# close the database connection
db.close()
```

## License

This project is licensed under the MIT License.
