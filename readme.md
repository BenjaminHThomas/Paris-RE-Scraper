# Paris Real Estate Scraper

![License](https://img.shields.io/badge/license-MIT-blue)

## Overview
The paris-RE-Scraper is a Python-based web scraper designed to extract Paris real estate information from various websites and store it in a MySQL database. At this stage in development it's only set up to extract information from bienici.com. 

## Pre-requisites:
- You need to have MySQL installed prior to running the code. You can find the guide [here.](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/)

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


## License

This project is licensed under the MIT License.
