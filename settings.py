# This file contains the users settings and preferences

# This variable determines the number of home pages that are scraped for properties. The home page contains all the individual properties.
## There's a 100 page limit on BienIci. There's no hard limit for Seloger.
property_page_limit = 2

## Headless determines whether or not the browser window is invisible. (useful for testing)
headless = False

## The maximum number of times the script tries to get a new driver when there's issues getting the desired page/element
max_retry = 5

## Change to true to print details for each property
print_results = False

## Demo mode slows the script down for you to inspect. Ensure headless = False if using demo_mode.
demo_mode = False