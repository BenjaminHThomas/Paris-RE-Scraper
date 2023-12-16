from seleniumbase import SB
from bs4 import BeautifulSoup

url = 'https://www.bienici.com/annonce/vente-de-prestige/paris-1er/appartement/5pieces/keller-williams-1-34_1_32-145206?q=%2Frecherche%2Fachat%2Fparis-75000%3Fpage%3D1'


with SB(uc=True, headless=False, demo=True) as sb:
    sb.get(url)
    
    # Get the page source
    page_source = sb.get_page_source()
    
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # Find the first .allDetails div
    all_details_div = soup.find('div', class_='allDetails')
    
    # If .allDetails div is found, find the first div containing the Euro symbol within it
    if all_details_div:
        euro_div = all_details_div.find('div', text=lambda t: '€' in t if t else False)
        
        # If found, you can access the text or other attributes
        if euro_div:
            div_text = euro_div.get_text(strip=True)
            print(f"Element text: {div_text}")
        else:
            print("Element with Euro symbol not found within the first .allDetails div.")
    else:
        print(".allDetails div not found.")