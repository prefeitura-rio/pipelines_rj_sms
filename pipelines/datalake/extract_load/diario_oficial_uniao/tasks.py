import time
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from urllib3.util.retry import Retry


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")  # Necessário para alguns sistemas operacionais
chrome_options.add_argument("--disable-plugins")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-images")
chrome_options.add_argument("--page-load-strategy=eager")

items = []
count = 0

current_date = datetime.now()
day, month, year = current_date.day, current_date.month, current_date.year

section = 'do3'
count = 0;

driver = webdriver.Chrome(options=chrome_options)
print("🤖 Iniciando o driver...")
driver.get(f"https://www.in.gov.br/leiturajornal?data={day}-{month}-{year}&secao={section}")


while True:
    print(f'Extração da página {count+1}')
    cards = driver.find_elements(by=By.CLASS_NAME, value='resultado')
    
    for card in cards:
        title = card.find_element(by=By.CLASS_NAME, value='title-marker')
        info = card.find_element(by=By.CLASS_NAME, value='breadcrumb-item.publication-info-marker')
        text_link = card.find_element(by=By.TAG_NAME, value='a')
        
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504, 104])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        try:
            response = session.get(text_link.get_attribute('href'))
        
            soup = BeautifulSoup(response.text, 'html.parser')
            decree_text = soup.find(class_='texto-dou')
            decree_date = soup.find(class_='publicado-dou-data')
            decree_edition = soup.find(class_='edicao-dou-data')
            decree_section = soup.find(class_='secao-dou')
            decree_page = soup.find(class_='secao-dou-data')
            decree_agency = soup.find(class_='orgao-dou-data')
        except:
            decree_text = decree_date = decree_edicao = decree_section = decree_page = decree_agency = None
        
        decree_infos = {
            'title': title.text,
            'published_at': decree_date.text,
            'edition': decree_edition.text,
            'section': decree_section.text,
            'agency': decree_agency.text,
            'page': decree_page.text,
            'text': decree_text.text,
            'html': decree_text,
            'url': text_link.get_attribute('href')
        }   
        items.append(decree_infos)
        session.close()

    pagination_buttons = driver.find_elements(by=By.CLASS_NAME, value='pagination-button')
    next_btn = None
    
    for button in pagination_buttons:
        if "Próximo" in button.text:
            next_btn = button
    
    if next_btn:
        next_btn.click()
        count += 1
        time.sleep(1)
    else:
        break
        
driver.quit()

source_name = 'DOU_EXTRACTION'
file_name = '_'.join([source_name, section, str(year),str(month),str(day)])
pd.DataFrame(items).to_csv(file_name + '.csv', index=False, sep=';', encoding='utf8')
print('✅ Extração finalizada.')    