import pandas as pd
import re
import openpyxl
import time
import random
import threading
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from difflib import get_close_matches
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import io
import os
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import streamlit as st

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920x1080")
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
chrome_options.add_argument(f"user-agent={user_agent}")

results_lock = threading.Lock()

def ex_dif_match(part, values):
    spa = r'[\s\n]'
    pattern = f'(^|{spa})(?P<k>{re.escape(part)})({spa}|$)'
    match = re.search(pattern, values, flags=re.IGNORECASE)
    return match

def duckduckgo_search(query, result_dict, index, domain, progress_callback):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()), options=chrome_options)
    url = f'https://www.google.com/search?q={query}+PDF'
    driver.get(url)
    time.sleep(random.uniform(2, 4))

    links = []
    for i in range(1, 10):
        try:
            xpath = f'//*[@id="rso"]/div[{i}]/div/div/div[1]/div/div/span/a'
            element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
            link = element.get_attribute('href')
            if link:
                links.append(link)
        except Exception as e:
            continue

    driver.quit()

    with results_lock:
        result_dict[index] = filter_and_search_content(links, query, domain)
        progress_callback(index)

def filter_and_search_content(links, mpn, domain):
    mpn_pattern = re.compile(re.escape(mpn), re.IGNORECASE)
    best_pdf_match = None
    exact_pdf_match = None
    exact_html_match = None
    filtered_links = [link for link in links if domain in link]

    for link in filtered_links:
        try:
            response = requests.get(link)
            content_type = response.headers.get('Content-Type', '')

            if 'application/pdf' in content_type:
                with io.BytesIO(response.content) as f:
                    reader = PdfReader(f)
                    pdf_text = ''.join(page.extract_text() or '' for page in reader.pages)
                    if ex_dif_match(mpn, pdf_text):
                        exact_pdf_match = link
                        return [exact_pdf_match]

                if not best_pdf_match:
                    best_pdf_match = link
                elif get_close_matches(mpn, [pdf_text], n=1, cutoff=0.95):
                    best_pdf_match = link

            elif 'text/html' in content_type:
                soup = BeautifulSoup(response.content, 'html.parser')
                text = soup.get_text()
                if mpn_pattern.search(text):
                    exact_html_match = link
                    return [exact_html_match]

        except Exception as e:
            continue

    for link in links:
        try:
            response = requests.get(link)
            content_type = response.headers.get('Content-Type', '')

            if 'application/pdf' in content_type:
                with io.BytesIO(response.content) as f:
                    reader = PdfReader(f)
                    pdf_text = ''.join(page.extract_text() or '' for page in reader.pages)
                    if ex_dif_match(mpn, pdf_text):
                        return [link]

        except Exception as e:
            continue

    return [best_pdf_match] if best_pdf_match else []

def clean_url(url):
    return re.sub(r'.+//|www\.|(\..+)', '', url)

# Streamlit UI
st.title("Real Go Search Tool")
uploaded_file = st.file_uploader("Upload your Excel file", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    if 'MPN' not in df.columns or 'SE_MAN_NAME' not in df.columns:
        st.error("Input file must contain 'MPN' and 'SE_MAN_NAME' columns.")
    else:
            uploaded_filee = st.file_uploader("Upload MFR file", type=["xlsx"])
            pd3=pd.read_excel(uploaded_filee)
            pd3.rename(columns={'SE Name': 'SE_MAN_NAME'}, inplace=True)
            inner_join = pd.merge(df, pd3[['SE_MAN_NAME', 'Website']], on='SE_MAN_NAME', how='left')
            inner_join['Online Link'] = ''
            result_dict = {}
            threads = []

            progress_bar = st.progress(0)

            for index, row in inner_join.iterrows():
                mpn = row['MPN']
                se_man_name = row['Website']
                search_domain = clean_url(se_man_name)
                search_query = f"{mpn}"
                thread = threading.Thread(target=duckduckgo_search, args=(search_query, result_dict, index, search_domain, lambda idx: progress_bar.progress((idx + 1) / len(inner_join))))
                threads.append(thread)
                thread.start()
                time.sleep(random.uniform(3, 10))

            for thread in threads:
                thread.join()

            for index, row in inner_join.iterrows():
                results = result_dict.get(index, [])
                found_link = results[0] if results else None
                inner_join.at[index, 'Online Link'] = found_link if found_link else "No link found"

            st.success("Process completed!")
            st.write(inner_join)

            output_file = f"results_{datetime.now().strftime('%Y%m%d')}.xlsx"
            inner_join.to_excel(output_file, index=False)

            with open(output_file, 'rb') as f:
                st.download_button("Download Results", f, file_name=output_file)
