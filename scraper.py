import urllib.parse
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
import requests
from selenium import webdriver
import logging
import warnings
import time
from logging import config
from contextlib import contextmanager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import openpyxl
import pandas as pd
import os

from credentials import SCRAPEOPS_CREDS

@contextmanager
def get_driver():
    chrome_options = Options()
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration (useful for headless mode)
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model (useful for Docker)
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

    driver = webdriver.Chrome(options=chrome_options)
    try:
        yield driver
    finally:
        driver.quit()


def retry(max_retry_count, interval_sec):
    def decorator(func):
        def wrapper(*args, **kwargs):
            log = args[0].log 
            retry_count = 0
            while retry_count < max_retry_count:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retry_count += 1
                    log.error(f'{func.__name__} failed on attempt {retry_count}: {str(e)}')
                    if retry_count < max_retry_count:
                        log.info(f'Retrying {func.__name__} in {interval_sec} seconds...')
                        time.sleep(interval_sec)
            log.warning(f'{func.__name__} reached maximum retry count of {max_retry_count}.')
            raise Exception(e)
        return wrapper
    return decorator


class Usps:
    def __init__(self, log: logging, zip) -> None:
        self.log = log
        self.zip = zip


    def unique_city(self, city_list):
        unique_cities = []
        seen_prefixes = set()

        for city in city_list:
            prefix = city[:3]
            if prefix not in seen_prefixes:
                unique_cities.append(city)
                seen_prefixes.add(prefix)
        
        return unique_cities


    @retry(max_retry_count=3, interval_sec=5)
    def get_city_from_zipcode(self):
        self.log.info(f"Fetching city of zipcode = {self.zip}")
        with get_driver() as driver:
            driver.get("https://tools.usps.com/zip-code-lookup.htm?citybyzipcode")
            zip_field = driver.find_element(By.ID, "tZip")
            zip_field.send_keys(self.zip)
            submit = driver.find_element(By.ID, """cities-by-zip-code""")
            submit.click()
            wait = WebDriverWait(driver, 20)
            wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "recommended-cities")))
            soup = BeautifulSoup(driver.page_source, "lxml")
            recommended = [text.text for text in soup.find(class_="recommended-cities").find_all(class_="row-detail-wrapper")]
            others = [text.text for text in soup.find(class_="other-city-names").find_all(class_="row-detail-wrapper")]
            recommended.extend(others)
            recommended = self.unique_city(recommended)
            self.log.info(f"Found cities: {recommended}")
            return recommended
        

class Truepeoplesearch:
    def __init__(self, log: logging, first_name='', last_name='', street='', city='', dist='', zip='') -> None:
        self.log = log
        self.first_name = first_name
        self.last_name = last_name
        self.street = street
        self.city = city
        self.dist = dist
        self.zip = zip
        self.BASE_URL = "https://www.truepeoplesearch.com"

    def proxied_request(self, url):
        PROXY_URL = 'https://proxy.scrapeops.io/v1/'
        API_KEY = SCRAPEOPS_CREDS
        return requests.get(
            url=PROXY_URL,
            params={
                'api_key': API_KEY,
                'url': url, 
                # 'residential': 'true', 
                'country': 'us'
            },
        )

    @retry(max_retry_count=3, interval_sec=5)
    def get_pople_search_result(self, name, address):
        base_url = f"{self.BASE_URL}/results?"
        # Encode the name and address for use in a URL
        encoded_name = urllib.parse.quote(name)
        encoded_address = urllib.parse.quote(address)
        # Construct the full URL
        full_url = f"{base_url}name={encoded_name}&citystatezip={encoded_address}"
        self.log.info(f"Url: {full_url}")
        response = self.proxied_request(full_url)
        if response.status_code != 200:
            raise Exception(f"Status_code: {response.status_code}, Text: {response.text}")
        return response.text
        
    def get_links_of_all_results(self, result):
        soup = BeautifulSoup(result, 'html.parser')
        names = soup.find_all('div', class_='card-summary')
        self.log.info(f"Got {len(names)} entries for the search")
        return [self.BASE_URL+name.get("data-detail-link") for name in names]

    def get_emails(self, soup: BeautifulSoup):
        emails = []
        slots = soup.find_all(class_='row pl-md-1')
        for slot in slots:
            if "Email Addresses" in slot.get_text():
                emails= [email.text.strip() for email in slot.find_all(class_="col")]
                break
        allowed_domains = [
            "@yahoo.com",
            "@hotmail.com",
            "@gmail.com",
            "@aol.com",
            "@msn.com",
            "@outlook.com",
            "@live.com"
        ]
        return [email for email in emails if any(domain in email for domain in allowed_domains)]
    
    def compare_addresses(self, address1, address2):
        similarity_score = fuzz.partial_ratio(address1.lower(), address2.lower())
        self.log.info(f"Matched addesses ({address1} | AND | {address2}) and got similarity score of {similarity_score}")
        return similarity_score >=90

    @retry(max_retry_count=3, interval_sec=5)
    def get_emails_after_verifying_address(self, url, source_address):
        response = self.proxied_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        addresses = soup.find_all(lambda tag: tag.get('data-link-to-more') == 'address')
        for address in addresses:
            address = address.find_all('span')
            address = " ".join([add.text for add in address])
            if self.compare_addresses(address, source_address):
                emails = self.get_emails(soup)
                return emails
        return None

    def truepeoplesearch_manager(self, name, address):
        result = self.get_pople_search_result(name, address)
        links = self.get_links_of_all_results(result)
        for link in links:
            emails = self.get_emails_after_verifying_address(link, address)
            if emails:
                self.log.info(f"Got emails {emails}")
                return emails
        self.log.error("Got no emails.")
        return []

def process_row(row, result_excel_file_path, log: logging):
    try:
        log.info(f"Scraping for: {row}")
        usps = Usps(zip=row["ZIP"], log=log)
        cities = usps.get_city_from_zipcode()
        rows = []
        for city in cities:
            city = city.split(" ")
            city, dist = ' '.join(city[:-1]), city[-1]
            truepeoplesearch = Truepeoplesearch(
                first_name=row["FIRST_NAME"],
                last_name=row["LAST_NAME"],
                street=row["STREET"],
                city=city,
                dist=dist,
                zip=str(row["ZIP"]),
                log=log
            )
            emails = truepeoplesearch.truepeoplesearch_manager(
                name=" ".join([row["FIRST_NAME"], row["LAST_NAME"]]), 
                address=" ".join([city, dist, str(row["ZIP"])]))
            if not emails:
                emails = truepeoplesearch.truepeoplesearch_manager(
                name=" ".join([row["FIRST_NAME"].split(" ")[0], row["LAST_NAME"]]), 
                address=" ".join([city, dist, str(row["ZIP"])]))
            new_row = {
            "FIRST_NAME": row["FIRST_NAME"],
            "LAST_NAME": row["LAST_NAME"],
            "STREET": row["STREET"],
            "CITY": city,
            "DIST": dist,
            "ZIP": row["ZIP"],
            "EMAIL": emails,
            "STATUS": 'SUCCESS'
        }
            rows.append(new_row)
    except:
        try:
            new_row = {
                "FIRST_NAME": row["FIRST_NAME"],
                "LAST_NAME": row["LAST_NAME"],
                "STREET": row["STREET"],
                "CITY": city,
                "DIST": dist,
                "ZIP": row["ZIP"],
                "EMAIL": [],
                "STATUS": "ERROR"
            }
        except:
            new_row = {
                "FIRST_NAME": row["FIRST_NAME"],
                "LAST_NAME": row["LAST_NAME"],
                "STREET": row["STREET"],
                "CITY": '',
                "DIST": '',
                "ZIP": row["ZIP"],
                "EMAIL": [],
                "STATUS": "ERROR"
            }

        rows.append(new_row)
    df = pd.DataFrame(rows)
    if os.path.exists(result_excel_file_path):
        existing_df = pd.read_excel(
            result_excel_file_path, names=["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP", "EMAIL", "STATUS"], engine="openpyxl"
        )
        existing_df = pd.concat([existing_df, df], ignore_index=True)
        df = existing_df
    else:
        with open(result_excel_file_path, "w"):
            pass

    df = df.explode("EMAIL", ignore_index=True)
    duplicated_rows = df.duplicated(subset=["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP", "STATUS"])
    df.loc[duplicated_rows, ["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP", "STATUS"]] = ""
    log.info(f"Saved to excel: {result_excel_file_path}")

    return df