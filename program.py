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


def configure_get_log():
    warnings.filterwarnings("ignore")

    config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"
                },
                "slack_format": {
                    "format": "`[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d]` %(message)s"
                },
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "formatter": "default",
                    "filename": "logs.log",
                },
            },
            "loggers": {
                "root": {
                    "level": logging.INFO,
                    "handlers": ["file"],
                    "propagate": False,
                },
            },
        }
    )
    log = logging.getLogger("root")
    return log


log = configure_get_log()


def retry(max_retry_count, interval_sec):
    def decorator(func):
        def wrapper(*args, **kwargs):
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
        return wrapper
    return decorator


class Usps:
    @staticmethod
    def unique_city(city_list):
        unique_cities = []
        seen_prefixes = set()

        for city in city_list:
            prefix = city[:3]
            if prefix not in seen_prefixes:
                unique_cities.append(city)
                seen_prefixes.add(prefix)
        
        return unique_cities

    @staticmethod
    @retry(max_retry_count=3, interval_sec=5)
    def get_city_from_zipcode(zip):
        log.info(f"Fetching city of zipcode = {zip}")
        with get_driver() as driver:
            driver.get("https://tools.usps.com/zip-code-lookup.htm?citybyzipcode")
            zip_field = driver.find_element(By.ID, "tZip")
            zip_field.send_keys(zip)
            submit = driver.find_element(By.ID, """cities-by-zip-code""")
            submit.click()
            wait = WebDriverWait(driver, 20)
            wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "recommended-cities")))
            soup = BeautifulSoup(driver.page_source, "lxml")
            recommended = [text.text for text in soup.find(class_="recommended-cities").find_all(class_="row-detail-wrapper")]
            others = [text.text for text in soup.find(class_="other-city-names").find_all(class_="row-detail-wrapper")]
            recommended.extend(others)
            recommended = Usps.unique_city(recommended)
            log.info(f"Found cities: {recommended}")
            return recommended
        

class Truepeoplesearch:
    BASE_URL = "https://www.truepeoplesearch.com"
    @staticmethod
    def proxied_request(url):
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
    @staticmethod
    @retry(max_retry_count=3, interval_sec=5)
    def get_pople_search_result(name, address):
        base_url = f"{Truepeoplesearch.BASE_URL}/results?"
        # Encode the name and address for use in a URL
        encoded_name = urllib.parse.quote(name)
        encoded_address = urllib.parse.quote(address)
        # Construct the full URL
        full_url = f"{base_url}name={encoded_name}&citystatezip={encoded_address}"
        log.info(f"Url: {full_url}")
        response = Truepeoplesearch.proxied_request(full_url)
        if response.status_code != 200:
            raise Exception(f"Status_code: {response.status_code}, Text: {response.text}")
        return response.text
        
    @staticmethod
    def get_links_of_all_results(result):
        soup = BeautifulSoup(result, 'html.parser')
        names = soup.find_all('div', class_='card-summary')
        log.info(f"Got {len(names)} entries for the search")
        return [Truepeoplesearch.BASE_URL+name.get("data-detail-link") for name in names]

    @staticmethod
    def get_emails(soup: BeautifulSoup):
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
    
    @staticmethod
    def compare_addresses(address1, address2):
        similarity_score = fuzz.partial_ratio(address1.lower(), address2.lower())
        log.info(f"Matched addesses ({address1} | AND | {address2}) and got similarity score of {similarity_score}")
        return similarity_score >=90

    @staticmethod
    @retry(max_retry_count=3, interval_sec=5)
    def get_emails_after_verifying_address(url, source_address):
        response = Truepeoplesearch.proxied_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        addresses = soup.find_all(lambda tag: tag.get('data-link-to-more') == 'address')
        for address in addresses:
            address = address.find_all('span')
            address = " ".join([add.text for add in address])
            if Truepeoplesearch.compare_addresses(address, source_address):
                emails = Truepeoplesearch.get_emails(soup)
                return emails
        return None

    @staticmethod
    def truepeoplesearch_manager(name, address):
        result = Truepeoplesearch.get_pople_search_result(name, address)
        links = Truepeoplesearch.get_links_of_all_results(result)
        for link in links:
            emails = Truepeoplesearch.get_emails_after_verifying_address(link, address)
            if emails:
                log.info(f"Got emails {emails}")
                return emails
        log.error("Got no emails.")
        return []

def process_row(row, result_excel_file_path):
    log.info(f"Scraping for: {row}")
    cities = Usps.get_city_from_zipcode(row["ZIP"])
    rows = []
    for city in cities:
        city = city.split(" ")
        city, dist = ' '.join(city[:-1]), city[-1]
        emails = Truepeoplesearch.truepeoplesearch_manager(
            name=" ".join([row["FIRST_NAME"], row["LAST_NAME"]]), 
            address=" ".join([city, dist, str(row["ZIP"])]))
        if not emails:
            emails = Truepeoplesearch.truepeoplesearch_manager(
            name=" ".join([row["FIRST_NAME"].split(" ")[0], row["LAST_NAME"]]), 
            address=" ".join([city, dist, str(row["ZIP"])]))
        new_row = {
        "FIRST_NAME": row["FIRST_NAME"],
        "LAST_NAME": row["LAST_NAME"],
        "STREET": row["STREET"],
        "CITY": city,
        "DIST": dist,
        "ZIP": row["ZIP"],
        "EMAIL": emails
    }
        rows.append(new_row)
    df = pd.DataFrame(rows)
    if os.path.exists(result_excel_file_path):
        existing_df = pd.read_excel(
            result_excel_file_path, names=["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP", "EMAIL"], engine="openpyxl"
        )
        existing_df = pd.concat([existing_df, df], ignore_index=True)
        df = existing_df
    else:
        with open(result_excel_file_path, "w"):
            pass

    df = df.explode("EMAIL", ignore_index=True)
    duplicated_rows = df.duplicated(subset=["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP"])
    df.loc[duplicated_rows, ["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP"]] = ""
    log.info(f"Saved to excel: {result_excel_file_path}")

    def show_try_again_popup():
        result = messagebox.askretrycancel("Error", "Updating excel could not be possible. Please close the file if you are viewing")
        return result

    while True:
        try:
            df.to_excel(result_excel_file_path, index=False)
            break
        except:
            if not show_try_again_popup():
                continue

def main():
    title = 'Truepeoplesearch & USPS scraper'
    root = tk.Tk()
    root.geometry("800x800")
    root.title(title)
    output_file_name = 'truepeoplesearch_email_list.xlsx'
    data = ""
    result_excel_file_path = ""

    def choose_source_file_path():
        nonlocal data
        data = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])

    def choose_save_path():
        nonlocal result_excel_file_path
        result_excel_file_path = filedialog.askdirectory()
        result_excel_file_path = os.path.join(result_excel_file_path, output_file_name)
        

    def submit():
        nonlocal root
        if data and result_excel_file_path:
            root.destroy()
        else:
            messagebox.showerror("Error", "Please choose both source and save paths before submitting.")

    label_source = tk.Label(root, text="Please choose your source excel file:")
    label_source.pack(pady=10)

    choose_source_button = tk.Button(root, text="Choose your excel sheet", command=choose_source_file_path)
    choose_source_button.pack(pady=5)

    label_save = tk.Label(root, text="Please choose the folder to save the result Excel file:")
    label_save.pack(pady=10)

    choose_path_button = tk.Button(root, text="Choose Save Path", command=choose_save_path)
    choose_path_button.pack(pady=5)

    submit_button = tk.Button(root, text="Submit", command=submit)
    submit_button.pack(pady=20)

    root.mainloop()

    if os.path.exists(result_excel_file_path):
        base_path, extension = os.path.splitext(result_excel_file_path)
        count = 1
        result_excel_file_path = f"{base_path}({count}){extension}"

        while os.path.exists(result_excel_file_path):
            count += 1
            result_excel_file_path = f"{base_path}({count}){extension}"

    df = pd.read_excel(data, header=None, engine="openpyxl")
    df.columns = ["FIRST_NAME", "LAST_NAME", "STREET", "CITY", "DIST", "ZIP"]

    progress_window = tk.Tk()
    progress_window.title(f"Progress: {title}")

    progress_frame = ttk.Frame(progress_window)
    progress_frame.pack(pady=20)

    progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", length=300, mode="determinate")
    progress_bar.grid(row=0, column=0, pady=5)

    progress_label = tk.Label(progress_frame, text="0/0 (0%)")
    progress_label.grid(row=1, column=0, pady=5)

    total_rows = len(df)

    for index, row in df.iterrows():
        finished = index + 1
        progress = (finished / total_rows) * 100
        progress_bar["value"] = progress
        progress_label.config(text=f"{finished}/{total_rows} ({progress:.2f}%)")
        progress_window.update_idletasks()  # Ensure the UI updates

        process_row(row, result_excel_file_path)

    progress_window.destroy()

if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception as e:
        log.error(traceback.format_exc())