from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from src.selenium_operations.driver_setup import open_browser

url = "https://etariff.ferc.gov/TariffList.aspx"
def navigate_to_url(url):
    try:
        driver = open_browser("chrome")
        if driver is None:
            print("Failed to open browser.")
            return
        driver.get(url)
        print(f"Navigated to {url}")
        time.sleep(5)  # Wait for the page to load
        driver.quit()
    except Exception as e:
        print(f"Error navigating to {url}: {e}")


navigate_to_url(url)