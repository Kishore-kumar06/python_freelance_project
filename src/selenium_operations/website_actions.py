from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .xpaths import no_files_message

def navigate_to_url(driver, url):
    try:
        if driver is None:
            print("Failed to open browser.")
            return
        driver.get(url)
        print(f"Navigated to {url}")
        
    except Exception as e:
        print(f"Error navigating to {url}: {e}")

def select_tariff_program(driver, program_name, xpath):
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        select = Select(dropdown)
        select.select_by_visible_text(program_name)
        print(f"Selected tariff program: {program_name}")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error selecting tariff program: {e}")

def enter_company_name(driver, company_name, xpath):
    try:
        company_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        company_input.clear()
        company_input.send_keys(company_name)
        print(f"Entered company name: {company_name}")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error entering company name: {e}")

def click_find_tariff(driver, xpath):
    try:
        find_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        find_button.click()
        print("Clicked 'Find Tariff' button")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking 'Find Tariff' button: {e}")

def check_no_files_message(driver, xpath):
    try:
        message_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        message_text = message_element.text.strip()
        return message_text
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error checking no files message: {e}")


def click_oil_tariff_program(driver, xpath):
    try:
        oil_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        oil_option.click()
        
        print("Clicked on Oil Tariff Program option")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking on Oil Tariff Program option: {e}")

def get_oil_tariff_program_from_results(driver, xpath): 
    try:
        tariff_program_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        tariff_program_text = tariff_program_element.text.strip()
        print(f"Tariff program from results: {tariff_program_text}")
        return tariff_program_text
    except (NoSuchElementException, TimeoutException) as e:
        try:
            message_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH,  no_files_message))
            )
            message_text = message_element.text.strip()
            return message_text
        except (NoSuchElementException, TimeoutException) as e:
            print(f"Error checking no files message: {e}")
            return None


def click_actual_tariff_option(driver, xpath):
    try:
        actual_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        actual_option.click()
        print("Clicked on Actual Tariff option")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking on Actual Tariff option: {e}")


def find_last_record_in_table(driver, table_xpath):
    try:
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, table_xpath))
        )
        rows = table.find_elements(By.TAG_NAME, "tr")
        if len(rows) > 1:
            last_row = rows[-1]
            cells = last_row.find_elements(By.TAG_NAME, "td")
            # return [cell.text.strip() for cell in cells]
            effective_button = cells[3].find_element(By.TAG_NAME, "a")
            return effective_button
        else:
            print("No records found in the table.")
            return None
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error finding last record in table: {e}")
        return None


def click_effective_file_option(driver, xpath):
    try:
        effective_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        effective_option.click()
        print("Clicked on Effective File option")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking on Effective File option: {e}")

    

def switch_to_iframe(driver, xpath):
    try:
        iframe_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        driver.switch_to.frame(iframe_element)
        print("Switched to iframe")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error switching to iframe: {e}")


def click_download_file_option(driver, xpath):
    try:
        download_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        download_option.click()
        print("Clicked on Download File option")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking on Download File option: {e}")

def click_close(driver, xpath): 
    try:
        close_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        close_button.click()
        print("Clicked on Close button")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking on Close button: {e}")

def get_company_name_from_results(driver, xpath):
    try:
        company_name_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        company_name_text = company_name_element.text.strip()
        print(f"Company name from results: {company_name_text}")
        return company_name_text
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error getting company name from results: {e}")
        return None
    
def get_tariff_program_from_results(driver, xpath):
    try:
        tariff_program_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        tariff_program_text = tariff_program_element.text.strip()
        print(f"Tariff program from results: {tariff_program_text}")
        return tariff_program_text
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error getting tariff program from results: {e}")
        return None