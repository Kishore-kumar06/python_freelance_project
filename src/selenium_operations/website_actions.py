from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# This function takes a Selenium WebDriver instance and a URL as input, and attempts to navigate the browser to the specified URL. It includes error handling to catch any exceptions that may occur during navigation, such as WebDriver errors or other general exceptions, and prints appropriate error messages.
def navigate_to_url(driver, url):
    try:
        if driver is None:
            print("Failed to open browser.")
            return
        driver.get(url)
        print(f"Navigated to {url}")
    except WebDriverException as e:
        print(f"WebDriver error navigating to {url}: {e}")
    except Exception as e:
        print(f"Error navigating to {url}: {e}")


# This function locates the tariff program dropdown using the provided XPath, creates a Select object, and selects the specified program by its visible text.
def select_tariff_program(driver, program_name, xpath):
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        select = Select(dropdown)
        select.select_by_visible_text(program_name) # Select the option by visible text
        print(f"Selected tariff program: {program_name}")
    except (Exception,NoSuchElementException, TimeoutException) as e:
        print(f"Error selecting tariff program: {e}")


# This function locates the company name input field using the provided XPath, clears any existing text, and enters the specified company name.
def enter_company_name(driver, company_name, xpath):
    try:
        company_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        company_input.clear()
        company_input.send_keys(company_name) # Enter the company name in the input field
        print(f"Entered company name: {company_name}")
    except (Exception, NoSuchElementException, TimeoutException) as e:
        print(f"Error entering company name: {e}")

# Button click function to click the dynamic buttons in website y specifying dynamic xpath as parameter. It waits for the button to be clickable and then clicks it.
def button_click_function(driver, xpath):
    try:
        find_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        find_button.click()
        print("Clicked button")
    except (Exception, NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking button: {e}")


# This function checks for presence of tariff title in the results page. If it is present, it returns the tariff title text. If not, it checks for the presence of a "no files" message and returns that text if found.
def get_oil_tariff_program_from_results(driver, xpath, no_files_xpath): 
    try:
        tariff_program_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        tariff_program_text = tariff_program_element.text.strip()
        print(f"Tariff program from results: {tariff_program_text}")
        return tariff_program_text
    except (Exception,NoSuchElementException, TimeoutException) as e:
        try:
            message_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH,  no_files_xpath))
            )
            message_text = message_element.text.strip()
            return message_text
        except (Exception, NoSuchElementException, TimeoutException) as e:
            print(f"Error checking no files message: {e}")
            return None


def click_actual_tariff_option(driver, xpath):
    try:
        actual_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        actual_option.click()
        print("Clicked on Actual Tariff option")
    except (Exception, NoSuchElementException, TimeoutException) as e:
        print(f"Error clicking on Actual Tariff option: {e}")


def find_last_value_from_oiltariff(driver, table_xpath):
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
    except (Exception, NoSuchElementException, TimeoutException) as e:
        print(f"Error finding last record in table: {e}")
        return None

# This function finds the last record in the table and checks if the effective file option is present in the last record. If it is present, it clicks on the effective link. If not, it returns False.
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
    except (Exception, NoSuchElementException, TimeoutException) as e:
        print(f"Error finding last record in table: {e}")
        return None

    
# This function checks for presence of iframe using the provided XPath and switches to it if found. It includes error handling to catch any exceptions that may occur during the process, such as NoSuchElementException or TimeoutException, and prints appropriate error messages.
def switch_to_iframe(driver, xpath):
    try:
        iframe_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        driver.switch_to.frame(iframe_element)
        print("Switched to iframe")
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error switching to iframe: {e}")


# This function gets the company name from the results page using the provided XPath. It waits for the element to be present, retrieves its text, and returns it.
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
    