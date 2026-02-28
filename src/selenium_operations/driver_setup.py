from selenium import webdriver
import os

# This function sets up the browser options, including download preferences, and returns the configured options object.
def driver_options(download_folder):
    
    # Set download preferences
    prefs = {
        "download.default_directory": os.path.abspath(download_folder),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", prefs)
    # options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    return options

# This function opens the specified browser with the given options and returns the driver instance.
def open_browser(browser_name, options):
    try:
        if browser_name.lower() == "chrome":
            driver = webdriver.Chrome(options=options)
        elif browser_name.lower() == "firefox":
            driver = webdriver.Firefox()
        elif browser_name.lower() == "edge":
            driver = webdriver.Edge()
        else:
            print(f"Unsupported browser: {browser_name}")
            return None
        return driver
    except Exception as e:
        print(f"Error opening the browser: {e}")
        return None
    
# This function closes the browser instance if it exists, handling any exceptions that may occur during the process.
def close_browser(driver):
    try:
        if driver is not None:
            driver.close()
            print("Browser closed successfully.")
        else:
            print("No browser instance to close.")
    except Exception as e:
        print(f"Error closing the browser: {e}")

# This function quits the browser instance if it exists, handling any exceptions that may occur during the process. Quitting the browser will close all associated windows and end the session.
def quit_browser(driver):
    try:
        if driver is not None:
            driver.quit()
            print("Browser quit successfully.")
        else:
            print("No browser instance to quit.")
    except Exception as e:
        print(f"Error quitting the browser: {e}")

# This function navigates back in the browser history if the driver instance exists, handling any exceptions that may occur during the process.
def back_browser(driver):
    try:
        if driver is not None:
            driver.back()
            print("Navigated back successfully.")
        else:
            print("No browser instance to navigate back.")
    except Exception as e:
        print(f"Error navigating back: {e}")    


