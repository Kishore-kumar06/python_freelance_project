from selenium import webdriver


def driver_options():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")  # Disable GPU acceleration
    # options.add_argument("--window-size=1920,1080")  # Set window size
    return options

def open_browser(browser_name, options=None):
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
    

def close_browser(driver):
    try:
        if driver is not None:
            driver.close()
            print("Browser closed successfully.")
        else:
            print("No browser instance to close.")
    except Exception as e:
        print(f"Error closing the browser: {e}")


def quit_browser(driver):
    try:
        if driver is not None:
            driver.quit()
            print("Browser quit successfully.")
        else:
            print("No browser instance to quit.")
    except Exception as e:
        print(f"Error quitting the browser: {e}")


def back_browser(driver):
    try:
        if driver is not None:
            driver.back()
            print("Navigated back successfully.")
        else:
            print("No browser instance to navigate back.")
    except Exception as e:
        print(f"Error navigating back: {e}")    


