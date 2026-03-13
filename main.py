from selenium.webdriver.common.by import By
from src.data_processing.pandas_operations import read_and_clean_csv
from src.selenium_operations.website_actions import (navigate_to_url, select_tariff_program, enter_company_name, button_click_function, click_actual_tariff_option, find_last_record_in_table, get_company_name_from_results, get_oil_tariff_program_from_results, switch_to_iframe)
from src.selenium_operations.driver_setup import open_browser, driver_options, close_browser, quit_browser, back_browser
from src.selenium_operations.xpaths import tariff_program_dropdown, company_name_input, find_tariff_button, no_files_message, oil_tariff_program_option, actual_tariff_division, actual_tariff_option, company_name, ferc_table, effective_file_option, download_file_option, close, iframe
from src.data_processing.tracker import create_pipeline_folder, create_excel_tracker_files
import time
import os
from datetime import datetime as dt


if __name__ == "__main__":
    base_path = os.getcwd()
    tariff_title = ""
    file_downloaded = False
    effective_file = ""

    url = "https://etariff.ferc.gov/TariffList.aspx"
    pipelines = read_and_clean_csv(r"D:\Project\python_freelance_project\data\source_files\available_pipelines_v2.csv")
    tracker_files_path = r"D:\Project\python_freelance_project\data"

    if pipelines is not None:
        for pipeline in pipelines['PipelineName']:

            pipeline_folder = create_pipeline_folder(mainpath=base_path, path="input_data_files", pipeline_name=pipeline.strip()) 
            start = dt.now()

            chrome_options = driver_options(download_folder=pipeline_folder)
            driver = open_browser("chrome", options=chrome_options)
            # print("Download Folder:", pipeline_folder)
            # print("Absolute Path:", os.path.abspath(pipeline_folder))

            navigate_to_url(driver, url)    
            time.sleep(2)  # Wait for the page to load
            select_tariff_program(driver, "Oil", tariff_program_dropdown)
            time.sleep(2)  # Wait to see the selection
            enter_company_name(driver, pipeline, company_name_input)
            time.sleep(2)  # Wait to see the input
            button_click_function(driver, find_tariff_button)
            time.sleep(3)  # Wait for the results to load
            
            try:
                tariff_bill = get_oil_tariff_program_from_results(driver, oil_tariff_program_option, no_files_message)
                if tariff_bill is not None:
                    tariff_title = tariff_bill.strip()

                    name_of_the_company = get_company_name_from_results(driver, company_name)
                    button_click_function(driver, oil_tariff_program_option)
                    time.sleep(2)  # Wait for the tariff program page to load
                    click_actual_tariff_option(driver, actual_tariff_division)
                    time.sleep(2)  # Wait for the actual tariff page to load

                    is_efective_file = find_last_record_in_table(driver, ferc_table)
                    # if is_efective_file is not None and is_efective_file.text.strip() == "Effective":
                    effective_file = is_efective_file.text.strip()
                    is_efective_file.click()
                    
                    time.sleep(2)  # Wait for the effective file to load   
                    # switch_to_iframe(driver, iframe)
                    # time.sleep(2)  # Wait for the iframe to load
                    # switch_to_iframe(driver, iframe)
                    driver.switch_to.frame("GB_frame")
                    driver.switch_to.frame(driver.find_element(By.XPATH, iframe))
                    
                    time.sleep(2)  # Wait for the iframe to load
                    button_click_function(driver, download_file_option)
                    file_downloaded = True
                    time.sleep(5)  # Wait for the file to download
                    button_click_function(driver, close)
                    time.sleep(2)  # Wait for the close action to complete
                    back_browser(driver)
                
                    stop = dt.now()
                    total_time = stop - start
                    if file_downloaded:
                        print(f"File downloaded successfully for {pipeline}.")

                    # else:
                    #     print("Effective file option not found in the last record.")
                    #     back_browser(driver)
                    #     close_browser(driver)
                    #     file_downloaded = False
                    #     continue

            except Exception as e:
                    print(f"Error retrieving tariff program: {e}")
                    pipeline = pipeline.strip()
                    tariff_title = "No Tariff Program Found"
                    file_downloaded = False
                    close_browser(driver)
                    continue
                
            create_excel_tracker_files(tracker_files_path, "tracker_files", pipeline, name_of_the_company, tariff_title, is_effective=effective_file, file_status="Downloaded" if file_downloaded else "Failed", time_taken=total_time)
            quit_browser(driver)