
from src.data_processing.pandas_operations import clean_data
from src.selenium_operations.website_actions import (navigate_to_url, select_tariff_program, enter_company_name, click_find_tariff, check_no_files_message, click_oil_tariff_program, get_company_name_from_results, get_tariff_program_from_results, get_oil_tariff_program_from_results)
from src.selenium_operations.driver_setup import open_browser, driver_options, close_browser, quit_browser, back_browser
from src.selenium_operations.xpaths import tariff_program_dropdown, company_name_input, find_tariff_button, no_files_message, oil_tariff_program_option, actual_tariff_option, company_name
from src.data_processing.tracker import create_pipeline_folder, create_excel_tracker_files
import time
from datetime import datetime as dt

if __name__ == "__main__":

    tariff_title = ""
    url = "https://etariff.ferc.gov/TariffList.aspx"
    pipelines = clean_data(r"D:\Project\python_freelance_project\data\source_files\pipelinenames.csv")
    tracker_files_path = r"D:\Project\python_freelance_project\data"

    if pipelines is not None:
        for pipeline in pipelines['PipelineName'][150:]:
            print(pipeline)

            # create_pipeline_folder(tracker_files_path, "input_files", pipeline.strip()) 
            start = dt.now()

            chrome_options = driver_options()
            driver = open_browser("chrome", options=chrome_options)
            navigate_to_url(driver, url)
            time.sleep(2)  # Wait for the page to load
            select_tariff_program(driver, "Oil", tariff_program_dropdown)
            time.sleep(2)  # Wait to see the selection
            enter_company_name(driver, pipeline, company_name_input)
            time.sleep(2)  # Wait to see the input
            click_find_tariff(driver, find_tariff_button)
            time.sleep(3)  # Wait for the results to load
            
            try:
                tariff_bill = get_oil_tariff_program_from_results(driver, oil_tariff_program_option)
                if tariff_bill is not None:
                    tariff_title = tariff_bill.strip()

            except Exception as e:
                    print(f"Error retrieving tariff program: {e}")
                    pipeline = pipeline.strip()
                    tariff_title = "No Tariff Program Found"
                    close_browser(driver)
                    continue
                
            print(f"Tariff program for {pipeline}: {tariff_title}")

            name_of_the_company = get_company_name_from_results(driver, company_name)
            click_oil_tariff_program(driver, oil_tariff_program_option)
            time.sleep(3)  # Wait for the tariff program page to load
            back_browser(driver)
        
            stop = dt.now()
            total_time = stop - start
            create_excel_tracker_files(tracker_files_path, "tracker_files", pipeline, name_of_the_company, tariff_title, total_time)

            # close_browser(driver)
            quit_browser(driver)


