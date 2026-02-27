import os
import datetime
import pandas as pd
from openpyxl import Workbook, load_workbook

# path = r"D:\Project\python_freelance_project\data\tracker_files"

def create_tracker_files(path, pipelines, company_name, tariff_program, time_taken):
   
    if not os.path.exists(path):
        os.makedirs(path)

    today = datetime.date.today()
    tracker_file = os.path.join(path, f"tracker_{today}.txt")

    with open(tracker_file, 'a') as f:
        f.write(f"{pipelines} -- {company_name} -- {tariff_program} -- {time_taken}\n") 



def create_excel_tracker_files(main_path, path, pipelines, company_name, tariff_program, is_effective, file_status, time_taken):
    
    tracker_path = os.path.join(main_path, path)
    
    if not os.path.exists(tracker_path):
        os.makedirs(tracker_path)

    today = datetime.date.today()
    tracker_file = os.path.join(tracker_path, f"file_status_record_{today}.xlsx")

    new_entry = [pipelines, company_name, tariff_program, is_effective, file_status, time_taken]

    if os.path.exists(tracker_file):
        wb = load_workbook(tracker_file)
        ws = wb.active
        ws.append(new_entry)
    else:
        wb = Workbook()
        ws = wb.active
        ws.append(['Pipeline', 'Company Name', 'Tariff Title', 'Effective Status', 'File Status', 'Time Taken'])
        ws.append(new_entry)
    
    wb.save(tracker_file)


def create_pipeline_folder(mainpath, path, pipeline_name):
    
    input_path = os.path.join(mainpath, path)

    if not os.path.exists(input_path):
        os.makedirs(input_path)

    pipeline_folder = os.path.join(input_path, pipeline_name)
    if not os.path.exists(pipeline_folder):
        os.makedirs(pipeline_folder)
        print(f"Folder created for {pipeline_name}")
        return pipeline_folder
    else:
        print(f"Folder already exists for {pipeline_name}") 
        return None