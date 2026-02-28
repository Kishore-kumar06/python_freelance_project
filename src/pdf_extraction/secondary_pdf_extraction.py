import pdfplumber
import pandas as pd
import re

file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

def extract_whole_pdf_as_text(file_path):
    """
    Extract the entire PDF content as text.
    """
    print(f"--- Extracting Entire PDF as Text from {file_path} ---\n")
    
    with pdfplumber.open(file_path) as pdf:
        first_page = pdf.pages[0]
        full_text = first_page.extract_text()

        # find and extract text contains PIPELINE, LLC, or LP
        match = re.search(r'([^\n]*?(PIPELINE|LLC|LP)[^\n]*)', full_text, re.IGNORECASE)
        if match:
            company_info = match.group(1).strip()
            print(f"Company Info: {company_info}")
        
        # Find and extract text after "Effective"
        match = re.search(r'EFFECTIVE[:\s]+([^\n]+)', full_text, )
        if match:
            effective_date = match.group(1).strip()
            print(f"EFFECTIVE: {effective_date}")
        else:
            print("Effective date not found on first page")

        
    # print(full_text)
    
extract_whole_pdf_as_text(file_name)