import pdfplumber
import pandas as pd
import re
import os
from datetime import datetime
import tkinter as tk
from tkinter.filedialog import askopenfilename

root = tk.Tk()
root.withdraw()

def get_transformed_files(curr_path):
    try:
        transformed_files_path = os.path.join(curr_path, 'input_data_files_page6')

        for root, dirs, files in os.walk(transformed_files_path):
            for file in files:
                full_path = os.path.join(root, file)
                yield full_path

    except Exception as er:
        print(f"An error occurred while fetching transformed files: {er}")


def extract_pipeline_metadata(pdf):
    pipeline_name = ""
    effective_date = ""

    page1_text = pdf.pages[0].extract_text()

    # Pipeline Name
    match_pipeline = re.search(r"(.*Pipeline.*(?:LLC|LP|L.P|L.L.C|Inc|INC|Ltd|COMPANY))", page1_text, re.IGNORECASE)
    if match_pipeline:
        pipeline_name = match_pipeline.group(1).strip().replace('"','')

    # Effective Date
        match_effective = re.search(r"\bEffective\b\s*:?\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page1_text, re.IGNORECASE)

        if match_effective:
            effective_date = match_effective.group(1)
        # Convert to datetime
        dt_obj = datetime.strptime(effective_date, "%B %d, %Y")
  
            # Convert to DD-MM-YYYY
        effective_date = dt_obj.strftime("%d-%m-%Y")

    return pipeline_name, effective_date


def extract_tariff_rate_type(text):
    try:
        text_clean = text.replace("\r", "")
        lines = text_clean.split("\n")

        tariff_lines = []
        capture = False

        for i, line in enumerate(lines):

            clean_line = line.strip()

            # Condition:
            # 1. Line must contain RATES
            # 2. Line must be fully uppercase
            if "RATES" in clean_line:

                tariff_lines.append(clean_line)
                # Capture next uppercase lines (header continuation)
                for j in range(1, 3):
                    if i + j < len(lines):
                        next_line = lines[i + j].strip()
                        if next_line and next_line.isupper():
                            tariff_lines.append(next_line)
                        else:
                            break

                break  # Stop after first valid header
            else:
                continue

        if tariff_lines != "":   
            tariff_rate_type = " ".join(tariff_lines)
            return tariff_rate_type.strip()
        else:
            return None

    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""
    

def extract_expiry_date(text):
    try:

        expiry_date = ""

        expiry_match = re.search(
            r"expire[s]?\s+on.*?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            text,
            re.IGNORECASE
        )

        if expiry_match:
            raw_date = expiry_match.group(1).strip()

            # Convert to datetime
            dt_obj = datetime.strptime(raw_date, "%B %d, %Y")

            # Convert to DD-MM-YYYY
            expiry_date = dt_obj.strftime("%d-%m-%Y")

        if expiry_date:
            return expiry_date
        else:
            return ""

    except Exception as e:
        print(f"Error extracting expiry date: {e}")
        return ""


def parse_volume_to_minmax(volume_text: str):
    """
    Convert volume strings like:
      - "10,000 BPD"
      - "3,000 – 4,999 BPD"
      - "0 – 15,000 BPD"
      - "13,000 bpd or greater"
    into (MinBPD, MaxBPD).
    """
    if not volume_text:
        return ("", "")

    t = volume_text.replace("–", "-").replace("—", "-")
    t_low = t.lower()

    # Range: 3,000 - 4,999 BPD
    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", t_low)
    if m:
        min_bpd = int(m.group(1).replace(",", ""))
        max_bpd = int(m.group(2).replace(",", ""))
        return (min_bpd, max_bpd)

    # Or greater: 13,000 bpd or greater  OR  13,000 or greater bpd
    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bpd\s*or\s*greater", t_low)
    if not m:
        m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", t_low)
    if m:
        min_bpd = int(m.group(1).replace(",", ""))
        return (min_bpd, None)

    # Single: 10,000 BPD
    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bpd", t_low)
    if m:
        min_bpd = int(m.group(1).replace(",", ""))
        return (min_bpd, min_bpd)

    return ("", "")


def extract_bpd_ranges(text):
    try:
        results = []

        # Normalize text (remove weird PDF chars)
        text = text.replace("–", "-").replace("—", "-")

        # Pattern 1: Range (5,000 - 11,999 BPD)
        range_pattern = r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*BPD"

        # Pattern 2: 13,000 BPD or greater
        greater_pattern_1 = r"(\d{1,3}(?:,\d{3})*)\s*BPD\s*or\s*greater"

        # Pattern 3: 13,000 or greater BPD
        greater_pattern_2 = r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*BPD"

        # Extract ranges
        for min_bpd, max_bpd in re.findall(range_pattern, text, re.IGNORECASE):
            results.append({
                "MinBPD": int(min_bpd.replace(",", "")),
                "MaxBPD": int(max_bpd.replace(",", ""))
            })

        # Extract "or greater"
        greater_matches = re.findall(greater_pattern_1, text, re.IGNORECASE)
        greater_matches += re.findall(greater_pattern_2, text, re.IGNORECASE)

        for min_bpd in greater_matches:
            results.append({
                "MinBPD": int(min_bpd.replace(",", "")),
                "MaxBPD": None
            })

        return results if results else []

    except Exception as e:
        print(f"Error extracting BPD ranges: {e}")
        return []


def extract_rate_tiers(text):
    try:
        pattern = r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b"

        matches = re.findall(pattern, text, re.IGNORECASE)

        if not matches:
            return None

        cleaned_tiers = []

        for tier_value in matches:
            tier_value = tier_value.strip()
            cleaned_tiers.append(f"Rate Tier {tier_value}")

        # Remove duplicates while preserving order
        cleaned_tiers = list(dict.fromkeys(cleaned_tiers))

        if cleaned_tiers and len(cleaned_tiers) > 0:
            return cleaned_tiers
        else:
            return None

    except Exception as e:
        print(f"Error extracting rate tiers: {e}")
        return ""
    


def clean(text):
    if text:
        return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()
    return ""


def is_rate(value):
    if not value:
        return False
    return bool(re.fullmatch(r"\d+\.\d+", value.strip()))



def extract_page6(pdf):
    unpivoted_data = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # with pdfplumber.open(pdf_path) as pdf:

    for i, page in enumerate(pdf.pages[0:1]):

        text = page.extract_text()

        if not text:
            continue

        rate_type = extract_tariff_rate_type(text)

        if rate_type:
            tariff_rate_type = rate_type
        else:
            if rate_type == "":
                tariff_rate_type = tariff_rate_type

        expiry_date_value = extract_expiry_date(text)

        bpd_ranges = extract_bpd_ranges(text)

        # If no BPD found → create single blank BPD
        if not bpd_ranges:
            bpd_ranges = [{"MinBPD": "", "MaxBPD": ""}] # continuing 


        rate_tier = extract_rate_tiers(text)

        
        # page6 = pdf.pages[5]
        tables = page.extract_tables()

        # for table in tables:

            # header = [clean(col).lower() if col else "" for col in table[0]]

            # origin_idx = None
            # destination_idx = None
            # rate_idx = None

            # for i, col in enumerate(header):
            #     if "origin" in col:
            #         origin_idx = i
            #     elif "destination" in col:
            #         destination_idx = i
            #     elif "rate" in col:
            #         rate_idx = i

            # if origin_idx is None or destination_idx is None or rate_idx is None:
            #     continue

            # for row in table[1:]:
            #     row = [clean(cell) if cell else "" for cell in row]

            #     origin = row[origin_idx]
            #     destination = row[destination_idx]
            #     rate = row[rate_idx]

            #     if origin and destination and is_rate(rate):
            #         for bpd in bpd_ranges:
            #             # Create record
                        # unpivoted_data.append(
                        #     {
                        #         "Pipeline Name": pipeline_name,
                        #         "PointfOrigin": origin,
                        #         "PointOfDestination": destination,
                        #         "LiquidTariffNumber": "", 
                        #         "Effective Date": effective_date,
                        #         "End Date": expiry_date_value,
                        #         "TariffStatus": "Effective",
                        #         "RateTier": rate_tier,
                        #         "RateType": tariff_rate_type,
                        #         "TermYear": "",
                        #         "MinBPD": bpd["MinBPD"],
                        #         "MaxBPD": bpd["MaxBPD"],
                        #         "AcreageDedicationMinAcres": "",
                        #         "AcreageDedicationMaxAcres": "",
                        #         "LiquidRateCentsPerBbl": rate,
                        #         "SurchargeCentsPerBbl": "",
                        #         "LiquidFuelType": "Crude",
                        #     }
                        # )

        unpivoted_data.append(
            {
                "Pipeline Name": pipeline_name,
                # "PointfOrigin": origin,
                # "PointOfDestination": destination,
                # "LiquidTariffNumber": "", 
                "Effective Date": effective_date,
                # "End Date": expiry_date_value,
                # "TariffStatus": "Effective",
                # "RateTier": rate_tier,
                # "RateType": tariff_rate_type,
                # "TermYear": "",
                # "MinBPD": bpd["MinBPD"],
                # "MaxBPD": bpd["MaxBPD"],
                # "AcreageDedicationMinAcres": "",
                # "AcreageDedicationMaxAcres": "",
                # "LiquidRateCentsPerBbl": rate,
                # "SurchargeCentsPerBbl": "",
                # "LiquidFuelType": "Crude",
            }
        )



    tariff_rate_type = ""
    return unpivoted_data




            
# --- Execution ---
# file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

def extract():
    curr_path = os.getcwd()
    tariff_data = []
    # file_path = askopenfilename()
    # print(file_path)
    for file in get_transformed_files(curr_path):
        
        input_file = os.path.basename(file).replace('.PDF','.csv')

        with pdfplumber.open(file) as pdf:
            
            data = extract_page6(pdf)
            tariff_data.extend(data)

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:

        # Export to CSV
        # output_csvfile = ''.join(input_file,'.csv')
        output_file = os.path.join('Page6_extracted_files', 'input_file.csv')
        final_data.to_csv(output_file, index=False, encoding="utf-8")
        # final_data.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {input_file}")
    else:
        print("\nFailed to extract table data.")


if __name__=="__main__":
    extract()