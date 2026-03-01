import pdfplumber
import pandas as pd
import re
from datetime import datetime


def extract_pipeline_metadata(pdf):
    pipeline_name = ""
    effective_date = ""

    page1_text = pdf.pages[0].extract_text()

    # Pipeline Name
    match_pipeline = re.search(r"(.*Pipeline.*LLC)", page1_text, re.IGNORECASE)
    if match_pipeline:
        pipeline_name = match_pipeline.group(1).strip()

    # Effective Date
    match_effective = re.search(r"EFFECTIVE:\s*(.*)", page1_text, re.IGNORECASE)
    if match_effective:
        effective_date = match_effective.group(1).strip()

        # Convert to datetime
        dt_obj = datetime.strptime(effective_date, "%B %d, %Y")

            # Convert to DD-MM-YYYY
        effective_date = dt_obj.strftime("%d-%m-%Y")

    return pipeline_name, effective_date


def split_complex(val):
    if not val:
        return []

    parts = re.split(
        r",|\s+Located\s+in\s+|\s+in\s+",
        val,
        flags=re.IGNORECASE,
    )
    return [p.strip() for p in parts if p.strip()]


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

        tariff_rate_type = " ".join(tariff_lines)
        return tariff_rate_type.strip()

    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""


def extract_expiry_date(text):
    try:

        expiry_date = ""

        # expiry_match = re.search(r"expire[s]?\s+on\s+(.*)", text, re.IGNORECASE)

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

def extract_rates(pdf):
    all_records = []

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for page_number, page in enumerate(pdf.pages[2:]):

        text = page.extract_text()

        if not text:
            continue

        # Detect Rate Type
        rate_type = None

        tariff_rate_type = extract_tariff_rate_type(text)

        expiry_date_value = extract_expiry_date(text)

        if not tariff_rate_type:
            continue

        record = {
            "Pipeline": pipeline_name,
            "EffectiveDate": effective_date,
            "RateType": tariff_rate_type,
            "ExpiryDate": expiry_date_value,
        }

        all_records.append(record)

    return pd.DataFrame(all_records)


# ---------------- EXECUTION ---------------- #

if __name__ == "__main__":
   
    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        df_final = extract_rates(pdf)

    if not df_final.empty:
        df_final.to_csv("extracted_rates_final.csv", index=False)
        print("Data exported successfully.")
        print(df_final.head())
    else:
        print("No data extracted.")