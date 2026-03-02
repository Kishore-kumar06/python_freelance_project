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



def extract_rate_tiers(text):
    try:
        pattern = r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b"

        matches = re.findall(pattern, text, re.IGNORECASE)

        if not matches:
            return [""]

        cleaned_tiers = []

        for tier_value in matches:
            tier_value = tier_value.strip()
            cleaned_tiers.append(f"Rate Tier {tier_value}")

        # Remove duplicates while preserving order
        cleaned_tiers = list(dict.fromkeys(cleaned_tiers))

        return cleaned_tiers

    except Exception as e:
        print(f"Error extracting rate tiers: {e}")
        return [""]
    


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


def extract_rates(pdf):
    all_records = []

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for page_number, page in enumerate(pdf.pages[2:]):

        text = page.extract_text()

        if not text:
            continue

        tariff_rate_type = extract_tariff_rate_type(text)

        expiry_date_value = extract_expiry_date(text)

        rate_tiers = extract_rate_tiers(text)

        bpd_ranges = extract_bpd_ranges(text)
        
        # If no tier found → still process BPD
        if not rate_tiers:
            rate_tiers = [""]

        # If no BPD found → create single blank BPD
        if not bpd_ranges:
            bpd_ranges = [{"MinBPD": "", "MaxBPD": ""}] # continuing 

        for tier in rate_tiers:
            for bpd in bpd_ranges:

                record = {
                    "Pipeline": pipeline_name,
                    "EffectiveDate": effective_date,
                    "RateType": tariff_rate_type,
                    "ExpiryDate": expiry_date_value,
                    "RateTier": tier,
                    "MinBPD": bpd["MinBPD"],
                    "MaxBPD": bpd["MaxBPD"]
                }

                all_records.append(record)

    return pd.DataFrame(all_records)




def extract_rate_tables_with_metadata(pdf):

    all_records = []

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # with pdfplumber.open(pdf_path) as pdf:

    for page_index, page in enumerate(pdf.pages):

        text = page.extract_text()

        if not text:
            continue

        # --- Detect RateType page ---
        rate_type_match = re.search(r"([A-Z\s\-]*RATES[ A-Z\s\-]*)", text)

        if not rate_type_match:
            continue

        tariff_rate_type = rate_type_match.group(1).strip()

        # --- Extract Expiry Date ---
        expiry_match = re.search(
            r"expire[s]?\s+on\s+.*?([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            text,
            re.IGNORECASE,
        )

        expiry_date_value = ""
        if expiry_match:
            expiry_date_value = expiry_match.group(1).strip()

        # --- Extract Rate Tiers ---
        tier_pattern = r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b"
        tier_matches = re.findall(tier_pattern, text, re.IGNORECASE)

        if tier_matches:
            rate_tiers = list(dict.fromkeys(
                [f"Rate Tier {t.strip()}" for t in tier_matches]
            ))
        else:
            rate_tiers = [""]

        print(f"\nProcessing Page {page_index + 1}")
        print(f"RateType: {tariff_rate_type}")
        print(f"Expiry: {expiry_date_value}")
        print(f"Tiers: {rate_tiers}")

        # --- Extract ALL Tables ---
        tables = page.extract_tables()

        if not tables:
            continue

        for table_index, table in enumerate(tables):

            if not table or len(table) < 3:
                continue

            # Clean table
            cleaned_table = []
            for row in table:
                cleaned_row = [
                    cell.replace("\n", " ").strip() if cell else ""
                    for cell in row
                ]
                if any(cleaned_row):
                    cleaned_table.append(cleaned_row)

            if len(cleaned_table) < 3:
                continue

            # Detect header row dynamically
            header_idx = None
            for idx, row in enumerate(cleaned_table):
                row_text = " ".join(row).lower()
                if "origin" in row_text:
                    header_idx = idx
                    break

            if header_idx is None:
                continue

            dest_headers = cleaned_table[header_idx]

            # --- Unpivot ---
            for row in cleaned_table[header_idx + 1:]:

                if len(row) < 2:
                    continue

                origin = row[1].strip()
                if not origin:
                    continue

                for col_idx in range(2, len(row)):

                    if col_idx >= len(dest_headers):
                        continue

                    destination = dest_headers[col_idx]
                    rate = row[col_idx]

                    if not rate:
                        continue

                    destination = destination.strip()
                    rate = rate.strip()

                    # Attach metadata for each tier
                    for tier in rate_tiers:

                        record = {
                            "Pipeline": pipeline_name,
                            "EffectiveDate": effective_date,
                            "RateType": tariff_rate_type,
                            "ExpiryDate": expiry_date_value,
                            "RateTier": tier,
                            "Origin": origin,
                            "Destination": destination,
                            "Rate": rate,
                        }

                        all_records.append(record)

    if all_records:
        return pd.DataFrame(all_records)

    return None

# ---------------- EXECUTION ---------------- #

if __name__ == "__main__":
   
    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        df_final = extract_rate_tables_with_metadata(pdf)
    # with pdfplumber.open(file_name) as pdf:
       
    #     df_final = extract_rates(pdf)

    if not df_final.empty:
        df_final.to_csv("extracted_rates_final.csv", index=False)
        print("Data exported successfully.")
        print(df_final.head())
    else:
        print("No data extracted.")