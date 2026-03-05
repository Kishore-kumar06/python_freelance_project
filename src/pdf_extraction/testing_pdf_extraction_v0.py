# import pdfplumber
# import pandas as pd
# import re

# pdf_path = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"



# output_csv = "Page9_Output.csv"


# def clean(text):
#     if text:
#         return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()
#     return ""


def split_origins(origin_cell):
    """
    Split multi-line origin cell into separate origins
    """
    if not origin_cell:
        return []

    parts = origin_cell.split("\n")
    origins = []

    for part in parts:
        part = clean(part)
        if part:
            origins.append(part)

    return origins


# def extract_page9(pdf_path):
#     results = []

#     with pdfplumber.open(pdf_path) as pdf:
#         page = pdf.pages[8]  # Page 9
#         tables = page.extract_tables()

#         for table in tables:
#             if not table or len(table) < 2:
#                 continue

#             header = [clean(col).lower() if col else "" for col in table[0]]

#             origin_idx = None
#             destination_idx = None
#             rate_indexes = []

#             # Detect columns dynamically
#             for i, col in enumerate(header):
#                 if "origin" in col:
#                     origin_idx = i
#                 elif "destination" in col:
#                     destination_idx = i
#                 elif "rate" in col:
#                     rate_indexes.append(i)

#             if origin_idx is None or destination_idx is None:
#                 continue

#             previous_destination = ""

#             for row in table[1:]:
#                 row = [cell if cell else "" for cell in row]

#                 origin_cell = row[origin_idx] if origin_idx < len(row) else ""
#                 destination = row[destination_idx] if destination_idx < len(row) else ""

#                 destination = clean(destination)

#                 if destination:
#                     previous_destination = destination
#                 else:
#                     destination = previous_destination

#                 # 🔥 Split multiple origins
#                 origins = split_origins(origin_cell)

#                 if not origins:
#                     continue

#                 # 🔥 CROSS JOIN origins × rates
#                 for origin in origins:
#                     for rate_col in rate_indexes:
#                         if rate_col < len(row):
#                             rate_value = clean(row[rate_col])

#                             if rate_value:  # DO NOT ignore n/a
#                                 results.append({
#                                     "Origin": origin,
#                                     "Destination": destination,
#                                     "Rate": rate_value
#                                 })

#     df = pd.DataFrame(results).drop_duplicates().reset_index(drop=True)
#     return df


# # Run extraction
# df_page9 = extract_page9(pdf_path)

# print(df_page9)

# df_page9.to_csv(output_csv, index=False)

# print(f"\nPage 9 extraction completed → {output_csv}")





import pdfplumber
import pandas as pd
import re
from datetime import datetime

# NOTE:
# This function is written to match the same patterns used in your attached script
# (pdfplumber.extract_tables + clean + forward-fill + record dict structure). :contentReference[oaicite:0]{index=0}


def clean(text):
    if text:
        return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()
    return ""


def is_rate_or_na(value: str) -> bool:
    """
    Page 9 includes 'n/a' in at least one row, so accept that too.
    Also accept decimals like 251.15, 387.47, 214.30, 244.01, etc.
    """
    if not value:
        return False
    v = value.strip()
    if v.lower() in ("n/a", "na"):
        return True
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", v))


def extract_pipeline_metadata(pdf):
    pipeline_name = ""
    effective_date = ""

    page1_text = pdf.pages[0].extract_text() or ""

    match_pipeline = re.search(r"(.*Pipeline.*LLC)", page1_text, re.IGNORECASE)
    if match_pipeline:
        pipeline_name = match_pipeline.group(1).strip()

    match_effective = re.search(r"EFFECTIVE:\s*(.*)", page1_text, re.IGNORECASE)
    if match_effective:
        effective_date = match_effective.group(1).strip()
        dt_obj = datetime.strptime(effective_date, "%B %d, %Y")
        effective_date = dt_obj.strftime("%d-%m-%Y")

    return pipeline_name, effective_date


def extract_tariff_rate_type(text):
    try:
        text_clean = (text or "").replace("\r", "")
        lines = text_clean.split("\n")

        tariff_lines = []
        for i, line in enumerate(lines):
            clean_line = line.strip()
            if "RATES" in clean_line:
                tariff_lines.append(clean_line)
                for j in range(1, 3):
                    if i + j < len(lines):
                        nxt = lines[i + j].strip()
                        if nxt and nxt.isupper():
                            tariff_lines.append(nxt)
                        else:
                            break
                break

        if tariff_lines:
            return " ".join(tariff_lines).strip()
        return None

    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""


def extract_expiry_date(text):
    try:
        expiry_date = ""
        expiry_match = re.search(
            r"expire[s]?\s+on.*?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            text or "",
            re.IGNORECASE
        )
        if expiry_match:
            raw_date = expiry_match.group(1).strip()
            dt_obj = datetime.strptime(raw_date, "%B %d, %Y")
            expiry_date = dt_obj.strftime("%d-%m-%Y")
        return expiry_date if expiry_date else ""
    except Exception as e:
        print(f"Error extracting expiry date: {e}")
        return ""

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


def extract_page9(pdf):
    """
    PAGE 9 (index 8): CONTRACT VOLUME INCENTIVE RATES
    Tables expected on Page 9:
      1) Volume Incentive Open Season (March 31, 2021): Origin, Destination, Long-term Incentive Rate
      2) Revenue Commitment Open Season (December 15, 2021): Origin, Destination, Augusta Blend Incentive Rate, Light Incentive Rate
      3) Dedication Program (May 2023): Origin, Destination, Committed Rate, Extra Barrel Rate
    """
    results = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # Page 9 is index 8 (0-based)
    page = pdf.pages[8]
    text = page.extract_text() or ""

    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)

    rate_tier = extract_rate_tiers(text)

    tables = page.extract_tables() or []
    if not tables:
        return results

    for table in tables:
        if not table or len(table) < 2:
            continue

        header = [clean(col).lower() if col else "" for col in table[0]]

        origin_idx = None
        destination_idx = None
        rate_indexes = []

        # Detect columns dynamically
        for i, col in enumerate(header):
            if "origin" in col:
                origin_idx = i
            elif "destination" in col:
                destination_idx = i
            elif "rate" in col:
                rate_indexes.append(i)

        if origin_idx is None or destination_idx is None:
            continue

        previous_destination = ""

        for row in table[1:]:
            row = [cell if cell else "" for cell in row]

            origin_cell = row[origin_idx] if origin_idx < len(row) else ""
            destination = row[destination_idx] if destination_idx < len(row) else ""

            destination = clean(destination)

            if destination:
                previous_destination = destination
            else:
                destination = previous_destination

            # 🔥 Split multiple origins
            origins = split_origins(origin_cell)

            if not origins:
                continue

            # 🔥 CROSS JOIN origins × rates
            for origin in origins:
                for rate_col in rate_indexes:
                    if rate_col < len(row):
                        rate_value = clean(row[rate_col])

                        if rate_value:  # DO NOT ignore n/a
                            results.append({
                            "Pipeline Name": pipeline_name,
                            "PointfOrigin": origin,
                            "PointOfDestination": destination,
                            "LiquidTariffNumber": "",
                            "Effective Date": effective_date,
                            "End Date": expiry_date_value,
                            "TariffStatus": "Effective",
                            "RateTier": rate_tier,
                            "RateType": tariff_rate_type,
                            "TermYear": "",
                            "MinBPD": "",
                            "MaxBPD": "",
                            "AcreageDedicationMinAcres": "",
                            "AcreageDedicationMaxAcres": "",
                            "LiquidRateCentsPerBbl": rate_value,
                            "SurchargeCentsPerBbl": "",
                            "LiquidFuelType": "Crude",
                        })
                
                    

    return results


# -----------------------------
# Example usage (same as your file)
# -----------------------------
if __name__ == "__main__":
    tariff_data = []

    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        df_rates9 = extract_page9(pdf)
        tariff_data.extend(df_rates9)

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:
        output_file = "extracted_page9_contract_volume_incentive_rates.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nPage 9 data successfully exported to {output_file}")
    else:
        print("\nFailed to extract Page 9 table data.")