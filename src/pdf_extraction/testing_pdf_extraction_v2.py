import pdfplumber
import pandas as pd
import re


# ------------------------------
# Extract Effective Date (Page 1)
# ------------------------------
def extract_effective_date(pdf):
    text = pdf.pages[0].extract_text()
    match = re.search(r"EFFECTIVE:\s*(.*)", text, re.IGNORECASE)
    return match.group(1).strip() if match else ""


# ------------------------------
# Extract Page Metadata
# ------------------------------
def extract_page_metadata(text):

    # Tariff Rate Type (Line containing RATES)
    rate_type_match = re.search(r"([A-Z\s\-]*RATES[ A-Z\s\-]*)", text)
    tariff_rate_type = rate_type_match.group(1).strip() if rate_type_match else ""

    # Expiry Date
    expiry_match = re.search(r"expire[s]?\s+on\s+(.*)", text, re.IGNORECASE)
    expiry_date = expiry_match.group(1).strip() if expiry_match else ""

    # Rate Tier
    tier_match = re.search(r"(Rate\s*Tier\s*\d+|Tier\s*\d+)", text, re.IGNORECASE)
    rate_tier = tier_match.group(1).strip() if tier_match else ""

    # BPD Range
    bpd_min = ""
    bpd_max = ""

    # Example patterns: 0 - 12000, 5000 - 19999
    range_match = re.search(r"(\d{1,6})\s*-\s*(\d{1,6})", text)
    if range_match:
        bpd_min = range_match.group(1)
        bpd_max = range_match.group(2)
    else:
        # fallback: if single number found
        numbers = re.findall(r"\b\d{1,6}\b", text)
        if numbers:
            bpd_min = "0"
            bpd_max = numbers[0]

    return tariff_rate_type, expiry_date, rate_tier, bpd_min, bpd_max


# ------------------------------
# Extract Tables Page Wise
# ------------------------------
def extract_tables_from_pdf(file_path):

    records = []

    with pdfplumber.open(file_path) as pdf:

        effective_date = extract_effective_date(pdf)

        # Process only Page 3–15
        for page_number in range(2, min(15, len(pdf.pages))):

            page = pdf.pages[page_number]
            text = page.extract_text()

            if not text:
                continue

            # Only process pages that contain tables + RATES keyword
            if "RATES" not in text:
                continue

            tariff_rate_type, expiry_date, rate_tier, bpd_min, bpd_max = extract_page_metadata(text)

            tables = page.extract_tables()
            if not tables:
                continue

            # Process each table separately
            for table in tables:

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

                # Destination headers (row 2 usually)
                headers = cleaned_table[1]

                for row in cleaned_table[2:]:

                    if len(row) < 2:
                        continue

                    origin = row[1].strip()
                    if not origin:
                        continue

                    for col_idx in range(2, len(row)):

                        if col_idx >= len(headers):
                            break

                        destination = headers[col_idx].strip()
                        rate = row[col_idx].strip()

                        if not rate:
                            continue

                        record = {
                            "EffectiveDate": effective_date,
                            "TariffRateType": tariff_rate_type,
                            "ExpiryDate": expiry_date,
                            "RateTier": rate_tier,
                            "BPDMin": bpd_min,
                            "BPDMax": bpd_max,
                            "Origin": origin,
                            "Destination": destination,
                            "Rate": rate,
                            "SourcePage": page_number + 1
                        }

                        records.append(record)

    return pd.DataFrame(records)


# ------------------------------
# RUN EXTRACTION
# ------------------------------
file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

df_final = extract_tables_from_pdf(file_name)

if not df_final.empty:
    df_final.to_csv("OilTariffs_Final_Output.csv", index=False)
    print("Extraction completed successfully.")
    print(df_final.head())
else:
    print("No data extracted.")