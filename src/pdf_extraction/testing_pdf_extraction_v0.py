import pdfplumber
import pandas as pd
import re


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


def extract_rates(pdf):
    all_records = []

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for page_number, page in enumerate(pdf.pages):

        text = page.extract_text()

        if not text:
            continue

        # Detect Rate Type
        rate_type = None

        if "NON-CONTRACT TRANSPORTATION RATES" in text:
            rate_type = "NON-CONTRACT TRANSPORTATION RATES"

        if "TEMPORARY VOLUME INCENTIVE RATES" in text:
            rate_type = "TEMPORARY VOLUME INCENTIVE RATES"

        if not rate_type:
            continue

        expiry_date = ""
        tier = ""
        bpd_min = ""
        bpd_max = ""

        # Expiry Date (Page 5 logic)
        expiry_match = re.search(r"expire[s]?\s+on\s+(.*)", text, re.IGNORECASE)
        if expiry_match:
            expiry_date = expiry_match.group(1).strip()

        # Tier
        tier_match = re.search(r"Rate\s*Tier\s*(\d+)", text, re.IGNORECASE)
        if tier_match:
            tier = f"Rate Tier {tier_match.group(1)}"

        # BPD range
        bpd_values = re.findall(r"\b\d{1,6}\b", text)
        if len(bpd_values) >= 2:
            bpd_min = bpd_values[0]
            bpd_max = bpd_values[1]

        tables = page.extract_tables()

        if not tables:
            continue

        largest_table = max(tables, key=lambda t: len(t) if t else 0)

        cleaned_table = []
        for row in largest_table:
            cleaned_row = [
                cell.replace("\n", " ").strip() if cell else ""
                for cell in row
            ]
            if any(cell for cell in cleaned_row):
                cleaned_table.append(cleaned_row)

        if len(cleaned_table) < 3:
            continue

        dest_headers = cleaned_table[1]

        for row in cleaned_table[2:]:

            if len(row) < 2:
                continue

            origin = row[1].strip()
            if not origin:
                continue

            for col_idx in range(2, len(row)):

                if col_idx >= len(dest_headers):
                    break

                destination = dest_headers[col_idx].strip()
                rate = row[col_idx].strip()

                if not rate:
                    continue

                record = {
                    "Pipeline": pipeline_name,
                    "EffectiveDate": effective_date,
                    "RateType": rate_type,
                    "Origin": origin,
                    "Destination": destination,
                    "Rate": rate,
                    "ExpiryDate": expiry_date,
                    "RateTier": tier,
                    "BPDMin": bpd_min,
                    "BPDMax": bpd_max,
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