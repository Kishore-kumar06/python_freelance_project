import pdfplumber
import pandas as pd
import re
from datetime import datetime


# -----------------------------
# Reuse from your existing file
# -----------------------------
def extract_pipeline_metadata(pdf):
    pipeline_name = ""
    effective_date = ""

    page1_text = pdf.pages[0].extract_text()

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
        text_clean = text.replace("\r", "")
        lines = text_clean.split("\n")

        tariff_lines = []

        for i, line in enumerate(lines):
            clean_line = line.strip()

            if "RATES" in clean_line:
                tariff_lines.append(clean_line)
                for j in range(1, 3):
                    if i + j < len(lines):
                        next_line = lines[i + j].strip()
                        if next_line and next_line.isupper():
                            tariff_lines.append(next_line)
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
            text,
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


def clean(text):
    if text:
        return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()
    return ""


def is_rate(value):
    if not value:
        return False
    v = value.strip().lower()
    if v in ("n/a", "na"):
        return True
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", v))  # allow ints or decimals


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


# -----------------------------
# NEW: Page 8 extractor
# -----------------------------
def extract_page8(pdf):
    """
    Extracts Page 8: CONTRACT RATES
    Handles multiple tables on the page (Refinery OS, Carpenter OS 2020, Guernsey->Sterling OS, Carpenter OS 2024)
    """
    records = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # Page 8 in the PDF = index 7 (0-based)
    page = pdf.pages[7]
    text = page.extract_text() or ""

    # Header type + expiry date (likely blank for page 8, but keep consistent)
    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)

    tables = page.extract_tables() or []
    if not tables:
        return records

    for table in tables:
        if not table or len(table) < 2:
            continue

        # Clean all cells
        cleaned = []
        for row in table:
            cleaned.append([clean(c) for c in row])

        # Find the header row containing "Origin"
        header_idx = None
        for i, row in enumerate(cleaned):
            row_join = " ".join([c.lower() for c in row if c])
            if "origin" in row_join and ("destination" in row_join or "minimum volume" in row_join or "production dedication volume" in row_join):
                header_idx = i
                break
        if header_idx is None:
            continue

        header = cleaned[header_idx]
        header_low = [h.lower() for h in header]

        # Identify key columns
        origin_col = None
        vol_col = None

        for i, h in enumerate(header_low):
            if "origin" in h:
                origin_col = i
            elif "minimum volume" in h or "production dedication volume" in h:
                vol_col = i

        if origin_col is None:
            continue

        # Identify destination + rate columns:
        # Usually one destination column exists in header (contains "Located in").
        # Sometimes there are multiple rate columns, e.g. "Sterling ...", "Incremental Committed Shipper Rates"
        dest_text = ""
        dest_col_candidates = [i for i, h in enumerate(header) if "Located in" in h]
        if dest_col_candidates:
            dest_col = dest_col_candidates[0]
            dest_text = header[dest_col]
        else:
            dest_col = None

        # Rate columns: any columns after origin/volume that contain rate values in data
        # We'll decide per row by checking is_rate(cell).
        prev_origin = ""
        prev_vol = ""

        for row in cleaned[header_idx + 1:]:
            # pad row to header length
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))

            origin_val = row[origin_col].strip() if origin_col is not None else ""
            if origin_val:
                prev_origin = origin_val
            else:
                origin_val = prev_origin

            vol_val = row[vol_col].strip() if vol_col is not None else ""
            if vol_val:
                prev_vol = vol_val
            else:
                vol_val = prev_vol

            if not origin_val:
                continue

            # destination can sometimes appear in the header (dest_text),
            # but if there is a destination column in the body (rare here), use it.
            destination_val = dest_text

            # If the table actually has a "Destination" column, use it
            for i, h in enumerate(header_low):
                if "destination" in h:
                    body_dest = row[i].strip()
                    if body_dest:
                        destination_val = body_dest
                    break

            # For each cell that looks like a rate, create a record.
            # If multiple rate columns exist, set RateTier to that column header to keep them distinguishable.
            for col_i in range(len(header)):
                if col_i == origin_col or (vol_col is not None and col_i == vol_col):
                    continue

                cell = row[col_i].strip()
                if not is_rate(cell):
                    continue

                rate_val = cell.upper() if cell.lower() in ("n/a", "na") else cell

                # If this is the destination column itself (e.g., header is destination and cell is the rate),
                # keep RateTier empty.
                tier_label = ""
                if dest_col is not None and col_i != dest_col:
                    # this is a secondary rate column like "Incremental Committed Shipper Rates"
                    tier_label = header[col_i].strip()

                min_bpd, max_bpd = parse_volume_to_minmax(vol_val)

                records.append({
                    "Pipeline Name": pipeline_name,
                    "PointfOrigin": origin_val,
                    "PointOfDestination": destination_val,
                    "LiquidTariffNumber": "",
                    "Effective Date": effective_date,
                    "End Date": expiry_date_value,
                    "TariffStatus": "Effective",
                    "RateTier": tier_label if tier_label else None,
                    "RateType": tariff_rate_type,
                    "TermYear": "",
                    "MinBPD": min_bpd if min_bpd is not None else "",
                    "MaxBPD": max_bpd if max_bpd is not None else "",
                    "AcreageDedicationMinAcres": "",
                    "AcreageDedicationMaxAcres": "",
                    "LiquidRateCentsPerBbl": rate_val,
                    "SurchargeCentsPerBbl": "",
                    "LiquidFuelType": "Crude",
                })

    return records


# -----------------------------
# Example main execution
# -----------------------------
if __name__ == "__main__":
    tariff_data = []

    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        df_rates8 = extract_page8(pdf)
        tariff_data.extend(df_rates8)

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:
        output_file = "extracted_page8_contract_rates.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nPage 8 data successfully exported to {output_file}")
    else:
        print("\nFailed to extract Page 8 table data.")