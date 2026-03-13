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



def parse_bpd_header_to_minmax(bpd_header: str):
    """
    Examples:
      '5,000 – 11,999 BPD'  -> (5000, 11999)
      '12,000 – 23,999 BPD' -> (12000, 23999)
      '24,000 or greater BPD' -> (24000, None)
    """
    if not bpd_header:
        return ("", "")

    t = clean(bpd_header).replace("–", "-").replace("—", "-")
    low = t.lower()

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", low)
    if m:
        return (int(m.group(1).replace(",", "")), int(m.group(2).replace(",", "")))

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", low)
    if m:
        return (int(m.group(1).replace(",", "")), None)

    return ("", "")


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
    

def extract_rate_tier_label(text):
    """
    Extract only Tier / Rate Tier values.
    Examples:
      Tier I -> Rate Tier I
      Tier 2 -> Rate Tier 2
    """
    text = clean(text)
    m = re.search(r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b", text, re.IGNORECASE)
    if m:
        return f"Rate Tier {m.group(1).upper()}"
    return ""


def extract_rates_table_for_Page3_4_12_13_14_15(pdf, start_Page_number, end_page_number):
    print(f"--- Extracting Rates Table from {pdf} ---\n")

    unpivoted_data = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # Search through all pages to find the one with "NON-CONTRACT TRANSPORTATION RATES"
    for i, page in enumerate(pdf.pages[start_Page_number:end_page_number]):

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
        
        # Check for both the title and the presence of rate information
        if (
            tariff_rate_type in text
            or "cents per Barrel" in text
            or "All rates are unchanged." in text
        ):
            print(f"Found target table on Page {i + 1}.")

            # Extract table using pdfplumber's table extraction
            tables = page.extract_tables()

            if tables:
                print(f"Found {len(tables)} table(s) on the page.")

                # for idx, table in enumerate(tables):
                #     print(f"\n--- Table {idx + 1} ---")
                #     print(f"Rows: {len(table)}")
                #     print(f"Columns: {len(table[0]) if table else 0}")
                #     print("\nFirst 10 rows:")
                #     for i, row in enumerate(table[:10]):
                #         print(f"Row {i}: {row}")

                # # Usually the rates table is the largest one
                # largest_table = max(tables, key=lambda t: len(t) if t else 0)

                for table in tables:

                    if table:
                        # Clean the table data
                        cleaned_table = []
                        for row in table:
                            cleaned_row = [
                                cell.replace("\n", " ").strip() if cell else ""
                                for cell in row
                            ]
                            # Only add rows with some content
                            if any(cell for cell in cleaned_row):
                                cleaned_table.append(cleaned_row)

                        if cleaned_table:
                            # --- Unpivoting logic ---
                            # Headers (Destinations) are in row 1, starting from index 2
                            # Origins are in column 1 (Column 2) of each data row
                            # Data starts from row 2
                            # Rates are from index 2 onwards

                            # unpivoted_data = []
                            if len(cleaned_table) > 1:
                                dest_headers = cleaned_table[1]
                                
                                for row in cleaned_table[2:]:
                                    if len(row) < 2:
                                        continue

                                    # Origin value is in Column 2 (index 1)
                                    origin = row[1]
                                    if origin:
                                        origin = origin.replace("\n", " ").strip()

                                    if (
                                        not origin
                                        or origin.lower() == "none"
                                        or origin == ""
                                    ):
                                        continue

                                    for col_idx in range(2, len(row)):
                                        if col_idx >= len(dest_headers):
                                            break

                                        destination = dest_headers[col_idx]
                                        if destination:
                                            destination = destination.replace(
                                                "\n", " "
                                            ).strip()
                                        else:
                                            destination = f"Unknown_Dest_{col_idx}"

                                        rate = row[col_idx]
                                        if rate:
                                            rate = rate.replace("\n", " ").strip()

                                        for bpd in bpd_ranges:
                                            # Create record
                                            unpivoted_data.append(
                                                {
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
                                                    "MinBPD": bpd["MinBPD"],
                                                    "MaxBPD": bpd["MaxBPD"],
                                                    "AcreageDedicationMinAcres": "",
                                                    "AcreageDedicationMaxAcres": "",
                                                    "LiquidRateCentsPerBbl": rate,
                                                    "SurchargeCentsPerBbl": "",
                                                    "LiquidFuelType": "Crude",
                                                }
                                            )

        if unpivoted_data:
            df_final = unpivoted_data
    
    # tariff_rate_type = ""  # Reset for next page
    return df_final

 
def extract_page_5(pdf):

    print("\n--- Extracting Page 5 (Final Version) ---\n")

    records = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for page_index, page in enumerate(pdf.pages[4:5]):

        text = page.extract_text()

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

        print(tariff_rate_type)

        # # Page 5 (0-based index 4)
        # page = pdf.pages[4]

        
        if not text or "All rates are unchanged." not in text:
            print("Keyword not found on Page 5.")
            return None

        tables = page.extract_tables()
        if not tables:
            print("No tables found on Page 5.")
            return None

        for table in tables:

            if not table:
                continue

            # Clean table cells
            cleaned = []
            for row in table:
                cleaned_row = [
                    cell.replace("\n", " ").strip() if cell else ""
                    for cell in row
                ]
                cleaned.append(cleaned_row)

            # Locate header row
            header_index = None
            for i, row in enumerate(cleaned):
                row_text = " ".join(row).lower()
                if "origin" in row_text and "destination" in row_text:
                    header_index = i
                    break

            if header_index is None:
                continue

            header = cleaned[header_index]

            # Detect columns dynamically
            origin_col = None
            dest_col = None
            tier_cols = {}

            for idx, col in enumerate(header):
                col_lower = col.lower()
                if "origin" in col_lower:
                    origin_col = idx
                elif "destination" in col_lower:
                    dest_col = idx
                elif "rate tier" in col_lower:
                    tier_cols[idx] = col.strip()

            if origin_col is None or dest_col is None or not tier_cols:
                continue

            previous_origin = ""
            previous_dest = ""

            # Process rows after header
            for row in cleaned[header_index + 1:]:

                if len(row) <= max(tier_cols.keys()):
                    continue

                origin = row[origin_col].strip()
                dest = row[dest_col].strip()

                # Forward fill origin
                if origin:
                    previous_origin = origin
                else:
                    origin = previous_origin

                # Forward fill destination
                if dest:
                    previous_dest = dest
                else:
                    dest = previous_dest

                if not origin or not dest:
                    continue

                origin = " ".join(origin.split())
                dest = " ".join(dest.split())

                # 🔥 Split merged origins
                origin_parts = re.split(
                    r'(?=[A-Z][a-zA-Z]+\s+Located in)', origin
                )
                origin_parts = [
                    o.strip() for o in origin_parts if o.strip()
                ]

                for col_index, tier_name in tier_cols.items():

                    if col_index >= len(row):
                        continue

                    rate = row[col_index].strip()

                    if not rate:
                        continue

                    # Extract valid decimal rate
                    rate_match = re.search(r"\d+\.\d{2}", rate)

                    if rate_match:
                        valid_rate = rate_match.group()
                    elif rate.lower() == "n/a":
                        valid_rate = "N/A"
                    else:
                        continue

                    # Duplicate record for each split origin
                    for single_origin in origin_parts:
                        for bpd in bpd_ranges:
                            records.append({
                                "Pipeline Name": pipeline_name,
                                "PointfOrigin": single_origin,
                                "PointOfDestination": dest,
                                "LiquidTariffNumber": "", 
                                "Effective Date": effective_date,
                                "End Date": expiry_date_value,
                                "TariffStatus": "Effective",
                                "RateTier": tier_name,
                                "RateType": tariff_rate_type,
                                "TermYear": "",
                                "MinBPD": bpd["MinBPD"],
                                "MaxBPD": bpd["MaxBPD"],
                                "AcreageDedicationMinAcres": "",
                                "AcreageDedicationMaxAcres": "",
                                "LiquidRateCentsPerBbl": valid_rate,
                                "SurchargeCentsPerBbl": "",
                                "LiquidFuelType": "Crude",
   
                            })

            break  # Only one relevant table on Page 5

    if records:
        return records

    print("No valid records extracted.")
    return None



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

    for i, page in enumerate(pdf.pages[5:6]):

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

        if (
            tariff_rate_type in text
            or "cents per Barrel" in text
            or "All rates are unchanged." in text
        ):
            print(f"Found target table on Page {i + 1}.")

       
        # page6 = pdf.pages[5]
        tables = page.extract_tables()

        for table in tables:
            if not table or len(table) < 2:
                continue

            header = [clean(col).lower() if col else "" for col in table[0]]

            origin_idx = None
            destination_idx = None
            rate_idx = None

            for i, col in enumerate(header):
                if "origin" in col:
                    origin_idx = i
                elif "destination" in col:
                    destination_idx = i
                elif "rate" in col:
                    rate_idx = i

            if origin_idx is None or destination_idx is None or rate_idx is None:
                continue

            for row in table[1:]:
                row = [clean(cell) if cell else "" for cell in row]

                origin = row[origin_idx]
                destination = row[destination_idx]
                rate = row[rate_idx]

                if origin and destination and is_rate(rate):
                    for bpd in bpd_ranges:
                        # Create record
                        unpivoted_data.append(
                            {
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
                                "MinBPD": bpd["MinBPD"],
                                "MaxBPD": bpd["MaxBPD"],
                                "AcreageDedicationMinAcres": "",
                                "AcreageDedicationMaxAcres": "",
                                "LiquidRateCentsPerBbl": rate,
                                "SurchargeCentsPerBbl": "",
                                "LiquidFuelType": "Crude",
                            }
                        )


    tariff_rate_type = ""
    return unpivoted_data



def extract_page7(pdf):
    unpivoted_data = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # with pdfplumber.open(pdf_path) as pdf:

    for i, page in enumerate(pdf.pages[6:7]):

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

        if (
            tariff_rate_type in text
            or "cents per Barrel" in text
            or "All rates are unchanged." in text
        ):
            print(f"Found target table on Page {i + 1}.")

        # -----------------------------
        # PAGE 6 (Index 5)
        # -----------------------------
        # page6 = pdf.pages[5]
        tables = page.extract_tables()

        for table in tables:
            for row in table:
                row = [clean(cell) for cell in row if cell]

                if len(row) < 2:
                    continue

                origin = row[0]

                rate_candidates = [cell for cell in row if is_rate(cell)]
                if not rate_candidates:
                    continue

                rate = rate_candidates[-1]

                # Destination is in header section of page 7
                destination = "Deeprock North Terminal in Cushing, OK"

                for bpd in bpd_ranges:
                    # Create record
                    unpivoted_data.append(
                        {
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
                            "MinBPD": bpd["MinBPD"],
                            "MaxBPD": bpd["MaxBPD"],
                            "AcreageDedicationMinAcres": "",
                            "AcreageDedicationMaxAcres": "",
                            "LiquidRateCentsPerBbl": rate,
                            "SurchargeCentsPerBbl": "",
                            "LiquidFuelType": "Crude",
                        }
                    )
            

    tariff_rate_type = ""
    return unpivoted_data



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

    rate_tier = extract_rate_tiers(text)

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
                # # keep RateTier empty.
                # tier_label = ""
                # if dest_col is not None and col_i != dest_col:
                #     # this is a secondary rate column like "Incremental Committed Shipper Rates"
                #     tier_label = header[col_i].strip()

                min_bpd, max_bpd = parse_volume_to_minmax(vol_val)

                records.append({
                    "Pipeline Name": pipeline_name,
                    "PointfOrigin": origin_val,
                    "PointOfDestination": destination_val,
                    "LiquidTariffNumber": "",
                    "Effective Date": effective_date,
                    "End Date": expiry_date_value,
                    "TariffStatus": "Effective",
                    "RateTier": rate_tier,
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

def split_origins(origin_cell):
    """
    Split origin cell into separate origins based on state code length.
    """
    if not origin_cell:
        return []

    # Clean the cell first to remove extra spaces
    origin_cell = origin_cell.strip()
    
    # Split by comma to check the last part (e.g., the State)
    parts_by_comma = origin_cell.split(',')
    last_part = parts_by_comma[-1].strip() if parts_by_comma else ""

    # CONDITION: If last value length is NOT less than 3 (e.g., "Wyoming")
    # We treat it as a list and split by newline or specific patterns.
    if len(last_part) > 3:
        # Split by newline if present, otherwise keep as is
        # (PDFs often group 'Guernsey, Wyoming \n Platteville, Colorado' with newlines)
        potential_list = origin_cell.split("\n")
        return [clean(p) for p in potential_list if clean(p)]

    # CONDITION: If last value length IS 3 or less (e.g., "CO", "WY", "KS")
    # Consider it a single origin from the table row
    return [origin_cell.replace("\n", " ").strip()]


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
        previous_origin = [] # Store as a list to maintain consistency

        for row in table[1:]:
            row = [cell if cell else "" for cell in row]

            # 1. Handle Destination Logic
            raw_dest = row[destination_idx] if destination_idx < len(row) else ""
            destination = clean(raw_dest)

            if destination:
                previous_destination = destination
            else:
                destination = previous_destination

            # 2. Handle Origin Logic
            origin_cell = row[origin_idx] if origin_idx < len(row) else ""
            
            # 🔥 FIXED: Changed from clean(destination) to clean(origin_cell)
            # This was the bug overwriting your origin with destination data.
            current_origin_clean = clean(origin_cell) 

            # 3. Determine if we use current cell or carry over from previous row
            if current_origin_clean:
                # 🔥 Use the new logic to split or keep as single origin
                origins = split_origins(origin_cell)
                previous_origin = origins
            else:
                # Carry over the list of origins from the row above
                origins = previous_origin

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


def is_rate_or_na(value: str) -> bool:
    if value is None:
        return False
    v = str(value).strip()
    if not v:
        return False
    if v.lower() in ("n/a", "na"):
        return True
    # allow decimals like 270.00
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", v))


def extract_term_year_from_page10_text(text):
    """
    Extract term year values dynamically from page text.
    Example:
      'Initial Term of one (1) year' -> '1'
      'Initial Term of four (4) years and three (3) months' -> '4.25'
    """
    term_map = {}

    pattern = re.findall(
        r"Initial\s+Term\s+of\s+[A-Za-z]+\s*\((\d+)\)\s*year[s]?(?:\s+and\s+[A-Za-z]+\s*\((\d+)\)\s*month[s]?)?",
        text,
        re.IGNORECASE
    )

    for idx, match in enumerate(pattern):
        years = int(match[0]) if match[0] else 0
        months = int(match[1]) if match[1] else 0

        # Build output format
        if months > 0:
            term_map[idx] = f"{years}.{months}"
        else:
            term_map[idx] = str(years)

    return term_map


def extract_page10(pdf):
    """
    Page 10 (0-based index 9): CONTRACT VOLUME INCENTIVE RATES
    Volume Incentive Program (April 2025)

    Two tables on this page:
      - Initial Term of one (1) year
      - Initial Term of four (4) years and three (3) months

    Output: unpivoted rows (one record per rate cell) with MinBPD/MaxBPD
    """
    records = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[9]  # Page 10
    text = page.extract_text() or ""

    tariff_rate_type = extract_tariff_rate_type(text)
    expiry_date_value = extract_expiry_date(text)

    tables = page.extract_tables() or []
    if not tables:
        return records

    # Dynamic term year extraction
    term_map = extract_term_year_from_page10_text(text)

    # Extract page-level rate tiers using your regex logic
    rate_tiers = extract_rate_tiers(text)

    for t_index, table in enumerate(tables):
        if not table or len(table) < 3:
            continue

        cleaned = []
        for row in table:
            cleaned.append([clean(c) for c in row])

        header_bpd = cleaned[0]
        header_rate_type = cleaned[1] if len(cleaned) > 1 else [""] * len(header_bpd)

        # Find origin/destination indices
        origin_idx = None
        dest_idx = None
        for i, col in enumerate(header_bpd):
            cl = col.lower()
            if "origin" in cl:
                origin_idx = i
            elif "destination" in cl:
                dest_idx = i

        if origin_idx is None or dest_idx is None:
            continue

        # Build column mappings
        col_maps = []

        for col_i in range(len(header_bpd)):
            if col_i in (origin_idx, dest_idx):
                continue

            bpd_label = header_bpd[col_i]

            # forward-fill BPD label
            if not bpd_label:
                for j in range(col_i - 1, -1, -1):
                    if header_bpd[j]:
                        bpd_label = header_bpd[j]
                        break

                
            if not bpd_label:
                continue

            min_bpd, max_bpd = parse_bpd_header_to_minmax(bpd_label)

            col_maps.append({
                "col_i": col_i,
                "min_bpd": min_bpd if min_bpd is not None else "",
                "max_bpd": max_bpd if max_bpd is not None else "",
            })

        if not col_maps:
            continue

        previous_origin = ""
        previous_dest = ""

        for row in cleaned[2:]:
            if len(row) < len(header_bpd):
                row = row + [""] * (len(header_bpd) - len(row))

            origin = row[origin_idx].strip()
            dest = row[dest_idx].strip()

            if origin:
                previous_origin = origin
            else:
                origin = previous_origin

            if dest:
                previous_dest = dest
            else:
                dest = previous_dest

            if not origin or not dest:
                continue

            origin = clean(origin)
            dest = clean(dest)

            for m in col_maps:
                rate_val = row[m["col_i"]].strip() if m["col_i"] < len(row) else ""
                if not is_rate_or_na(rate_val):
                    continue

                if rate_val.lower() in ("n/a", "na"):
                    rate_val = "N/A"
                else:
                    rate_val = str(rate_val)

                records.append({
                    "Pipeline Name": pipeline_name,
                    "PointfOrigin": origin,
                    "PointOfDestination": dest,
                    "LiquidTariffNumber": "",
                    "Effective Date": effective_date,
                    "End Date": expiry_date_value,
                    "TariffStatus": "Effective",
                    "RateTier": rate_tiers,
                    "RateType": tariff_rate_type,
                    "TermYear": term_map.get(t_index, ""),
                    "MinBPD": m["min_bpd"],
                    "MaxBPD": m["max_bpd"],
                    "AcreageDedicationMinAcres": "",
                    "AcreageDedicationMaxAcres": "",
                    "LiquidRateCentsPerBbl": rate_val,
                    "SurchargeCentsPerBbl": "",
                    "LiquidFuelType": "Crude",
                })

    return records




def get_col_value(row, idx):
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


def pad_row(row, target_len):
    if len(row) < target_len:
        return row + [""] * (target_len - len(row))
    return row


def find_header_row(rows, required_terms):
    """
    Find row index containing all required terms.
    """
    for i, row in enumerate(rows):
        row_text = " ".join([c.lower() for c in row if c])
        if all(term in row_text for term in required_terms):
            return i
    return None


def find_col_index(header_low, include_terms, exclude_terms=None):
    exclude_terms = exclude_terms or []
    for i, h in enumerate(header_low):
        if all(term in h for term in include_terms) and not any(term in h for term in exclude_terms):
            return i
    return 
    

def get_val(row, idx):
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


def split_origins_val(origin_cell):
    """
    Split combined origins at boundaries ending in ', XX'
    where XX is a 2-letter state code.

    Example:
    'Guernsey Located in Platte County, WY Sterling Located in Logan County, CO'
    ->
    [
        'Guernsey Located in Platte County, WY',
        'Sterling Located in Logan County, CO'
    ]
    """
    if not origin_cell:
        return []

    origin_cell = str(origin_cell).replace("\n", " ")
    origin_cell = re.sub(r"\s+", " ", origin_cell).strip()

    parts = re.split(r'(?<=,\s[A-Z]{2})\s+(?=[A-Z])', origin_cell)
    parts = [clean(p) for p in parts if clean(p)]

    return parts if parts else [clean(origin_cell)]


def extract_page11(pdf):
    """
    Page 11 (0-based index 10): CONTRACT VOLUME INCENTIVE RATES

    Fixes included:
    1) First table exports all 8 records, including both 206.88 values
    2) Second table splits combined origins correctly
    3) No hard-coded origin/destination values from PDF
    """
    records = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[10]  # Page 11
    text = page.extract_text() or ""

    tariff_rate_type = extract_tariff_rate_type(text)
    expiry_date_value = extract_expiry_date(text)

    tables = page.extract_tables() or []
    if not tables:
        return records

    def add_record(origin, dest, rate, rate_tier="", min_bpd="", max_bpd="", term_year=""):
        if rate is None:
            return

        rate_str = clean(rate)
        if not is_rate_or_na(rate_str):
            return

        if rate_str.lower() in ("n/a", "na"):
            rate_str = "N/A"

        records.append({
            "Pipeline Name": pipeline_name,
            "PointfOrigin": clean(origin),
            "PointOfDestination": clean(dest),
            "LiquidTariffNumber": "",
            "Effective Date": effective_date,
            "End Date": expiry_date_value,
            "TariffStatus": "Effective",
            "RateTier": rate_tier,
            "RateType": tariff_rate_type,
            "TermYear": term_year,
            "MinBPD": min_bpd if min_bpd is not None else "",
            "MaxBPD": max_bpd if max_bpd is not None else "",
            "AcreageDedicationMinAcres": "",
            "AcreageDedicationMaxAcres": "",
            "LiquidRateCentsPerBbl": rate_str,
            "SurchargeCentsPerBbl": "",
            "LiquidFuelType": "Crude",
        })

    for table in tables:
        if not table or len(table) < 2:
            continue

        # raw rows preserve line breaks for origin splitting
        raw_rows = [[("" if c is None else str(c)) for c in row] for row in table]

        cleaned = [[clean(c) for c in row] for row in table]
        max_len = max(len(r) for r in cleaned)
        cleaned = [pad_row(r, max_len) for r in cleaned]
        raw_rows = [pad_row(r, max_len) for r in raw_rows]

        flat_text = " ".join([" ".join(r).lower() for r in cleaned])

        # ---------------------------------------------------------
        # TABLE 1: Main Shipper A / Shipper B table
        # ---------------------------------------------------------
        if "shipper a" in flat_text and "shipper b" in flat_text:
            header_idx = find_header_row(cleaned, ["origin", "destination"])
            if header_idx is None:
                continue

            header = cleaned[header_idx]
            header_low = [h.lower() for h in header]

            tier_col = find_col_index(header_low, ["tier"])
            mv_col = find_col_index(header_low, ["minimum", "volume"])
            if mv_col is None:
                mv_col = find_col_index(header_low, ["commitment"])

            origin_col = find_col_index(header_low, ["origin"])
            dest_col = find_col_index(header_low, ["destination"])
            ship_a_incent_col = find_col_index(header_low, ["shipper", "a", "incentive"])
            ship_a_extra_col = find_col_index(header_low, ["shipper", "a", "extra"])
            ship_b_incent_col = find_col_index(header_low, ["shipper", "b", "incentive"])
            ship_b_extra_col = find_col_index(header_low, ["shipper", "b", "extra"])

            if tier_col is None:
                tier_col = 0

            prev_origin = ""
            prev_dest = ""

            for row in cleaned[header_idx + 1:]:
                row = pad_row(row, len(header))

                tier_val = get_val(row, tier_col)
                mv_val = get_val(row, mv_col)
                origin = get_val(row, origin_col)
                dest = get_val(row, dest_col)

                if origin:
                    prev_origin = origin
                else:
                    origin = prev_origin

                if dest:
                    prev_dest = dest
                else:
                    dest = prev_dest

                if not origin or not dest:
                    continue

                min_bpd, max_bpd = ("", "")
                if mv_val and "bpd" in mv_val.lower():
                    min_bpd, max_bpd = parse_volume_to_minmax(mv_val)
                elif tier_val and "bpd" in tier_val.lower():
                    min_bpd, max_bpd = parse_volume_to_minmax(tier_val)

                derived_tier =  extract_rate_tier_label(tier_val)
                if not derived_tier:
                    derived_tier =  extract_rate_tier_label(mv_val)

                a_incent = get_val(row, ship_a_incent_col)
                a_extra = get_val(row, ship_a_extra_col)
                b_incent = get_val(row, ship_b_incent_col)
                b_extra = get_val(row, ship_b_extra_col)

                # Export all 4 rate cells independently if present
                if a_incent:
                    add_record(origin, dest, a_incent, derived_tier, min_bpd, max_bpd)
                if a_extra:
                    add_record(origin, dest, a_extra, derived_tier, min_bpd, max_bpd)
                if b_incent:
                    add_record(origin, dest, b_incent, derived_tier, min_bpd, max_bpd)
                if b_extra:
                    add_record(origin, dest, b_extra, derived_tier, min_bpd, max_bpd)

            continue

        # ---------------------------------------------------------
        # TABLE 2: Secondary Origin Barrel Rate table
        # ---------------------------------------------------------
        if "secondary origin" in flat_text:
            header_idx = find_header_row(cleaned, ["origin", "destination"])
            if header_idx is None:
                header_idx = find_header_row(cleaned, ["secondary", "origin"])
            if header_idx is None:
                continue

            header = cleaned[header_idx]
            header_low = [h.lower() for h in header]

            tier_col = find_col_index(header_low, ["tier"])
            vol_col = find_col_index(header_low, ["volume"])
            origin_col = find_col_index(header_low, ["origin"])
            dest_col = find_col_index(header_low, ["destination"])
            rate_col = find_col_index(header_low, ["secondary", "origin", "rate"])

            if rate_col is None:
                for i in range(len(header_low)):
                    sample_vals = [get_val(r, i) for r in cleaned[header_idx + 1:]]
                    if any(is_rate_or_na(v) for v in sample_vals):
                        rate_col = i
                        break

            prev_origin_clean = ""
            prev_origin_raw = ""
            prev_dest = ""

            for row_idx in range(header_idx + 1, len(cleaned)):
                row = pad_row(cleaned[row_idx], len(header))
                raw_row = pad_row(raw_rows[row_idx], len(header))

                tier_val = get_val(row, tier_col)
                vol_val = get_val(row, vol_col)
                rate_val = get_val(row, rate_col)

                origin_clean = get_val(row, origin_col)
                origin_raw = raw_row[origin_col] if origin_col is not None and origin_col < len(raw_row) else ""

                dest = get_val(row, dest_col)

                if origin_clean:
                    prev_origin_clean = origin_clean
                    prev_origin_raw = origin_raw
                else:
                    origin_clean = prev_origin_clean
                    origin_raw = prev_origin_raw

                if dest:
                    prev_dest = dest
                else:
                    dest = prev_dest

                if not origin_clean or not dest or not rate_val:
                    continue

                min_bpd, max_bpd = ("", "")
                source_for_bpd = vol_val if vol_val and "bpd" in vol_val.lower() else tier_val
                if source_for_bpd and "bpd" in source_for_bpd.lower():
                    min_bpd, max_bpd = parse_volume_to_minmax(source_for_bpd)

                derived_tier = extract_rate_tier_label(tier_val)
                if not derived_tier:
                    derived_tier =  extract_rate_tier_label(vol_val)

                origins = split_origins_val(origin_raw if origin_raw else origin_clean)

                for single_origin in origins:
                    add_record(single_origin, dest, rate_val, derived_tier, min_bpd, max_bpd)

            continue

        # ---------------------------------------------------------
        # TABLE 3: Buckingham Barrel Rate table
        # ---------------------------------------------------------
        if "buckingham barrel rate" in flat_text:
            header_idx = find_header_row(cleaned, ["origin", "destination"])
            if header_idx is None:
                continue

            header = cleaned[header_idx]
            header_low = [h.lower() for h in header]

            origin_col = find_col_index(header_low, ["origin"])
            dest_col = find_col_index(header_low, ["destination"])
            rate_col = find_col_index(header_low, ["rate"])

            if rate_col is None:
                for i in range(len(header_low)):
                    sample_vals = [get_val(r, i) for r in cleaned[header_idx + 1:]]
                    if any(is_rate_or_na(v) for v in sample_vals):
                        rate_col = i
                        break

            prev_origin = ""
            prev_dest = ""

            for row in cleaned[header_idx + 1:]:
                row = pad_row(row, len(header))

                origin = get_val(row, origin_col)
                dest = get_val(row, dest_col)
                rate_val = get_val(row, rate_col)

                if origin:
                    prev_origin = origin
                else:
                    origin = prev_origin

                if dest:
                    prev_dest = dest
                else:
                    dest = prev_dest

                if not origin or not dest or not rate_val:
                    continue

                add_record(origin, dest, rate_val, "", "", "")

            continue

    return records





if __name__ == "__main__":
    
    tariff_data = []

    # --- Execution ---
    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"
        
    with pdfplumber.open(file_name) as pdf:
        
        # df_rates1 = extract_rates_table_for_Page3_4_12_13_14_15(pdf, 2, 4)
        # tariff_data.extend(df_rates1)

        # df_rates5 = extract_page_5(pdf)
        # tariff_data.extend(df_rates5)

        # df_rates6 = extract_page6(pdf)
        # tariff_data.extend(df_rates6)

        # df_rates7 = extract_page7(pdf)
        # tariff_data.extend(df_rates7)

        # df_rates8 = extract_page8(pdf)
        # tariff_data.extend(df_rates8)

        # df_rates9 = extract_page9(pdf)
        # tariff_data.extend(df_rates9)

        df_rates10 = extract_page10(pdf)
        tariff_data.extend(df_rates10)

        # df_rates11 = extract_page11(pdf)
        # tariff_data.extend(df_rates11)

        # df_rates12 = extract_rates_table_for_Page3_4_12_13_14_15(pdf, 11, 15)
        # tariff_data.extend(df_rates12)

        
    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:

        # Export to CSV
        output_file = "sample_tariff_data_v5.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {output_file}")
    else:
        print("\nFailed to extract table data.")

