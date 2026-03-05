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


def extract_rates_table_from_text(pdf):
    print(f"--- Extracting Rates Table from {pdf} ---\n")

    unpivoted_data = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # Search through all pages to find the one with "NON-CONTRACT TRANSPORTATION RATES"
    for i, page in enumerate(pdf.pages[2:15]):

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

        print(tariff_rate_type)
        
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

                for idx, table in enumerate(tables):
                    print(f"\n--- Table {idx + 1} ---")
                    print(f"Rows: {len(table)}")
                    print(f"Columns: {len(table[0]) if table else 0}")
                    print("\nFirst 10 rows:")
                    for i, row in enumerate(table[:10]):
                        print(f"Row {i}: {row}")

                # Usually the rates table is the largest one
                largest_table = max(tables, key=lambda t: len(t) if t else 0)

                if largest_table and len(largest_table) > 1:
                    # Clean the table data
                    cleaned_table = []
                    for row in largest_table:
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
    
    tariff_rate_type = ""  # Reset for next page
    return df_final

 

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

    for i, page in enumerate(pdf.pages[12:13]):

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





if __name__ == "__main__":
    
    tariff_data = []

    # --- Execution ---
    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"
        
    with pdfplumber.open(file_name) as pdf:
        
        # df_rates = extract_rates_table_from_text(pdf)
        # tariff_data.extend(df_rates)

        # df_rates = extract_page_5(pdf)
        # tariff_data.extend(df_rates)

        df_rates6 = extract_page6(pdf)
        tariff_data.extend(df_rates6)

        # df_rates = extract_page7(pdf)
        # tariff_data.extend(df_rates)
        
    final_data = pd.DataFrame(tariff_data)
    # final_data = final_data.drop_duplicates()
    # final_data = final_data.reset_index(drop=True)

    if final_data is not None and len(final_data) > 0:

        # Export to CSV

        output_file = "extracted_rates_vfinal_15.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {output_file}")
    else:
        print("\nFailed to extract table data.")

