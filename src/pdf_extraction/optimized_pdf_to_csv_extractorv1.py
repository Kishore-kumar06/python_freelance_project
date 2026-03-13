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
    

def verify_tables(pdf, start_page_number, end_page_number):
    print(f"--- Extracting Rates Table from {pdf} ---\n")

    unpivoted_data = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)


    # enumerate start value adjusted so it prints the actual PDF page number
    # Note: pdfplumber is 0-indexed, so start_page_number=2 is Page 3.
    for i, page in enumerate(pdf.pages[start_page_number:end_page_number], start=start_page_number + 1):

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
       

        # Extract table using pdfplumber's table extraction
        tables = page.extract_tables()

        if tables:
            print(f"Found {len(tables)} table(s) on page {i}.")

            for table in tables:
                # Basic safety checks
                if not table or len(table) <= 2 or not table[0] or len(table[0]) <= 2:
                    continue

                destination = (table[0][2] or "").strip()
                origin = (table[2][0] or "").strip()

                # Fix reversed "Origins"
                if origin == "snigirO":
                    origin = origin[::-1]

                if ("Destination" in destination or "Destinations" in destination) and \
                    ("Origin" in origin or "Origins" in origin):
                    print(f"Page {page} value: {destination}")
            
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
    
    tariff_rate_type = ""  # Reset for next page
    return df_final



# --- Execution ---
file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"
        
with pdfplumber.open(file_name) as pdf:
    
    tariff_data = []
    data = verify_tables(pdf,2,15)
    tariff_data.extend(data)


    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:

        # Export to CSV
        output_file = "sample_tariff_data_v4.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {output_file}")
    else:
        print("\nFailed to extract table data.")