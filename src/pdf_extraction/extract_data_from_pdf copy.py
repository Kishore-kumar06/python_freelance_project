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
    

def extract_rates_table_from_text(pdf):
    print(f"--- Extracting Rates Table from {pdf} ---\n")

    unpivoted_data = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # Search through all pages to find the one with "NON-CONTRACT TRANSPORTATION RATES"
    for i, page in enumerate(pdf.pages[2:4]):

        page_number = page.page_number

        text = page.extract_text()

        if not text:
            continue

        rate_type = extract_tariff_rate_type(text)
        if rate_type:
            tariff_rate_type = rate_type
        else:
            if rate_type == "":
                tariff_rate_type = tariff_rate_type

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

                                    # Create record
                                    unpivoted_data.append(
                                        {
                                            "Pipeline": pipeline_name,
                                            "EffectiveDate": effective_date,
                                            "PageNumber": page_number,
                                            "RateType": tariff_rate_type,
                                            "Origin": origin,
                                            "Destination": destination,
                                            "Rate": rate,
                                        }
                                    )

                                    if unpivoted_data:
                                        df_final = pd.DataFrame(unpivoted_data)
    tariff_rate_type = ""  # Reset for next page
    return df_final

 


def extract_simple_origin_destination_tables(pdf):

    print("\n--- Extracting Simple Origin-Destination Tables ---\n")

    all_records = []

    for page_index, page in enumerate(pdf.pages[7:8]):

        text = page.extract_text()
        if not text:
            continue

        # Only process valid rate pages
        if "All rates are unchanged." not in text:
            continue

        print(f"\nProcessing Page {page_index + 1}")

        tables = page.extract_tables()
        if not tables:
            continue

        for table_index, table in enumerate(tables):

            if not table or len(table) < 2:
                continue

            print(f"  → Checking Table {table_index + 1}")

            previous_origin = ""

            for row in table:

                # Clean row
                row = [
                    cell.replace("\n", " ").strip() if cell else ""
                    for cell in row
                ]

                if len(row) < 3:
                    continue

                origin = row[0]
                destination = row[1]
                rate = row[2]

                # Skip header row
                header_check = " ".join(row).lower()
                if "origin" in header_check and "destination" in header_check:
                    continue

                # Forward-fill origin if blank
                if origin:
                    previous_origin = origin
                else:
                    origin = previous_origin

                # If still no origin → skip
                if not origin:
                    continue

                # Validate rate format
                if rate:
                    rate_clean = rate.strip()

                    # Accept decimal rates like 233.00 or 54.44
                    if re.match(r"^\d+\.\d{2}$", rate_clean):
                        pass
                    # Accept N/A or n/a
                    elif rate_clean.lower() == "n/a":
                        rate_clean = "N/A"
                    else:
                        continue  # ignore invalid rows
                else:
                    continue

                # Clean spacing
                origin = " ".join(origin.split())
                destination = " ".join(destination.split())

                if not destination:
                    continue

                all_records.append({
                    "Origin": origin,
                    "Destination": destination,
                    "Rate": rate_clean
                })

    if all_records:
        df = pd.DataFrame(all_records)
        print("\nExtraction Complete.")
        return df

    print("No valid records found.")
    return None


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



def extract_page_5(pdf):

    print("\n--- Extracting Page 5 (Final Version) ---\n")

    records = []
    tariff_rate_type = ""

    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    # with pdfplumber.open(pdf_path) as pdf:

    for page_index, page in enumerate(pdf.pages[4:5]):

        text = page.extract_text()

        rate_type = extract_tariff_rate_type(text)
        if rate_type:
            tariff_rate_type = rate_type
        else:
            if rate_type == "":
                tariff_rate_type = tariff_rate_type

        expiry_date_value = extract_expiry_date(text)

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
                        records.append({
                            "Pipeline": pipeline_name,
                            "EffectiveDate": effective_date,
                            "RateType": tariff_rate_type,
                            "ExpiryDate": expiry_date_value,
                            "Origin": single_origin,
                            "Destination": dest,
                            "RateTier": tier_name,
                            "Rate": valid_rate
                        })

            break  # Only one relevant table on Page 5

    if records:
        df = pd.DataFrame(records)
        print("Page 5 extraction successful.")
        return df

    print("No valid records extracted.")
    return None


if __name__ == "__main__":
    # --- Execution ---
    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"
        
    with pdfplumber.open(file_name) as pdf:
        df_rates = extract_page_5(pdf)
    
    # with pdfplumber.open(file_name) as pdf:
       
    #     # Extract Specific Table
    #     df_rates = extract_rates_table_from_text(pdf)

    #     # df_rates = extract_simple_origin_destination_tables(pdf)

    if df_rates is not None and len(df_rates) > 0:

        # Export to CSV
        output_file = "extracted_rates_vfinal_5.csv"
        df_rates.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {output_file}")

        # Print the first few records as requested
        print("\n--- Sample Records ---")
        # for idx, row in df_rates.head(5).iterrows():
        #     print(
        #         f"Origin: {row['Origin']} | Destination: {row['Destination']} | Rate: {row['Rate']}"
        #     )
    else:
        print("\nFailed to extract table data.")

