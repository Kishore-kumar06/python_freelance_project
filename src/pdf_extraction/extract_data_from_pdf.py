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

        tariff_rate_type = " ".join(tariff_lines)
        return tariff_rate_type.strip()

    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""
    

def extract_rates_table_from_text(file_path):
    print(f"--- Extracting Rates Table from {file_path} ---\n")

    
    with pdfplumber.open(file_path) as pdf:

        pipeline_name, effective_date = extract_pipeline_metadata(pdf)

        # Search through all pages to find the one with "NON-CONTRACT TRANSPORTATION RATES"
        for i, page in enumerate(pdf.pages[2:4]):
            text = page.extract_text()

            rate_type = extract_tariff_rate_type(text)
            print(rate_type)
            

            # Check for both the title and the presence of rate information
            if (
                rate_type in text
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

                            unpivoted_data = []
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
                                                "RateType": rate_type,
                                                "Origin": origin,
                                                "Destination": destination,
                                                "Rate": rate,
                                            }
                                        )

                            if unpivoted_data:
                                df_final = pd.DataFrame(unpivoted_data)

                                return df_final

                # If table extraction didn't work, try text parsing
                print("Table extraction failed or table too small.")
                return None

        print("Target table not found.")
        return None


import pdfplumber
import pandas as pd


def extract_page_5_rates(pdf_path):

    print("\n--- Extracting Page 5 Data ---\n")

    all_records = []

    with pdfplumber.open(pdf_path) as pdf:

        page = pdf.pages[4]  # Page 5 (0-based index)

        text = page.extract_text()
        if not text or "All rates are unchanged." not in text:
            print("Keyword not found on Page 5.")
            return None

        tables = page.extract_tables()

        if not tables:
            print("No tables found on Page 5.")
            return None

        for table in tables:

            if not table or len(table) < 2:
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

            if len(cleaned_table) < 2:
                continue

            header = cleaned_table[0]

            # Identify tier columns
            tier_columns = {}
            for idx, col in enumerate(header):
                if col and "rate tier" in col.lower():
                    tier_columns[idx] = col.strip()

            # Ensure this is correct structure
            if not tier_columns:
                continue

            # Process rows
            for row in cleaned_table[1:]:

                if len(row) < 2:
                    continue

                origin = row[0].strip() if row[0] else ""
                destination = row[1].strip() if len(row) > 1 and row[1] else ""

                if not origin or not destination:
                    continue

                for col_idx, tier_name in tier_columns.items():

                    if col_idx >= len(row):
                        continue

                    rate = row[col_idx]
                    if not rate:
                        continue

                    all_records.append({
                        "Origin": origin,
                        "Destination": destination,
                        "RateTier": tier_name,
                        "Rate": rate.strip()
                    })

    if all_records:
        return pd.DataFrame(all_records)

    print("No valid data extracted.")
    return None



# --- Execution ---
file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"
    
# Extract Specific Table
df_rates = extract_rates_table_from_text(file_name)

# df_rates = extract_page_5_rates(file_name)

if df_rates is not None and len(df_rates) > 0:
    print("\n--- Extracted and Unpivoted Table Data ---")
    print(df_rates.head(20).to_markdown(index=False))

    # Export to CSV
    output_file = "extracted_rates_vfinal_2.csv"
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

