import pdfplumber
import pandas as pd
import re


def extract_rates_table_from_text(file_path):
    """
    Extract the 'NON-CONTRACT TRANSPORTATION RATES' table by parsing text directly.
    """
    print(f"--- Extracting Rates Table from {file_path} ---\n")

    with pdfplumber.open(file_path) as pdf:
        # Search through all pages to find the one with "NON-CONTRACT TRANSPORTATION RATES"
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            # Check for both the title and the presence of rate information
            if (
                "NON-CONTRACT TRANSPORTATION RATES" in text
                and "cents per Barrel" in text
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
                                                "Origin": origin,
                                                "Destination": destination,
                                                "Rate": rate,
                                            }
                                        )

                            if unpivoted_data:
                                df_final = pd.DataFrame(unpivoted_data)

                                # --- Refining Splitting Logic ---
                                def split_complex(val):
                                    if not val:
                                        return []
                                    # Use regex to split by 'Located in', ' in ' (case insensitive) or ','
                                    parts = re.split(
                                        r",|\s+Located\s+in\s+|\s+in\s+",
                                        val,
                                        flags=re.IGNORECASE,
                                    )
                                    return [p.strip() for p in parts if p.strip()]

                                # Process Origins
                                origin_parts = df_final["Origin"].apply(split_complex)
                                max_o_parts = origin_parts.apply(len).max()
                                for i in range(max_o_parts):
                                    df_final[f"Origin_Part_{i+1}"] = origin_parts.apply(
                                        lambda x: x[i] if i < len(x) else ""
                                    )

                                # Process Destinations
                                dest_parts = df_final["Destination"].apply(
                                    split_complex
                                )
                                max_d_parts = dest_parts.apply(len).max()
                                for i in range(max_d_parts):
                                    col_name = f"Destination_Part_{i + 1}"
                                    df_final[col_name] = dest_parts.apply(
                                        lambda x: x[i] if i < len(x) else ""
                                    )
                                    # Slicing to first 2 letters for Part 3 (State)
                                    if col_name == "Destination_Part_3":
                                        df_final[col_name] = df_final[col_name].str[:2]

                                return df_final

                # If table extraction didn't work, try text parsing
                print("Table extraction failed or table too small.")
                return None

        print("Target table not found.")
        return None


# --- Execution ---
file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"
    
# Extract Specific Table
df_rates = extract_rates_table_from_text(file_name)

if df_rates is not None and len(df_rates) > 0:
    print("\n--- Extracted and Unpivoted Table Data ---")
    print(df_rates.head(20).to_markdown(index=False))

    # Export to CSV
    output_file = "extracted_rates_vfinal_2.csv"
    df_rates.to_csv(output_file, index=False)
    print(f"\nData successfully exported to {output_file}")

    # Print the first few records as requested
    print("\n--- Sample Records ---")
    for idx, row in df_rates.head(5).iterrows():
        print(
            f"Origin: {row['Origin']} | Destination: {row['Destination']} | Rate: {row['Rate']}"
        )
else:
    print("\nFailed to extract table data.")

