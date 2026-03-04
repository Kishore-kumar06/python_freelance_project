import pdfplumber
import pandas as pd
import re

pdf_path = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"



output_csv = "Page9_Output.csv"


def clean(text):
    if text:
        return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()
    return ""


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


def extract_page9(pdf_path):
    results = []

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[8]  # Page 9
        tables = page.extract_tables()

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
                                    "Origin": origin,
                                    "Destination": destination,
                                    "Rate": rate_value
                                })

    df = pd.DataFrame(results).drop_duplicates().reset_index(drop=True)
    return df


# Run extraction
df_page9 = extract_page9(pdf_path)

print(df_page9)

df_page9.to_csv(output_csv, index=False)

print(f"\nPage 9 extraction completed → {output_csv}")