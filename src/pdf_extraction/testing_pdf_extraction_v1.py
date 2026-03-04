import pdfplumber
import pandas as pd

pdf_path = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

results = []

def clean_text(val):
    if val is None:
        return "n/a"
    return str(val).replace("\n", " ").strip()

with pdfplumber.open(pdf_path) as pdf:
    
    page_number = 9   # Page 10 (0-based index)
    page = pdf.pages[page_number]
    
    tables = page.extract_tables()

    for table in tables:
        df = pd.DataFrame(table)
        df = df.dropna(how="all").reset_index(drop=True)

        # Skip invalid tables
        if df.shape[0] < 3:
            continue

        # First usable header row usually contains "Origin"
        header_row_index = None
        for i in range(len(df)):
            if "Origin" in str(df.iloc[i].tolist()):
                header_row_index = i
                break
        
        if header_row_index is None:
            continue

        headers = df.iloc[header_row_index]
        df = df[header_row_index + 1:]
        df.columns = headers

        # Clean column names
        df.columns = [clean_text(col) for col in df.columns]

        origin_col = "Origin"
        destination_col = "Destination"

        for _, row in df.iterrows():
            origin = clean_text(row.get(origin_col))
            destination = clean_text(row.get(destination_col))

            # Loop through all other columns (rate columns)
            for col in df.columns:
                if col in [origin_col, destination_col]:
                    continue

                rate = clean_text(row[col])

                if rate == "":
                    continue

                results.append({
                    "Page": "Page 10",
                    "PointOfOrigin": origin,
                    "PointOfDestination": destination,
                    "Rate": rate
                })

# Create final dataframe
final_df = pd.DataFrame(results)

print(final_df)

# Optional: Save output
final_df.to_csv("Page_10_CrossJoin_Output.csv", index=False)
print("\nSaved as Page_10_CrossJoin_Output.csv")