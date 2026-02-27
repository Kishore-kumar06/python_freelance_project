import logging
import os
import re
from typing import List, Optional, Dict, Any

import pandas as pd
import pdfplumber

# --- Constants & Configuration ---
TARGET_TABLE_KEYWORDS = ["NON-CONTRACT TRANSPORTATION RATES", "cents per Barrel"]
DEFAULT_OUTPUT_FILE = "extracted_rates_docling.csv"
LOCATION_SPLIT_REGEX = r",|\s+Located\s+in\s+|\s+in\s+"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class OilTariffExtractor:
    """
    Cleaner and more robust extractor for Oil Tariff PDF data using pdfplumber.
    """

    def __init__(self):
        pass

    def extract(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Main entry point for extraction.
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        logger.info(f"Extracting Rates Table from {file_path}")

        try:
            with pdfplumber.open(file_path) as pdf:
                for page_idx, page in enumerate(pdf.pages):
                    text = page.extract_text() or ""
                    if all(keyword in text for keyword in TARGET_TABLE_KEYWORDS):
                        logger.info(f"Found target table on Page {page_idx + 1}")

                        tables = page.extract_tables()
                        if not tables:
                            continue

                        # Usually the rates table is the largest one
                        largest_table = max(tables, key=lambda t: len(t) if t else 0)
                        if largest_table and len(largest_table) > 2:
                            return self._process_table(largest_table)

        except Exception as e:
            logger.error(f"Error during PDF processing: {e}")
            return None

        logger.warning("Target table not found in the document.")
        return None

    def _process_table(self, table_data: List[List[Any]]) -> pd.DataFrame:
        """
        Cleans, unpivots, and refines the table data.
        """
        # Initial Cleaning
        cleaned_table = []
        for row in table_data:
            cleaned_row = [
                str(cell).replace("\n", " ").strip() if cell is not None else ""
                for cell in row
            ]
            if any(cleaned_row):
                cleaned_table.append(cleaned_row)

        if len(cleaned_table) < 3:
            logger.warning("Table found but it is too small to contain data.")
            return pd.DataFrame()

        # Unpivoting logic
        unpivoted_data = []
        dest_headers = cleaned_table[1]

        for row in cleaned_table[2:]:
            if len(row) < 2:
                continue

            origin = row[1].strip()
            if not origin or origin.lower() == "none":
                continue

            for col_idx in range(2, len(row)):
                if col_idx >= len(dest_headers):
                    break

                destination = dest_headers[col_idx].strip() or f"Unknown_Dest_{col_idx}"
                rate = row[col_idx].strip()

                if rate:
                    unpivoted_data.append(
                        {
                            "Origin": origin,
                            "Destination": destination,
                            "Rate": rate,
                        }
                    )

        if not unpivoted_data:
            logger.warning("No rate data found during unpivoting.")
            return pd.DataFrame()

        df_final = pd.DataFrame(unpivoted_data)
        return self._refine_parts(df_final)

    def _refine_parts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Splits Origin and Destination into granular parts.
        """

        def split_complex(val: str) -> List[str]:
            if not val:
                return []
            parts = re.split(LOCATION_SPLIT_REGEX, val, flags=re.IGNORECASE)
            return [p.strip() for p in parts if p.strip()]

        # Process Origins
        origin_parts = df["Origin"].apply(split_complex)
        max_o_parts = origin_parts.apply(len).max()
        if max_o_parts:
            for i in range(max_o_parts):
                df[f"Origin_Part_{i+1}"] = origin_parts.apply(
                    lambda x: x[i] if i < len(x) else ""
                )

        # Process Destinations
        dest_parts = df["Destination"].apply(split_complex)
        max_d_parts = dest_parts.apply(len).max()
        if max_d_parts:
            for i in range(max_d_parts):
                col_name = f"Destination_Part_{i + 1}"
                df[col_name] = dest_parts.apply(lambda x: x[i] if i < len(x) else "")
                if col_name == "Destination_Part_3":
                    df[col_name] = df[col_name].str[:2].str.upper()

        return df


def main():
    """
    Main execution logic for the script.
    """
    pdf_path = r"OilTariffFiles\Pony Express Pipeline.PDF"
    extractor = OilTariffExtractor()

    df_rates = extractor.extract(pdf_path)

    if df_rates is not None and not df_rates.empty:
        logger.info(f"Successfully extracted {len(df_rates)} rate records.")

        print("\n--- Extracted Table Data (Sample) ---")
        print(df_rates.head(10).to_markdown(index=False))

        df_rates.to_csv(DEFAULT_OUTPUT_FILE, index=False)
        logger.info(f"Data exported to {DEFAULT_OUTPUT_FILE}")
    else:
        logger.error("Failed to extract data or no data found.")


if __name__ == "__main__":
    main()
