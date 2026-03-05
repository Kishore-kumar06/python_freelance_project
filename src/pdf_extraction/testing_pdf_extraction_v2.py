import pdfplumber
import pandas as pd
import re
from datetime import datetime

# -----------------------------
# Helpers (same style as your existing file)
# -----------------------------
def clean(text):
    if text:
        return re.sub(r"\s+", " ", str(text).replace("\n", " ")).strip()
    return ""

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

def extract_pipeline_metadata(pdf):
    pipeline_name = ""
    effective_date = ""

    page1_text = pdf.pages[0].extract_text() or ""

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
        text_clean = (text or "").replace("\r", "")
        lines = text_clean.split("\n")

        tariff_lines = []
        for i, line in enumerate(lines):
            clean_line = line.strip()
            if "RATES" in clean_line:
                tariff_lines.append(clean_line)
                for j in range(1, 3):
                    if i + j < len(lines):
                        nxt = lines[i + j].strip()
                        if nxt and nxt.isupper():
                            tariff_lines.append(nxt)
                        else:
                            break
                break

        if tariff_lines:
            return " ".join(tariff_lines).strip()
        return ""

    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""

def extract_expiry_date(text):
    # Page 10 normally doesn't have expiry wording, keep consistent with your pipeline
    try:
        expiry_date = ""
        expiry_match = re.search(
            r"expire[s]?\s+on.*?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            text or "",
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

# -----------------------------
# PAGE 10 extractor
# -----------------------------
def extract_page10(pdf):
    """
    Page 10 (0-based index 9): CONTRACT VOLUME INCENTIVE RATES
    Volume Incentive Program (April 2025)
    Two tables on this page:
      - Initial Term of one (1) year
      - Initial Term of four (4) years and three (3) months

    Output: unpivoted rows (one record per rate cell) with MinBPD/MaxBPD
            and RateTier = 'Incentive Rate' / 'Secondary Origin Barrel Rate'
    """
    records = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[10]  # Page 10
    text = page.extract_text() or ""

    tariff_rate_type = extract_tariff_rate_type(text)
    expiry_date_value = extract_expiry_date(text)

    tables = page.extract_tables() or []
    if not tables:
        return records

    # Table order on this PDF is consistent: table[0] = 1-year, table[1] = 4y3m
    term_map = {
        0: "1",      # 1 year
        1: "4.25",   # 4 years 3 months = 4.25 years
    }

    for t_index, table in enumerate(tables):
        if not table or len(table) <= 3:
            continue

        # Clean cells
        cleaned = []
        for row in table:
            cleaned.append([clean(c) for c in row])

        # We expect:
        # row0: ['Origin','Destination','5,000 – 11,999 BPD', None, '12,000 – 23,999 BPD', None, '24,000 or greater BPD', None]
        # row1: [None,None,'Incentive Rate','Secondary Origin Barrel Rate', ... repeat ...]
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

        # Build column mappings for rate cells:
        # For each BPD block, there are 2 columns: Incentive Rate & Secondary Origin Barrel Rate
        col_maps = []  # list of dict: {col_i, rate_tier, min_bpd, max_bpd}
        for col_i in range(len(header_bpd)):
            if col_i in (origin_idx, dest_idx):
                continue

            bpd_label = header_bpd[col_i]
            # when the bpd header uses None placeholders, the label may be ""
            # forward-fill bpd label from the left
            if not bpd_label:
                # search left for nearest bpd label
                for j in range(col_i - 1, -1, -1):
                    if header_bpd[j]:
                        bpd_label = header_bpd[j]
                        break

            rate_type_label = header_rate_type[col_i]  # 'Incentive Rate' or 'Secondary Origin Barrel Rate'
            rate_type_label = clean(rate_type_label)

            if not bpd_label or not rate_type_label:
                continue

            min_bpd, max_bpd = parse_bpd_header_to_minmax(bpd_label)

            col_maps.append({
                "col_i": col_i,
                "rate_tier": rate_type_label,  # keep clean: Incentive Rate / Secondary Origin Barrel Rate
                "min_bpd": min_bpd if min_bpd is not None else "",
                "max_bpd": max_bpd if max_bpd is not None else "",
            })

        if not col_maps:
            continue

        previous_origin = ""
        previous_dest = ""

        # Data rows start from row index 2
        for row in cleaned[2:]:
            # pad row
            if len(row) < len(header_bpd):
                row = row + [""] * (len(header_bpd) - len(row))

            origin = row[origin_idx].strip()
            dest = row[dest_idx].strip()

            # forward fill like your other pages
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
                    # keep numeric as string for consistency with your CSV output
                    rate_val = str(rate_val)

                records.append({
                    "Pipeline Name": pipeline_name,
                    "PointfOrigin": origin,
                    "PointOfDestination": dest,
                    "LiquidTariffNumber": "",
                    "Effective Date": effective_date,
                    "End Date": expiry_date_value,
                    "TariffStatus": "Effective",
                    "RateTier": m["rate_tier"],
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


# -----------------------------
# Example main (match your style)
# -----------------------------
if __name__ == "__main__":
    tariff_data = []

    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        df_rates10 = extract_page10(pdf)
        tariff_data.extend(df_rates10)

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:
        output_file = "extracted_page11_contract_volume_incentive_rates.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nPage 10 data successfully exported to {output_file}")
    else:
        print("\nFailed to extract Page 10 table data.")