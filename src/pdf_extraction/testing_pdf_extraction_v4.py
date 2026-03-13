import pdfplumber
import pandas as pd
import re
from datetime import datetime

# -----------------------------
# Helpers
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


def parse_bpd_tier(tier_text: str):
    """
    Examples:
      Tier I: 5,000 – 19,999 BPD
      Tier II: 20,000 or greater BPD
      1-12,999 BPD
      13,000 or greater BPD
    """
    if not tier_text:
        return ("", "")

    t = clean(tier_text).replace("–", "-").replace("—", "-")
    low = t.lower()

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", low)
    if m:
        return (int(m.group(1).replace(",", "")), int(m.group(2).replace(",", "")))

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", low)
    if m:
        return (int(m.group(1).replace(",", "")), None)

    return ("", "")


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


def pad_row(row, target_len):
    if len(row) < target_len:
        return row + [""] * (target_len - len(row))
    return row


def get_val(row, idx):
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


def find_header_row(rows, required_terms):
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
    return None


def split_origins(origin_cell):
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


# -----------------------------
# PAGE 11 extractor
# -----------------------------
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
                    min_bpd, max_bpd = parse_bpd_tier(mv_val)
                elif tier_val and "bpd" in tier_val.lower():
                    min_bpd, max_bpd = parse_bpd_tier(tier_val)

                derived_tier = extract_rate_tier_label(tier_val)
                if not derived_tier:
                    derived_tier = extract_rate_tier_label(mv_val)

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
                    min_bpd, max_bpd = parse_bpd_tier(source_for_bpd)

                derived_tier = extract_rate_tier_label(tier_val)
                if not derived_tier:
                    derived_tier = extract_rate_tier_label(vol_val)

                origins = split_origins(origin_raw if origin_raw else origin_clean)

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


# -----------------------------
# Example main
# -----------------------------
if __name__ == "__main__":
    tariff_data = []

    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        page11_records = extract_page11(pdf)
        tariff_data.extend(page11_records)

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:
        output_file = "extracted_page21_contract_volume_incentive_rates.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nPage 11 data successfully exported to {output_file}")
    else:
        print("\nFailed to extract Page 11 table data.")