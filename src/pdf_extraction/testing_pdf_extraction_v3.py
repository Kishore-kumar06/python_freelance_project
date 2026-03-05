import pdfplumber
import pandas as pd
import re
from datetime import datetime

# -----------------------------
# Helpers (same style)
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
    For page 11 table 1:
      Tier I: 5,000 – 19,999 BPD
      Tier II: 20,000 or greater BPD

    For page 11 table 2:
      Tier I: 1-12,999 BPD
      Tier II: 13,000 or greater BPD
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

def looks_like_tier_cell(val: str) -> bool:
    v = clean(val).lower()
    return v.startswith("tier") or bool(re.search(r"\bbpd\b", v))


# -----------------------------
# PAGE 11 extractor
# -----------------------------
def extract_page11(pdf):
    """
    Page 11 (0-based index 10): CONTRACT VOLUME INCENTIVE RATES (Volume Incentive Open Season - March 7, 2025)

    Page 11 contains 3 logical tables/blocks:
      A) Main incentive table with:
          Tier (I/II with BPD ranges)
          Origin, Destination
          Shipper A Incentive Rate / Extra Barrel Rate
          Shipper B Incentive Rate / Extra Barrel Rate
         -> Output: unpivoted: one record per (shipper + rate_type) per tier.

      B) Secondary Origin Barrel Rate table:
          Tier I 1-12,999 BPD -> 251.15
          Tier II 13,000 or greater BPD -> 234.59
         with Origin Guernsey (WY), Destination Sterling (CO) + Various Cushing Destinations (OK)
         -> Output: RateTier="Secondary Origin Barrel Rate", plus MinBPD/MaxBPD.

      C) Buckingham Barrel Rate table:
          Origin Buckingham (CO), Destination Various Cushing Destinations (OK), Rate 244.01
         -> Output: RateTier="Buckingham Barrel Rate" (Min/Max blank).
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

    # Helper to emit a record
    def add_record(origin, dest, rate, rate_tier, min_bpd="", max_bpd="", term_year=""):
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

    # Iterate tables found on the page
    for table in tables:
        if not table or len(table) < 2:
            continue

        cleaned = []
        for row in table:
            cleaned.append([clean(c) for c in row])

        # Identify which table this is using header keywords
        header_join = " ".join([c.lower() for c in cleaned[0] if c])

        # ---------------------------------------------------------
        # A) Main Shipper A/B table (has "Shipper A", "Shipper B")
        # ---------------------------------------------------------
        flat_text = " ".join([(" ".join(r)).lower() for r in cleaned[:2]])
        if "shipper a" in flat_text and "shipper b" in flat_text:
            # Find header row containing "Origin" and "Destination"
            header_idx = None
            for i, row in enumerate(cleaned):
                row_join = " ".join([c.lower() for c in row if c])
                if "origin" in row_join and "destination" in row_join and "shipper a" in row_join:
                    header_idx = i
                    break
            if header_idx is None:
                # sometimes header spans 2 lines; take first row that has origin/destination
                for i, row in enumerate(cleaned):
                    row_join = " ".join([c.lower() for c in row if c])
                    if "origin" in row_join and "destination" in row_join:
                        header_idx = i
                        break
            if header_idx is None:
                continue

            header = cleaned[header_idx]
            header_low = [h.lower() for h in header]

            # Find columns
            tier_col = None
            mv_col = None
            origin_col = None
            dest_col = None
            ship_a_incent_col = None
            ship_a_extra_col = None
            ship_b_incent_col = None
            ship_b_extra_col = None

            for i, h in enumerate(header_low):
                if h.startswith("tier"):
                    tier_col = i
                elif "minimum volume" in h or "commitment" in h:
                    mv_col = i
                elif "origin" in h:
                    origin_col = i
                elif "destination" in h:
                    dest_col = i
                elif "shipper a" in h and "incentive" in h:
                    ship_a_incent_col = i
                elif "shipper a" in h and "extra" in h:
                    ship_a_extra_col = i
                elif "shipper b" in h and "incentive" in h:
                    ship_b_incent_col = i
                elif "shipper b" in h and "extra" in h:
                    ship_b_extra_col = i

            # Fallback: if tier column missing, assume first col is tier
            if tier_col is None:
                tier_col = 0

            prev_origin = ""
            prev_dest = ""

            for row in cleaned[header_idx + 1:]:
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))

                tier_val = row[tier_col].strip() if tier_col is not None else ""
                mv_val = row[mv_col].strip() if mv_col is not None else ""
                origin = row[origin_col].strip() if origin_col is not None else ""
                dest = row[dest_col].strip() if dest_col is not None else ""

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

                # For min/max bpd, parse from the "Minimum Volume Commitment" cell if present,
                # else parse from tier cell if it contains BPD range.
                min_bpd, max_bpd = ("", "")
                if mv_val and "bpd" in mv_val.lower():
                    min_bpd, max_bpd = parse_bpd_tier(mv_val)
                elif tier_val and "bpd" in tier_val.lower():
                    min_bpd, max_bpd = parse_bpd_tier(tier_val)

                # Build a tier label: "Tier I" or "Tier II" + range if available
                tier_label = clean(tier_val) if tier_val else ""
                if tier_label and mv_val:
                    tier_label = f"{tier_label} ({clean(mv_val)})"
                elif mv_val and not tier_label:
                    tier_label = clean(mv_val)

                # Extract rates
                def safe_get(idx):
                    if idx is None or idx >= len(row):
                        return ""
                    return row[idx].strip()

                a_incent = safe_get(ship_a_incent_col)
                a_extra = safe_get(ship_a_extra_col)
                b_incent = safe_get(ship_b_incent_col)
                b_extra = safe_get(ship_b_extra_col)

                # Emit unpivoted records (4 per row if present)
                if a_incent:
                    add_record(origin, dest, a_incent, f"{tier_label} | Shipper A Incentive Rate", min_bpd, max_bpd)
                if a_extra:
                    add_record(origin, dest, a_extra, f"{tier_label} | Shipper A Extra Barrel Rate", min_bpd, max_bpd)
                if b_incent:
                    add_record(origin, dest, b_incent, f"{tier_label} | Shipper B Incentive Rate", min_bpd, max_bpd)
                if b_extra:
                    add_record(origin, dest, b_extra, f"{tier_label} | Shipper B Extra Barrel Rate", min_bpd, max_bpd)

            continue

        # ---------------------------------------------------------
        # B) Secondary Origin Barrel Rate table (has "Secondary Origin Barrel Rate")
        # ---------------------------------------------------------
        if "secondary origin" in " ".join([(" ".join(r)).lower() for r in cleaned[:2]]):
            # Find header row containing "Tier" and "Secondary Origin Barrel"
            header_idx = None
            for i, row in enumerate(cleaned):
                row_join = " ".join([c.lower() for c in row if c])
                if "tier" in row_join and "secondary origin" in row_join:
                    header_idx = i
                    break
            if header_idx is None:
                header_idx = 0

            header = cleaned[header_idx]
            header_low = [h.lower() for h in header]

            tier_col = None
            vol_col = None
            rate_col = None
            origin_col = None
            dest_col = None

            for i, h in enumerate(header_low):
                if h.startswith("tier"):
                    tier_col = i
                elif "volume" in h or "bpd" in h:
                    vol_col = i
                elif "secondary origin" in h and "rate" in h:
                    rate_col = i
                elif "origin" in h:
                    origin_col = i
                elif "destination" in h:
                    dest_col = i

            # If this extraction puts origin/dest outside header, we’ll forward-fill from body.
            prev_origin = ""
            prev_dest = ""

            for row in cleaned[header_idx + 1:]:
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))

                tier_val = row[tier_col].strip() if tier_col is not None else ""
                vol_val = row[vol_col].strip() if vol_col is not None else ""
                rate_val = row[rate_col].strip() if rate_col is not None else ""

                origin = row[origin_col].strip() if origin_col is not None else ""
                dest = row[dest_col].strip() if dest_col is not None else ""

                if origin:
                    prev_origin = origin
                else:
                    origin = prev_origin

                if dest:
                    prev_dest = dest
                else:
                    dest = prev_dest

                # If origin/dest still empty, try to infer from page text (stable for this table)
                if not origin:
                    origin = "Guernsey Located in Platte County, WY"
                if not dest:
                    dest = "Sterling Located in Logan County, CO / Various Cushing Destinations Located in Payne County, OK"

                if not rate_val:
                    continue

                # min/max from vol_val if possible, else from tier_val
                min_bpd, max_bpd = ("", "")
                if vol_val and "bpd" in vol_val.lower():
                    min_bpd, max_bpd = parse_bpd_tier(vol_val)
                elif tier_val and "bpd" in tier_val.lower():
                    min_bpd, max_bpd = parse_bpd_tier(tier_val)

                tier_label = clean(tier_val) if tier_val else ""
                if tier_label and vol_val:
                    tier_label = f"{tier_label} ({clean(vol_val)})"
                elif vol_val and not tier_label:
                    tier_label = clean(vol_val)

                add_record(origin, dest, rate_val, f"{tier_label} | Secondary Origin Barrel Rate", min_bpd, max_bpd)
            continue

        # ---------------------------------------------------------
        # C) Buckingham Barrel Rate table (simple 3-col table)
        # ---------------------------------------------------------
        if "buckingham barrel rate" in " ".join([(" ".join(r)).lower() for r in cleaned[:2]]):
            # Find header row with origin/destination
            header_idx = None
            for i, row in enumerate(cleaned):
                row_join = " ".join([c.lower() for c in row if c])
                if "origin" in row_join and "destination" in row_join:
                    header_idx = i
                    break
            if header_idx is None:
                header_idx = 0

            header = cleaned[header_idx]
            header_low = [h.lower() for h in header]

            origin_col = None
            dest_col = None
            rate_col = None
            for i, h in enumerate(header_low):
                if "origin" in h:
                    origin_col = i
                elif "destination" in h:
                    dest_col = i
                elif "rate" in h:
                    rate_col = i

            prev_origin = ""
            prev_dest = ""

            for row in cleaned[header_idx + 1:]:
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))

                origin = row[origin_col].strip() if origin_col is not None else ""
                dest = row[dest_col].strip() if dest_col is not None else ""
                rate_val = row[rate_col].strip() if rate_col is not None else ""

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

                add_record(origin, dest, rate_val, "Buckingham Barrel Rate")
            continue

        # If it didn't match any, ignore (keeps behavior consistent and safe)

    return records


# -----------------------------
# Example main (same pattern)
# -----------------------------
if __name__ == "__main__":
    tariff_data = []

    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        page11_records = extract_page11(pdf)
        tariff_data.extend(page11_records)

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:
        output_file = "extracted_page11_contract_volume_incentive_rates.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nPage 11 data successfully exported to {output_file}")
    else:
        print("\nFailed to extract Page 11 table data.")