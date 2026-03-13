import pdfplumber
import pandas as pd
import re
from datetime import datetime


# ---------------------------------------------------------
# Precompiled regex patterns
# ---------------------------------------------------------
RE_PIPELINE = re.compile(r"(.*Pipeline.*LLC)", re.IGNORECASE)
RE_EFFECTIVE = re.compile(r"EFFECTIVE:\s*(.*)", re.IGNORECASE)
RE_EXPIRE = re.compile(
    r"expire[s]?\s+on.*?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)
RE_SPACE = re.compile(r"\s+")
RE_RANGE_BPD = re.compile(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", re.IGNORECASE)
RE_GREATER_BPD_1 = re.compile(r"(\d{1,3}(?:,\d{3})*)\s*bpd\s*or\s*greater", re.IGNORECASE)
RE_GREATER_BPD_2 = re.compile(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", re.IGNORECASE)
RE_SINGLE_BPD = re.compile(r"(\d{1,3}(?:,\d{3})*)\s*bpd", re.IGNORECASE)
RE_TIER = re.compile(r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b", re.IGNORECASE)
RE_DECIMAL_RATE = re.compile(r"\d+\.\d{2}")
RE_RATE_FULL = re.compile(r"\d+\.\d+")
RE_RATE_OR_NA = re.compile(r"\d+(?:\.\d+)?")
RE_TERM = re.compile(
    r"Initial\s+Term\s+of\s+[A-Za-z]+\s*\((\d+)\)\s*year[s]?(?:\s+and\s+[A-Za-z]+\s*\((\d+)\)\s*month[s]?)?",
    re.IGNORECASE,
)
RE_SPLIT_PAGE5 = re.compile(r"(?=[A-Z][a-zA-Z]+\s+Located in)")
RE_SPLIT_STATE_CODE = re.compile(r"(?<=,\s[A-Z]{2})\s+(?=[A-Z])")


# ---------------------------------------------------------
# Shared constants
# ---------------------------------------------------------
DEFAULT_BPD_RANGE = [{"MinBPD": "", "MaxBPD": ""}]
BASE_RECORD_DEFAULTS = {
    "LiquidTariffNumber": "",
    "TariffStatus": "Effective",
    "TermYear": "",
    "AcreageDedicationMinAcres": "",
    "AcreageDedicationMaxAcres": "",
    "SurchargeCentsPerBbl": "",
    "LiquidFuelType": "Crude",
}


# ---------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------
def clean(text):
    if not text:
        return ""
    return RE_SPACE.sub(" ", str(text).replace("\n", " ")).strip()


def normalize_dashes(text):
    if not text:
        return ""
    return str(text).replace("–", "-").replace("—", "-")


def safe_extract_text(page):
    return page.extract_text() or ""


def safe_extract_tables(page):
    return page.extract_tables() or []


def clean_row(row):
    return [clean(cell) for cell in row]


def clean_table(table, drop_empty_rows=False):
    cleaned = []
    for row in table:
        row_clean = clean_row(row)
        if drop_empty_rows and not any(row_clean):
            continue
        cleaned.append(row_clean)
    return cleaned


def pad_row(row, target_len):
    if len(row) < target_len:
        return row + [""] * (target_len - len(row))
    return row


def get_val(row, idx):
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


get_col_value = get_val


def find_header_row(rows, required_terms):
    for i, row in enumerate(rows):
        row_text = " ".join(c.lower() for c in row if c)
        if all(term in row_text for term in required_terms):
            return i
    return None


def find_col_index(header_low, include_terms, exclude_terms=None):
    exclude_terms = exclude_terms or []
    for i, h in enumerate(header_low):
        if all(term in h for term in include_terms) and not any(term in h for term in exclude_terms):
            return i
    return None


def extract_page_context(pdf, page, tariff_rate_type=""):
    text = safe_extract_text(page)

    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)
    bpd_ranges = extract_bpd_ranges(text) or DEFAULT_BPD_RANGE
    rate_tier = extract_rate_tiers(text)

    return {
        "text": text,
        "tariff_rate_type": tariff_rate_type,
        "expiry_date_value": expiry_date_value,
        "bpd_ranges": bpd_ranges,
        "rate_tier": rate_tier,
    }


def build_base_record(pipeline_name, effective_date, expiry_date, rate_type, rate_tier="", term_year=""):
    record = {
        "Pipeline Name": pipeline_name,
        "Effective Date": effective_date,
        "End Date": expiry_date,
        "RateTier": rate_tier,
        "RateType": rate_type,
        **BASE_RECORD_DEFAULTS,
    }
    record["TermYear"] = term_year
    return record


def append_record(records, base_record, origin, destination, rate, min_bpd="", max_bpd=""):
    records.append(
        {
            **base_record,
            "PointfOrigin": clean(origin),
            "PointOfDestination": clean(destination),
            "MinBPD": "" if min_bpd is None else min_bpd,
            "MaxBPD": "" if max_bpd is None else max_bpd,
            "LiquidRateCentsPerBbl": rate,
        }
    )


def append_records_for_bpd(records, base_record, origin, destination, rate, bpd_ranges):
    for bpd in bpd_ranges:
        append_record(
            records,
            base_record,
            origin,
            destination,
            rate,
            bpd.get("MinBPD", ""),
            bpd.get("MaxBPD", ""),
        )


def carry_forward(current, previous):
    return current if current else previous


# ---------------------------------------------------------
# Metadata and parsing helpers
# ---------------------------------------------------------
def extract_pipeline_metadata(pdf):
    pipeline_name = ""
    effective_date = ""

    page1_text = safe_extract_text(pdf.pages[0])

    match_pipeline = RE_PIPELINE.search(page1_text)
    if match_pipeline:
        pipeline_name = match_pipeline.group(1).strip()

    match_effective = RE_EFFECTIVE.search(page1_text)
    if match_effective:
        effective_date = match_effective.group(1).strip()
        dt_obj = datetime.strptime(effective_date, "%B %d, %Y")
        effective_date = dt_obj.strftime("%d-%m-%Y")

    return pipeline_name, effective_date


def extract_tariff_rate_type(text):
    try:
        lines = str(text).replace("\r", "").split("\n")
        tariff_lines = []

        for i, line in enumerate(lines):
            clean_line = line.strip()
            if "RATES" in clean_line:
                tariff_lines.append(clean_line)
                for j in range(1, 3):
                    if i + j < len(lines):
                        next_line = lines[i + j].strip()
                        if next_line and next_line.isupper():
                            tariff_lines.append(next_line)
                        else:
                            break
                break

        if tariff_lines:
            return " ".join(tariff_lines).strip()
        return None

    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""


def extract_expiry_date(text):
    try:
        expiry_match = RE_EXPIRE.search(str(text))
        if not expiry_match:
            return ""

        raw_date = expiry_match.group(1).strip()
        dt_obj = datetime.strptime(raw_date, "%B %d, %Y")
        return dt_obj.strftime("%d-%m-%Y")

    except Exception as e:
        print(f"Error extracting expiry date: {e}")
        return ""


def parse_volume_to_minmax(volume_text: str):
    if not volume_text:
        return ("", "")

    t_low = normalize_dashes(volume_text).lower()

    m = RE_RANGE_BPD.search(t_low)
    if m:
        return (int(m.group(1).replace(",", "")), int(m.group(2).replace(",", "")))

    m = RE_GREATER_BPD_1.search(t_low) or RE_GREATER_BPD_2.search(t_low)
    if m:
        return (int(m.group(1).replace(",", "")), None)

    m = RE_SINGLE_BPD.search(t_low)
    if m:
        min_bpd = int(m.group(1).replace(",", ""))
        return (min_bpd, min_bpd)

    return ("", "")


def extract_bpd_ranges(text):
    try:
        results = []
        normalized = normalize_dashes(text)

        for min_bpd, max_bpd in RE_RANGE_BPD.findall(normalized):
            results.append({
                "MinBPD": int(min_bpd.replace(",", "")),
                "MaxBPD": int(max_bpd.replace(",", "")),
            })

        greater_matches = RE_GREATER_BPD_1.findall(normalized) + RE_GREATER_BPD_2.findall(normalized)
        for min_bpd in greater_matches:
            results.append({
                "MinBPD": int(min_bpd.replace(",", "")),
                "MaxBPD": None,
            })

        return results if results else []

    except Exception as e:
        print(f"Error extracting BPD ranges: {e}")
        return []


def parse_bpd_header_to_minmax(bpd_header: str):
    if not bpd_header:
        return ("", "")

    low = normalize_dashes(clean(bpd_header)).lower()

    m = RE_RANGE_BPD.search(low)
    if m:
        return (int(m.group(1).replace(",", "")), int(m.group(2).replace(",", "")))

    m = RE_GREATER_BPD_2.search(low)
    if m:
        return (int(m.group(1).replace(",", "")), None)

    return ("", "")


def extract_rate_tiers(text):
    try:
        matches = RE_TIER.findall(str(text))
        if not matches:
            return None

        cleaned_tiers = list(dict.fromkeys(f"Rate Tier {tier.strip()}" for tier in matches))
        return cleaned_tiers if cleaned_tiers else None

    except Exception as e:
        print(f"Error extracting rate tiers: {e}")
        return ""


def extract_rate_tier_label(text):
    m = RE_TIER.search(clean(text))
    if m:
        return f"Rate Tier {m.group(1).upper()}"
    return ""


def is_rate(value):
    if not value:
        return False
    return bool(RE_RATE_FULL.fullmatch(str(value).strip()))


def is_rate_or_na(value: str) -> bool:
    if value is None:
        return False
    v = str(value).strip()
    if not v:
        return False
    if v.lower() in ("n/a", "na"):
        return True
    return bool(RE_RATE_OR_NA.fullmatch(v))


# ---------------------------------------------------------
# Page-specific extraction functions
# Note: extraction logic retained; only shared helpers/redundancy reduced.
# ---------------------------------------------------------
def extract_rates_table_for_Page3_4_12_13_14_15(pdf, start_Page_number, end_page_number):
    print(f"--- Extracting Rates Table from {pdf} ---\n")

    unpivoted_data = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for i, page in enumerate(pdf.pages[start_Page_number:end_page_number]):
        ctx = extract_page_context(pdf, page, tariff_rate_type)
        text = ctx["text"]
        if not text:
            continue

        tariff_rate_type = ctx["tariff_rate_type"]
        expiry_date_value = ctx["expiry_date_value"]
        bpd_ranges = ctx["bpd_ranges"]
        rate_tier = ctx["rate_tier"]

        if tariff_rate_type in text or "cents per Barrel" in text or "All rates are unchanged." in text:
            print(f"Found target table on Page {i + 1}.")

            tables = safe_extract_tables(page)
            if tables:
                print(f"Found {len(tables)} table(s) on the page.")

                for table in tables:
                    if not table:
                        continue

                    cleaned_table = clean_table(table, drop_empty_rows=True)
                    if len(cleaned_table) <= 1:
                        continue

                    dest_headers = cleaned_table[1]
                    base_record = build_base_record(
                        pipeline_name,
                        effective_date,
                        expiry_date_value,
                        tariff_rate_type,
                        rate_tier,
                    )

                    for row in cleaned_table[2:]:
                        if len(row) < 2:
                            continue

                        origin = clean(row[1])
                        if not origin or origin.lower() == "none":
                            continue

                        for col_idx in range(2, len(row)):
                            if col_idx >= len(dest_headers):
                                break

                            destination = clean(dest_headers[col_idx]) or f"Unknown_Dest_{col_idx}"
                            rate = clean(row[col_idx])
                            append_records_for_bpd(unpivoted_data, base_record, origin, destination, rate, bpd_ranges)

    return unpivoted_data


def extract_page_5(pdf):
    print("\n--- Extracting Page 5 (Final Version) ---\n")

    records = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for _, page in enumerate(pdf.pages[4:5]):
        ctx = extract_page_context(pdf, page, tariff_rate_type)
        text = ctx["text"]
        tariff_rate_type = ctx["tariff_rate_type"]
        expiry_date_value = ctx["expiry_date_value"]
        bpd_ranges = ctx["bpd_ranges"]

        print(tariff_rate_type)

        if not text or "All rates are unchanged." not in text:
            print("Keyword not found on Page 5.")
            return None

        tables = safe_extract_tables(page)
        if not tables:
            print("No tables found on Page 5.")
            return None

        base_record = build_base_record(
            pipeline_name,
            effective_date,
            expiry_date_value,
            tariff_rate_type,
        )

        for table in tables:
            if not table:
                continue

            cleaned = clean_table(table)

            header_index = None
            for i, row in enumerate(cleaned):
                row_text = " ".join(row).lower()
                if "origin" in row_text and "destination" in row_text:
                    header_index = i
                    break

            if header_index is None:
                continue

            header = cleaned[header_index]
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

            for row in cleaned[header_index + 1:]:
                if len(row) <= max(tier_cols):
                    continue

                origin = row[origin_col].strip()
                dest = row[dest_col].strip()

                previous_origin = carry_forward(origin, previous_origin)
                previous_dest = carry_forward(dest, previous_dest)
                origin = previous_origin
                dest = previous_dest

                if not origin or not dest:
                    continue

                origin = clean(origin)
                dest = clean(dest)
                origin_parts = [o.strip() for o in RE_SPLIT_PAGE5.split(origin) if o.strip()]

                for col_index, tier_name in tier_cols.items():
                    if col_index >= len(row):
                        continue

                    rate = row[col_index].strip()
                    if not rate:
                        continue

                    rate_match = RE_DECIMAL_RATE.search(rate)
                    if rate_match:
                        valid_rate = rate_match.group()
                    elif rate.lower() == "n/a":
                        valid_rate = "N/A"
                    else:
                        continue

                    local_base = {**base_record, "RateTier": tier_name}
                    for single_origin in origin_parts:
                        append_records_for_bpd(records, local_base, single_origin, dest, valid_rate, bpd_ranges)

            break

    if records:
        return records

    print("No valid records extracted.")
    return None


def extract_page6(pdf):
    unpivoted_data = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for i, page in enumerate(pdf.pages[5:6]):
        ctx = extract_page_context(pdf, page, tariff_rate_type)
        text = ctx["text"]
        if not text:
            continue

        tariff_rate_type = ctx["tariff_rate_type"]
        expiry_date_value = ctx["expiry_date_value"]
        bpd_ranges = ctx["bpd_ranges"]
        rate_tier = ctx["rate_tier"]

        if tariff_rate_type in text or "cents per Barrel" in text or "All rates are unchanged." in text:
            print(f"Found target table on Page {i + 1}.")

        base_record = build_base_record(
            pipeline_name,
            effective_date,
            expiry_date_value,
            tariff_rate_type,
            rate_tier,
        )

        tables = safe_extract_tables(page)
        for table in tables:
            if not table or len(table) < 2:
                continue

            header = [clean(col).lower() if col else "" for col in table[0]]
            origin_idx = None
            destination_idx = None
            rate_idx = None

            for idx, col in enumerate(header):
                if "origin" in col:
                    origin_idx = idx
                elif "destination" in col:
                    destination_idx = idx
                elif "rate" in col:
                    rate_idx = idx

            if origin_idx is None or destination_idx is None or rate_idx is None:
                continue

            for row in table[1:]:
                row = clean_row(row)
                origin = row[origin_idx]
                destination = row[destination_idx]
                rate = row[rate_idx]

                if origin and destination and is_rate(rate):
                    append_records_for_bpd(unpivoted_data, base_record, origin, destination, rate, bpd_ranges)

    return unpivoted_data


def extract_page7(pdf):
    unpivoted_data = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for i, page in enumerate(pdf.pages[6:7]):
        ctx = extract_page_context(pdf, page, tariff_rate_type)
        text = ctx["text"]
        if not text:
            continue

        tariff_rate_type = ctx["tariff_rate_type"]
        expiry_date_value = ctx["expiry_date_value"]
        bpd_ranges = ctx["bpd_ranges"]
        rate_tier = ctx["rate_tier"]

        if tariff_rate_type in text or "cents per Barrel" in text or "All rates are unchanged." in text:
            print(f"Found target table on Page {i + 1}.")

        base_record = build_base_record(
            pipeline_name,
            effective_date,
            expiry_date_value,
            tariff_rate_type,
            rate_tier,
        )

        tables = safe_extract_tables(page)
        for table in tables:
            for row in table:
                row = [clean(cell) for cell in row if cell]
                if len(row) < 2:
                    continue

                origin = row[0]
                rate_candidates = [cell for cell in row if is_rate(cell)]
                if not rate_candidates:
                    continue

                rate = rate_candidates[-1]
                destination = "Deeprock North Terminal in Cushing, OK"
                append_records_for_bpd(unpivoted_data, base_record, origin, destination, rate, bpd_ranges)

    return unpivoted_data


def extract_page8(pdf):
    """
    Extracts Page 8: CONTRACT RATES
    Handles multiple tables on the page (Refinery OS, Carpenter OS 2020, Guernsey->Sterling OS, Carpenter OS 2024)
    """
    records = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[7]
    text = safe_extract_text(page)

    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)
    rate_tier = extract_rate_tiers(text)
    tables = safe_extract_tables(page)
    if not tables:
        return records

    base_record = build_base_record(
        pipeline_name,
        effective_date,
        expiry_date_value,
        tariff_rate_type,
        rate_tier,
    )

    for table in tables:
        if not table or len(table) < 2:
            continue

        cleaned = clean_table(table)

        header_idx = None
        for i, row in enumerate(cleaned):
            row_join = " ".join(c.lower() for c in row if c)
            if "origin" in row_join and (
                "destination" in row_join or "minimum volume" in row_join or "production dedication volume" in row_join
            ):
                header_idx = i
                break
        if header_idx is None:
            continue

        header = cleaned[header_idx]
        header_low = [h.lower() for h in header]

        origin_col = None
        vol_col = None
        for idx, h in enumerate(header_low):
            if "origin" in h:
                origin_col = idx
            elif "minimum volume" in h or "production dedication volume" in h:
                vol_col = idx

        if origin_col is None:
            continue

        dest_text = ""
        dest_col_candidates = [idx for idx, h in enumerate(header) if "Located in" in h]
        if dest_col_candidates:
            dest_col = dest_col_candidates[0]
            dest_text = header[dest_col]
        else:
            dest_col = None

        prev_origin = ""
        prev_vol = ""

        for row in cleaned[header_idx + 1:]:
            row = pad_row(row, len(header))

            origin_val = row[origin_col].strip() if origin_col is not None else ""
            prev_origin = carry_forward(origin_val, prev_origin)
            origin_val = prev_origin

            vol_val = row[vol_col].strip() if vol_col is not None else ""
            prev_vol = carry_forward(vol_val, prev_vol)
            vol_val = prev_vol

            if not origin_val:
                continue

            destination_val = dest_text
            for idx, h in enumerate(header_low):
                if "destination" in h:
                    body_dest = row[idx].strip()
                    if body_dest:
                        destination_val = body_dest
                    break

            for col_i in range(len(header)):
                if col_i == origin_col or (vol_col is not None and col_i == vol_col):
                    continue

                cell = row[col_i].strip()
                if not is_rate(cell):
                    continue

                rate_val = cell.upper() if cell.lower() in ("n/a", "na") else cell
                min_bpd, max_bpd = parse_volume_to_minmax(vol_val)
                append_record(records, base_record, origin_val, destination_val, rate_val, min_bpd, max_bpd)

    return records


def split_origins(origin_cell):
    if not origin_cell:
        return []

    origin_cell = origin_cell.strip()
    parts_by_comma = origin_cell.split(',')
    last_part = parts_by_comma[-1].strip() if parts_by_comma else ""

    if len(last_part) > 3:
        potential_list = origin_cell.split("\n")
        return [clean(part) for part in potential_list if clean(part)]

    return [origin_cell.replace("\n", " ").strip()]


def extract_page9(pdf):
    """
    PAGE 9 (index 8): CONTRACT VOLUME INCENTIVE RATES
    """
    results = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[8]
    text = safe_extract_text(page)

    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)
    rate_tier = extract_rate_tiers(text)

    tables = safe_extract_tables(page)
    if not tables:
        return results

    base_record = build_base_record(
        pipeline_name,
        effective_date,
        expiry_date_value,
        tariff_rate_type,
        rate_tier,
    )

    for table in tables:
        if not table or len(table) < 2:
            continue

        header = [clean(col).lower() if col else "" for col in table[0]]
        origin_idx = None
        destination_idx = None
        rate_indexes = []

        for idx, col in enumerate(header):
            if "origin" in col:
                origin_idx = idx
            elif "destination" in col:
                destination_idx = idx
            elif "rate" in col:
                rate_indexes.append(idx)

        if origin_idx is None or destination_idx is None:
            continue

        previous_destination = ""
        previous_origin = []

        for row in table[1:]:
            row = [cell if cell else "" for cell in row]

            raw_dest = row[destination_idx] if destination_idx < len(row) else ""
            destination = clean(raw_dest)
            previous_destination = carry_forward(destination, previous_destination)
            destination = previous_destination

            origin_cell = row[origin_idx] if origin_idx < len(row) else ""
            current_origin_clean = clean(origin_cell)

            if current_origin_clean:
                origins = split_origins(origin_cell)
                previous_origin = origins
            else:
                origins = previous_origin

            if not origins:
                continue

            for origin in origins:
                for rate_col in rate_indexes:
                    if rate_col < len(row):
                        rate_value = clean(row[rate_col])
                        if rate_value:
                            append_record(results, base_record, origin, destination, rate_value, "", "")

    return results


def extract_term_year_from_page10_text(text):
    term_map = {}
    pattern = RE_TERM.findall(str(text))

    for idx, match in enumerate(pattern):
        years = int(match[0]) if match[0] else 0
        months = int(match[1]) if match[1] else 0
        term_value = years + (months / 12)
        term_map[idx] = str(int(term_value)) if term_value.is_integer() else str(round(term_value, 2))

    return term_map


def extract_page10(pdf):
    """
    Page 10 (0-based index 9): CONTRACT VOLUME INCENTIVE RATES
    """
    records = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[9]
    text = safe_extract_text(page)

    tariff_rate_type = extract_tariff_rate_type(text)
    expiry_date_value = extract_expiry_date(text)

    tables = safe_extract_tables(page)
    if not tables:
        return records

    term_map = extract_term_year_from_page10_text(text)
    rate_tiers = extract_rate_tiers(text)

    for t_index, table in enumerate(tables):
        if not table or len(table) < 3:
            continue

        cleaned = clean_table(table)
        header_bpd = cleaned[0]

        origin_idx = None
        dest_idx = None
        for idx, col in enumerate(header_bpd):
            cl = col.lower()
            if "origin" in cl:
                origin_idx = idx
            elif "destination" in cl:
                dest_idx = idx

        if origin_idx is None or dest_idx is None:
            continue

        col_maps = []
        for col_i in range(len(header_bpd)):
            if col_i in (origin_idx, dest_idx):
                continue

            bpd_label = header_bpd[col_i]
            if not bpd_label:
                for j in range(col_i - 1, -1, -1):
                    if header_bpd[j]:
                        bpd_label = header_bpd[j]
                        break

            if not bpd_label:
                continue

            min_bpd, max_bpd = parse_bpd_header_to_minmax(bpd_label)
            col_maps.append({
                "col_i": col_i,
                "min_bpd": "" if min_bpd is None else min_bpd,
                "max_bpd": "" if max_bpd is None else max_bpd,
            })

        if not col_maps:
            continue

        previous_origin = ""
        previous_dest = ""
        base_record = build_base_record(
            pipeline_name,
            effective_date,
            expiry_date_value,
            tariff_rate_type,
            rate_tiers,
            term_map.get(t_index, ""),
        )

        for row in cleaned[2:]:
            row = pad_row(row, len(header_bpd))

            origin = row[origin_idx].strip()
            dest = row[dest_idx].strip()
            previous_origin = carry_forward(origin, previous_origin)
            previous_dest = carry_forward(dest, previous_dest)
            origin = clean(previous_origin)
            dest = clean(previous_dest)

            if not origin or not dest:
                continue

            for mapping in col_maps:
                rate_val = row[mapping["col_i"]].strip() if mapping["col_i"] < len(row) else ""
                if not is_rate_or_na(rate_val):
                    continue

                rate_val = "N/A" if rate_val.lower() in ("n/a", "na") else str(rate_val)
                append_record(
                    records,
                    base_record,
                    origin,
                    dest,
                    rate_val,
                    mapping["min_bpd"],
                    mapping["max_bpd"],
                )

    return records


def split_origins_val(origin_cell):
    if not origin_cell:
        return []

    origin_cell = clean(str(origin_cell).replace("\n", " "))
    parts = RE_SPLIT_STATE_CODE.split(origin_cell)
    parts = [clean(part) for part in parts if clean(part)]
    return parts if parts else [clean(origin_cell)]


def extract_page11(pdf):
    """
    Page 11 (0-based index 10): CONTRACT VOLUME INCENTIVE RATES
    """
    records = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[10]
    text = safe_extract_text(page)

    tariff_rate_type = extract_tariff_rate_type(text)
    expiry_date_value = extract_expiry_date(text)

    tables = safe_extract_tables(page)
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

        base_record = build_base_record(
            pipeline_name,
            effective_date,
            expiry_date_value,
            tariff_rate_type,
            rate_tier,
            term_year,
        )
        append_record(records, base_record, origin, dest, rate_str, min_bpd, max_bpd)

    for table in tables:
        if not table or len(table) < 2:
            continue

        raw_rows = [["" if c is None else str(c) for c in row] for row in table]
        cleaned = clean_table(table)
        max_len = max(len(r) for r in cleaned)
        cleaned = [pad_row(r, max_len) for r in cleaned]
        raw_rows = [pad_row(r, max_len) for r in raw_rows]

        flat_text = " ".join(" ".join(r).lower() for r in cleaned)

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

                prev_origin = carry_forward(origin, prev_origin)
                prev_dest = carry_forward(dest, prev_dest)
                origin = prev_origin
                dest = prev_dest

                if not origin or not dest:
                    continue

                min_bpd, max_bpd = ("", "")
                if mv_val and "bpd" in mv_val.lower():
                    min_bpd, max_bpd = parse_volume_to_minmax(mv_val)
                elif tier_val and "bpd" in tier_val.lower():
                    min_bpd, max_bpd = parse_volume_to_minmax(tier_val)

                derived_tier = extract_rate_tier_label(tier_val) or extract_rate_tier_label(mv_val)

                a_incent = get_val(row, ship_a_incent_col)
                a_extra = get_val(row, ship_a_extra_col)
                b_incent = get_val(row, ship_b_incent_col)
                b_extra = get_val(row, ship_b_extra_col)

                if a_incent:
                    add_record(origin, dest, a_incent, derived_tier, min_bpd, max_bpd)
                if a_extra:
                    add_record(origin, dest, a_extra, derived_tier, min_bpd, max_bpd)
                if b_incent:
                    add_record(origin, dest, b_incent, derived_tier, min_bpd, max_bpd)
                if b_extra:
                    add_record(origin, dest, b_extra, derived_tier, min_bpd, max_bpd)

            continue

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
                for idx in range(len(header_low)):
                    sample_vals = [get_val(r, idx) for r in cleaned[header_idx + 1:]]
                    if any(is_rate_or_na(v) for v in sample_vals):
                        rate_col = idx
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

                prev_dest = carry_forward(dest, prev_dest)
                dest = prev_dest

                if not origin_clean or not dest or not rate_val:
                    continue

                min_bpd, max_bpd = ("", "")
                source_for_bpd = vol_val if vol_val and "bpd" in vol_val.lower() else tier_val
                if source_for_bpd and "bpd" in source_for_bpd.lower():
                    min_bpd, max_bpd = parse_volume_to_minmax(source_for_bpd)

                derived_tier = extract_rate_tier_label(tier_val) or extract_rate_tier_label(vol_val)
                origins = split_origins_val(origin_raw if origin_raw else origin_clean)

                for single_origin in origins:
                    add_record(single_origin, dest, rate_val, derived_tier, min_bpd, max_bpd)

            continue

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
                for idx in range(len(header_low)):
                    sample_vals = [get_val(r, idx) for r in cleaned[header_idx + 1:]]
                    if any(is_rate_or_na(v) for v in sample_vals):
                        rate_col = idx
                        break

            prev_origin = ""
            prev_dest = ""

            for row in cleaned[header_idx + 1:]:
                row = pad_row(row, len(header))

                origin = get_val(row, origin_col)
                dest = get_val(row, dest_col)
                rate_val = get_val(row, rate_col)

                prev_origin = carry_forward(origin, prev_origin)
                prev_dest = carry_forward(dest, prev_dest)
                origin = prev_origin
                dest = prev_dest

                if not origin or not dest or not rate_val:
                    continue

                add_record(origin, dest, rate_val, "", "", "")

            continue

    return records


if __name__ == "__main__":
    tariff_data = []

    file_name = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"

    with pdfplumber.open(file_name) as pdf:
        df_rates1 = extract_rates_table_for_Page3_4_12_13_14_15(pdf, 2, 4)
        tariff_data.extend(df_rates1 or [])

        df_rates5 = extract_page_5(pdf)
        tariff_data.extend(df_rates5 or [])

        df_rates6 = extract_page6(pdf)
        tariff_data.extend(df_rates6 or [])

        df_rates7 = extract_page7(pdf)
        tariff_data.extend(df_rates7 or [])

        df_rates8 = extract_page8(pdf)
        tariff_data.extend(df_rates8 or [])

        df_rates9 = extract_page9(pdf)
        tariff_data.extend(df_rates9 or [])

        df_rates10 = extract_page10(pdf)
        tariff_data.extend(df_rates10 or [])

        df_rates11 = extract_page11(pdf)
        tariff_data.extend(df_rates11 or [])

        df_rates12 = extract_rates_table_for_Page3_4_12_13_14_15(pdf, 11, 15)
        tariff_data.extend(df_rates12 or [])

    final_data = pd.DataFrame(tariff_data)

    if final_data is not None and len(final_data) > 0:
        output_file = "sample_tariff_data_v5.csv"
        final_data.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {output_file}")
    else:
        print("\nFailed to extract table data.")
