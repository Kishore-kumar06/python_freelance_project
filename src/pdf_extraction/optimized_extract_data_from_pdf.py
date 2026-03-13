import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import pdfplumber


BLANK_BPD_RANGE = [{"MinBPD": "", "MaxBPD": ""}]
DEFAULT_OUTPUT_FILE = "sample_tariff_data_v4.csv"
DEFAULT_INPUT_FILE = r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF"


def clean(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\n", " ")).strip()


def normalize_dash(text: str) -> str:
    return text.replace("–", "-").replace("—", "-") if text else ""


def parse_full_month_date(date_text: str) -> str:
    dt_obj = datetime.strptime(date_text.strip(), "%B %d, %Y")
    return dt_obj.strftime("%d-%m-%Y")


def clean_row(row: Sequence[Any]) -> List[str]:
    return [clean(cell) for cell in row]


def clean_table_rows(table: Sequence[Sequence[Any]], skip_empty_rows: bool = False) -> List[List[str]]:
    cleaned_rows: List[List[str]] = []
    for row in table:
        cleaned_row = clean_row(row)
        if skip_empty_rows and not any(cleaned_row):
            continue
        cleaned_rows.append(cleaned_row)
    return cleaned_rows


def pad_row(row: List[str], target_len: int) -> List[str]:
    if len(row) < target_len:
        return row + [""] * (target_len - len(row))
    return row


def get_val(row: Sequence[str], idx: Optional[int]) -> str:
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


def get_col_value(row: Sequence[str], idx: Optional[int]) -> str:
    return get_val(row, idx)


def is_rate(value: Any) -> bool:
    if value is None:
        return False
    return bool(re.fullmatch(r"\d+\.\d+", str(value).strip()))


def is_rate_or_na(value: Any) -> bool:
    if value is None:
        return False
    v = str(value).strip()
    if not v:
        return False
    if v.lower() in ("n/a", "na"):
        return True
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", v))


def extract_pipeline_metadata(pdf) -> Tuple[str, str]:
    pipeline_name = ""
    effective_date = ""

    page1_text = pdf.pages[0].extract_text() or ""

    match_pipeline = re.search(r"(.*Pipeline.*LLC)", page1_text, re.IGNORECASE)
    if match_pipeline:
        pipeline_name = match_pipeline.group(1).strip()

    match_effective = re.search(r"EFFECTIVE:\s*(.*)", page1_text, re.IGNORECASE)
    if match_effective:
        try:
            effective_date = parse_full_month_date(match_effective.group(1).strip())
        except ValueError:
            effective_date = match_effective.group(1).strip()

    return pipeline_name, effective_date


def extract_tariff_rate_type(text: str) -> str:
    try:
        lines = (text or "").replace("\r", "").split("\n")
        tariff_lines: List[str] = []

        for i, line in enumerate(lines):
            clean_line = line.strip()
            if "RATES" not in clean_line:
                continue

            tariff_lines.append(clean_line)
            for j in range(1, 3):
                if i + j >= len(lines):
                    break
                next_line = lines[i + j].strip()
                if next_line and next_line.isupper():
                    tariff_lines.append(next_line)
                else:
                    break
            break

        return " ".join(tariff_lines).strip() if tariff_lines else ""
    except Exception as e:
        print(f"Error extracting tariff rate type: {e}")
        return ""


def extract_expiry_date(text: str) -> str:
    try:
        expiry_match = re.search(
            r"expire[s]?\s+on.*?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
            text or "",
            re.IGNORECASE,
        )
        if not expiry_match:
            return ""
        return parse_full_month_date(expiry_match.group(1).strip())
    except Exception as e:
        print(f"Error extracting expiry date: {e}")
        return ""


def parse_volume_to_minmax(volume_text: str) -> Tuple[Any, Any]:
    if not volume_text:
        return ("", "")

    t_low = normalize_dash(volume_text).lower()

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", t_low)
    if m:
        return (int(m.group(1).replace(",", "")), int(m.group(2).replace(",", "")))

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bpd\s*or\s*greater", t_low)
    if not m:
        m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", t_low)
    if m:
        return (int(m.group(1).replace(",", "")), None)

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bpd", t_low)
    if m:
        value = int(m.group(1).replace(",", ""))
        return (value, value)

    return ("", "")


def extract_bpd_ranges(text: str) -> List[Dict[str, Any]]:
    try:
        results: List[Dict[str, Any]] = []
        text = normalize_dash(text or "")

        range_pattern = r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*BPD"
        greater_pattern_1 = r"(\d{1,3}(?:,\d{3})*)\s*BPD\s*or\s*greater"
        greater_pattern_2 = r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*BPD"

        for min_bpd, max_bpd in re.findall(range_pattern, text, re.IGNORECASE):
            results.append({
                "MinBPD": int(min_bpd.replace(",", "")),
                "MaxBPD": int(max_bpd.replace(",", "")),
            })

        greater_matches = re.findall(greater_pattern_1, text, re.IGNORECASE)
        greater_matches += re.findall(greater_pattern_2, text, re.IGNORECASE)
        for min_bpd in greater_matches:
            results.append({"MinBPD": int(min_bpd.replace(",", "")), "MaxBPD": None})

        return results
    except Exception as e:
        print(f"Error extracting BPD ranges: {e}")
        return []


def parse_bpd_header_to_minmax(bpd_header: str) -> Tuple[Any, Any]:
    if not bpd_header:
        return ("", "")

    low = normalize_dash(clean(bpd_header)).lower()

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", low)
    if m:
        return (int(m.group(1).replace(",", "")), int(m.group(2).replace(",", "")))

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", low)
    if m:
        return (int(m.group(1).replace(",", "")), None)

    return ("", "")


def extract_rate_tiers(text: str):
    try:
        matches = re.findall(r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b", text or "", re.IGNORECASE)
        if not matches:
            return None
        cleaned_tiers = list(dict.fromkeys(f"Rate Tier {m.strip()}" for m in matches if m.strip()))
        return cleaned_tiers or None
    except Exception as e:
        print(f"Error extracting rate tiers: {e}")
        return ""


def extract_rate_tier_label(text: str) -> str:
    m = re.search(r"\b(?:Rate\s*Tier|Tier)\s*(\d+|[IVXLC]+)\b", clean(text), re.IGNORECASE)
    return f"Rate Tier {m.group(1).upper()}" if m else ""


def find_header_row(rows: Sequence[Sequence[str]], required_terms: Sequence[str]) -> Optional[int]:
    for i, row in enumerate(rows):
        row_text = " ".join(c.lower() for c in row if c)
        if all(term in row_text for term in required_terms):
            return i
    return None


def find_col_index(header_low: Sequence[str], include_terms: Sequence[str], exclude_terms: Optional[Sequence[str]] = None) -> Optional[int]:
    exclude_terms = exclude_terms or []
    for i, h in enumerate(header_low):
        if all(term in h for term in include_terms) and not any(term in h for term in exclude_terms):
            return i
    return None


def split_origins(origin_cell: str) -> List[str]:
    if not origin_cell:
        return []

    origin_cell = origin_cell.strip()
    parts_by_comma = origin_cell.split(',')
    last_part = parts_by_comma[-1].strip() if parts_by_comma else ""

    if len(last_part) > 3:
        return [clean(p) for p in origin_cell.split("\n") if clean(p)]

    return [origin_cell.replace("\n", " ").strip()]


def split_origins_val(origin_cell: str) -> List[str]:
    if not origin_cell:
        return []

    origin_cell = re.sub(r"\s+", " ", str(origin_cell).replace("\n", " ")).strip()
    parts = re.split(r'(?<=,\s[A-Z]{2})\s+(?=[A-Z])', origin_cell)
    parts = [clean(p) for p in parts if clean(p)]
    return parts or [clean(origin_cell)]


def extract_term_year_from_page10_text(text: str) -> Dict[int, str]:
    term_map: Dict[int, str] = {}
    pattern = re.findall(
        r"Initial\s+Term\s+of\s+[A-Za-z]+\s*\((\d+)\)\s*year[s]?(?:\s+and\s+[A-Za-z]+\s*\((\d+)\)\s*month[s]?)?",
        text or "",
        re.IGNORECASE,
    )

    for idx, match in enumerate(pattern):
        years = int(match[0]) if match[0] else 0
        months = int(match[1]) if match[1] else 0
        # Build output format
        if months > 0:
            term_map[idx] = f"{years}.{months}"
        else:
            term_map[idx] = str(years)

    return term_map


def get_page_context(text: str, current_tariff_rate_type: str = "") -> Dict[str, Any]:
    rate_type = extract_tariff_rate_type(text)
    tariff_rate_type = rate_type if rate_type else current_tariff_rate_type
    bpd_ranges = extract_bpd_ranges(text) or BLANK_BPD_RANGE.copy()
    return {
        "tariff_rate_type": tariff_rate_type,
        "expiry_date_value": extract_expiry_date(text),
        "bpd_ranges": bpd_ranges,
        "rate_tier": extract_rate_tiers(text),
    }


def build_record(
    pipeline_name: str,
    effective_date: str,
    origin: str,
    destination: str,
    rate: Any,
    expiry_date_value: str = "",
    rate_tier: Any = "",
    tariff_rate_type: str = "",
    term_year: str = "",
    min_bpd: Any = "",
    max_bpd: Any = "",
) -> Dict[str, Any]:
    return {
        "Pipeline Name": pipeline_name,
        "PointfOrigin": origin,
        "PointOfDestination": destination,
        "LiquidTariffNumber": "",
        "Effective Date": effective_date,
        "End Date": expiry_date_value,
        "TariffStatus": "Effective",
        "RateTier": rate_tier,
        "RateType": tariff_rate_type,
        "TermYear": term_year,
        "MinBPD": "" if min_bpd is None else min_bpd,
        "MaxBPD": "" if max_bpd is None else max_bpd,
        "AcreageDedicationMinAcres": "",
        "AcreageDedicationMaxAcres": "",
        "LiquidRateCentsPerBbl": rate,
        "SurchargeCentsPerBbl": "",
        "LiquidFuelType": "Crude",
    }


def extract_matrix_tables(pdf, start_Page_number: int, end_page_number: int):
    print(f"--- Extracting Rates Table from {pdf} ---\n")

    unpivoted_data: List[Dict[str, Any]] = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for i, page in enumerate(pdf.pages[start_Page_number:end_page_number]):
        text = page.extract_text()
        if not text:
            continue

        context = get_page_context(text, tariff_rate_type)
        tariff_rate_type = context["tariff_rate_type"]
        expiry_date_value = context["expiry_date_value"]
        bpd_ranges = context["bpd_ranges"]
        rate_tier = context["rate_tier"]

        if not (
            tariff_rate_type in text
            or "cents per Barrel" in text
            or "All rates are unchanged." in text
        ):
            continue

        print(f"Found target table on Page {i + 1}.")
        tables = page.extract_tables() or []
        if tables:
            print(f"Found {len(tables)} table(s) on the page.")

        for table in tables:
            if not table:
                continue

            cleaned_table = clean_table_rows(table, skip_empty_rows=True)
            if len(cleaned_table) <= 1:
                continue

            dest_headers = cleaned_table[1]
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

                    for bpd in bpd_ranges:
                        unpivoted_data.append(
                            build_record(
                                pipeline_name,
                                effective_date,
                                origin,
                                destination,
                                rate,
                                expiry_date_value,
                                rate_tier,
                                tariff_rate_type,
                                min_bpd=bpd["MinBPD"],
                                max_bpd=bpd["MaxBPD"],
                            )
                        )

    return unpivoted_data


def extract_page_5(pdf):
    print("\n--- Extracting Page 5 (Final Version) ---\n")

    records: List[Dict[str, Any]] = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for _, page in enumerate(pdf.pages[4:5]):
        text = page.extract_text() or ""
        context = get_page_context(text, tariff_rate_type)
        tariff_rate_type = context["tariff_rate_type"]
        expiry_date_value = context["expiry_date_value"]
        bpd_ranges = context["bpd_ranges"]

        print(tariff_rate_type)

        if "All rates are unchanged." not in text:
            print("Keyword not found on Page 5.")
            return None

        tables = page.extract_tables()
        if not tables:
            print("No tables found on Page 5.")
            return None

        for table in tables:
            if not table:
                continue

            cleaned = clean_table_rows(table)
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
            tier_cols: Dict[int, str] = {}

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
                if len(row) <= max(tier_cols.keys()):
                    continue

                origin = row[origin_col].strip()
                dest = row[dest_col].strip()

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

                origin = " ".join(origin.split())
                dest = " ".join(dest.split())
                origin_parts = [o.strip() for o in re.split(r'(?=[A-Z][a-zA-Z]+\s+Located in)', origin) if o.strip()]

                for col_index, tier_name in tier_cols.items():
                    if col_index >= len(row):
                        continue

                    rate = row[col_index].strip()
                    if not rate:
                        continue

                    rate_match = re.search(r"\d+\.\d{2}", rate)
                    if rate_match:
                        valid_rate = rate_match.group()
                    elif rate.lower() == "n/a":
                        valid_rate = "N/A"
                    else:
                        continue

                    for single_origin in origin_parts:
                        for bpd in bpd_ranges:
                            records.append(
                                build_record(
                                    pipeline_name,
                                    effective_date,
                                    single_origin,
                                    dest,
                                    valid_rate,
                                    expiry_date_value,
                                    tier_name,
                                    tariff_rate_type,
                                    min_bpd=bpd["MinBPD"],
                                    max_bpd=bpd["MaxBPD"],
                                )
                            )
            break

    if records:
        return records

    print("No valid records extracted.")
    return None


def extract_page6(pdf):
    unpivoted_data: List[Dict[str, Any]] = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for i, page in enumerate(pdf.pages[5:6]):
        text = page.extract_text()
        if not text:
            continue

        context = get_page_context(text, tariff_rate_type)
        tariff_rate_type = context["tariff_rate_type"]
        expiry_date_value = context["expiry_date_value"]
        bpd_ranges = context["bpd_ranges"]
        rate_tier = context["rate_tier"]

        if (
            tariff_rate_type in text
            or "cents per Barrel" in text
            or "All rates are unchanged." in text
        ):
            print(f"Found target table on Page {i + 1}.")

        tables = page.extract_tables() or []
        for table in tables:
            if not table or len(table) < 2:
                continue

            header = [clean(col).lower() if col else "" for col in table[0]]
            origin_idx = destination_idx = rate_idx = None

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
                    for bpd in bpd_ranges:
                        unpivoted_data.append(
                            build_record(
                                pipeline_name,
                                effective_date,
                                origin,
                                destination,
                                rate,
                                expiry_date_value,
                                rate_tier,
                                tariff_rate_type,
                                min_bpd=bpd["MinBPD"],
                                max_bpd=bpd["MaxBPD"],
                            )
                        )

    return unpivoted_data


def extract_page7(pdf):
    unpivoted_data: List[Dict[str, Any]] = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    for i, page in enumerate(pdf.pages[6:7]):
        text = page.extract_text()
        if not text:
            continue

        context = get_page_context(text, tariff_rate_type)
        tariff_rate_type = context["tariff_rate_type"]
        expiry_date_value = context["expiry_date_value"]
        bpd_ranges = context["bpd_ranges"]
        rate_tier = context["rate_tier"]

        if (
            tariff_rate_type in text
            or "cents per Barrel" in text
            or "All rates are unchanged." in text
        ):
            print(f"Found target table on Page {i + 1}.")

        tables = page.extract_tables() or []
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

                for bpd in bpd_ranges:
                    unpivoted_data.append(
                        build_record(
                            pipeline_name,
                            effective_date,
                            origin,
                            destination,
                            rate,
                            expiry_date_value,
                            rate_tier,
                            tariff_rate_type,
                            min_bpd=bpd["MinBPD"],
                            max_bpd=bpd["MaxBPD"],
                        )
                    )

    return unpivoted_data


def extract_page8(pdf):
    records: List[Dict[str, Any]] = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[7]
    text = page.extract_text() or ""

    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)
    rate_tier = extract_rate_tiers(text)

    tables = page.extract_tables() or []
    if not tables:
        return records

    for table in tables:
        if not table or len(table) < 2:
            continue

        cleaned = clean_table_rows(table)
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
        for i, h in enumerate(header_low):
            if "origin" in h:
                origin_col = i
            elif "minimum volume" in h or "production dedication volume" in h:
                vol_col = i

        if origin_col is None:
            continue

        dest_text = ""
        dest_col_candidates = [i for i, h in enumerate(header) if "Located in" in h]
        if dest_col_candidates:
            dest_text = header[dest_col_candidates[0]]

        prev_origin = ""
        prev_vol = ""

        for row in cleaned[header_idx + 1:]:
            row = pad_row(row, len(header))

            origin_val = row[origin_col].strip() if origin_col is not None else ""
            if origin_val:
                prev_origin = origin_val
            else:
                origin_val = prev_origin

            vol_val = row[vol_col].strip() if vol_col is not None else ""
            if vol_val:
                prev_vol = vol_val
            else:
                vol_val = prev_vol

            if not origin_val:
                continue

            destination_val = dest_text
            for i, h in enumerate(header_low):
                if "destination" in h:
                    body_dest = row[i].strip()
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
                records.append(
                    build_record(
                        pipeline_name,
                        effective_date,
                        origin_val,
                        destination_val,
                        rate_val,
                        expiry_date_value,
                        rate_tier,
                        tariff_rate_type,
                        min_bpd=min_bpd,
                        max_bpd=max_bpd,
                    )
                )

    return records


def extract_page9(pdf):
    results: List[Dict[str, Any]] = []
    tariff_rate_type = ""
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[8]
    text = page.extract_text() or ""

    rate_type = extract_tariff_rate_type(text)
    if rate_type:
        tariff_rate_type = rate_type

    expiry_date_value = extract_expiry_date(text)
    rate_tier = extract_rate_tiers(text)

    tables = page.extract_tables() or []
    if not tables:
        return results

    for table in tables:
        if not table or len(table) < 2:
            continue

        header = [clean(col).lower() if col else "" for col in table[0]]
        origin_idx = None
        destination_idx = None
        rate_indexes: List[int] = []

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
        previous_origin: List[str] = []

        for row in table[1:]:
            row = [cell if cell else "" for cell in row]
            raw_dest = row[destination_idx] if destination_idx < len(row) else ""
            destination = clean(raw_dest)
            if destination:
                previous_destination = destination
            else:
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
                            results.append(
                                build_record(
                                    pipeline_name,
                                    effective_date,
                                    origin,
                                    destination,
                                    rate_value,
                                    expiry_date_value,
                                    rate_tier,
                                    tariff_rate_type,
                                )
                            )

    return results


def extract_page10(pdf):
    records: List[Dict[str, Any]] = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[9]
    text = page.extract_text() or ""

    tariff_rate_type = extract_tariff_rate_type(text)
    expiry_date_value = extract_expiry_date(text)
    tables = page.extract_tables() or []
    if not tables:
        return records

    term_map = extract_term_year_from_page10_text(text)
    rate_tiers = extract_rate_tiers(text)

    for t_index, table in enumerate(tables):
        if not table or len(table) < 3:
            continue

        cleaned = clean_table_rows(table)
        header_bpd = cleaned[0]

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

        col_maps: List[Dict[str, Any]] = []
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

        for row in cleaned[2:]:
            row = pad_row(row, len(header_bpd))
            origin = row[origin_idx].strip()
            dest = row[dest_idx].strip()

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

            for mapping in col_maps:
                rate_val = row[mapping["col_i"]].strip() if mapping["col_i"] < len(row) else ""
                if not is_rate_or_na(rate_val):
                    continue

                rate_val = "N/A" if rate_val.lower() in ("n/a", "na") else str(rate_val)
                records.append(
                    build_record(
                        pipeline_name,
                        effective_date,
                        origin,
                        dest,
                        rate_val,
                        expiry_date_value,
                        rate_tiers,
                        tariff_rate_type,
                        term_year=term_map.get(t_index, ""),
                        min_bpd=mapping["min_bpd"],
                        max_bpd=mapping["max_bpd"],
                    )
                )

    return records


def extract_page11(pdf):
    records: List[Dict[str, Any]] = []
    pipeline_name, effective_date = extract_pipeline_metadata(pdf)

    page = pdf.pages[10]
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

        records.append(
            build_record(
                pipeline_name,
                effective_date,
                clean(origin),
                clean(dest),
                rate_str,
                expiry_date_value,
                rate_tier,
                tariff_rate_type,
                term_year=term_year,
                min_bpd=min_bpd,
                max_bpd=max_bpd,
            )
        )

    for table in tables:
        if not table or len(table) < 2:
            continue

        raw_rows = [["" if c is None else str(c) for c in row] for row in table]
        cleaned = clean_table_rows(table)
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


def run_extraction(pdf_path: str, output_file: str = DEFAULT_OUTPUT_FILE) -> pd.DataFrame:
    tariff_data: List[Dict[str, Any]] = []

    with pdfplumber.open(pdf_path) as pdf:
        extractors = [
            lambda doc: extract_matrix_tables(doc, 2, 4),
            extract_page_5,
            extract_page6,
            extract_page7,
            extract_page8,
            extract_page9,
            extract_page10,
            extract_page11,
            lambda doc: extract_matrix_tables(doc, 11, 15),
        ]

        for extractor in extractors:
            result = extractor(pdf)
            if result:
                tariff_data.extend(result)

    final_data = pd.DataFrame(tariff_data)
    if not final_data.empty:
        final_data.to_csv(output_file, index=False)
        print(f"\nData successfully exported to {output_file}")
    else:
        print("\nFailed to extract table data.")
    return final_data


if __name__ == "__main__":
    run_extraction(DEFAULT_INPUT_FILE)
