#!/usr/bin/env python3
"""
Generic PDF tariff table extractor (pages 3 to 15 inclusive)
- NO hard-coded page indexes for specific formats.
- Uses "switch-case style" via nested try/except as requested:
    try:    extract_table_format1 (matrix)
    except: try: extract_table_format2 (3-column)
            except: extract_table_format3 (2-column)

It loops through pages 3..15, detects tables on each page, and extracts rows.
Pages that don't match any known format are automatically skipped.

Dependencies:
  pip install pdfplumber pandas

Run:
  python extract_tariff_tables.py --pdf "Pony Express Pipeline.PDF" --out "extracted_data.csv"
"""

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
import pandas as pd


# -----------------------------
# Utilities
# -----------------------------
def clean(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm(x: Any) -> str:
    return clean(x).lower()


def is_numeric_or_na(x: Any) -> bool:
    s = clean(x)
    if not s:
        return False
    if s.lower() in ("n/a", "na"):
        return True
    # allow: 19.90, 630.43, 0.150%, 0.050%
    if re.fullmatch(r"\d+(?:\.\d+)?%?", s):
        return True
    return False


def is_percent(x: Any) -> bool:
    s = clean(x)
    return bool(re.fullmatch(r"\d+(?:\.\d+)?%", s))


def safe_int(x: str) -> Optional[int]:
    try:
        return int(x.replace(",", ""))
    except Exception:
        return None


def parse_bpd_range(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract MinBPD/MaxBPD from strings like:
      - 5,000 – 11,999 BPD
      - 12,000 - 23,999 BPD
      - 24,000 or greater BPD
      - 13,000 bpd or greater
      - 10,000 BPD
    Returns (min_bpd, max_bpd) where max_bpd can be None for "or greater".
    """
    t = clean(text).replace("–", "-").replace("—", "-")
    low = t.lower()

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*bpd", low)
    if m:
        return safe_int(m.group(1)), safe_int(m.group(2))

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bpd\s*or\s*greater", low)
    if not m:
        m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*or\s*greater\s*bpd", low)
    if m:
        return safe_int(m.group(1)), None

    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bpd", low)
    if m:
        v = safe_int(m.group(1))
        return v, v

    return None, None


@dataclass
class Meta:
    pipeline_name: str = ""
    effective_date: str = ""  # dd-mm-yyyy
    rate_type: str = ""       # extracted from page text (best-effort)
    end_date: str = ""        # best-effort expiry wording (often blank)


def extract_pipeline_metadata(pdf: pdfplumber.PDF) -> Meta:
    meta = Meta()
    page1_text = (pdf.pages[0].extract_text() or "")

    m = re.search(r"(.*Pipeline.*LLC)", page1_text, re.IGNORECASE)
    if m:
        meta.pipeline_name = clean(m.group(1))

    m = re.search(r"EFFECTIVE:\s*(.*)", page1_text, re.IGNORECASE)
    if m:
        raw = clean(m.group(1))
        try:
            dt = datetime.strptime(raw, "%B %d, %Y")
            meta.effective_date = dt.strftime("%d-%m-%Y")
        except Exception:
            meta.effective_date = raw

    return meta


def extract_rate_type_from_text(page_text: str) -> str:
    """
    Best-effort: returns a 'section title' like:
      NON-CONTRACT TRANSPORTATION RATES
      CONTRACT RATES
      PLA RATES and ORIGIN/DESTINATION GRADE PAIRS
    """
    text = (page_text or "").replace("\r", "")
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    # pick first line containing 'RATES' (or 'PLA RATES')
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "RATES" in up:
            # sometimes next line belongs to title too (e.g. ORIGIN/DESTINATION GRADE PAIRS)
            extra = []
            for j in range(1, 3):
                if i + j < len(lines):
                    nxt = lines[i + j].strip()
                    if nxt.isupper():
                        extra.append(nxt)
                    else:
                        break
            return clean(" ".join([ln] + extra))
    return ""


def extract_end_date_from_text(page_text: str) -> str:
    """
    Best-effort; many pages won't have it.
    """
    t = page_text or ""
    m = re.search(r"expire[s]?\s+on.*?([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", t, re.IGNORECASE)
    if not m:
        return ""
    raw = clean(m.group(1))
    try:
        dt = datetime.strptime(raw, "%B %d, %Y")
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return raw


# -----------------------------
# Format detectors
# -----------------------------
def looks_like_matrix_table(table: List[List[Any]]) -> bool:
    """
    Matrix tables (pages like 3,4,12-15) usually:
      - have many destination columns (>=5)
      - first column is origins
      - lots of numeric/percent/N/A cells
      - header row includes destination labels (often 'Located in')
    """
    if not table or len(table) < 3:
        return False
    max_cols = max(len(r) for r in table)
    if max_cols < 5:
        return False

    header = [clean(c) for c in table[0]]
    header_text = " ".join(header).lower()

    # common signal words on these matrix headers
    header_signal = ("destinations" in header_text) or ("located in" in header_text)

    # numeric density on body (excluding first col)
    numeric = 0
    total = 0
    for r in table[1:]:
        for c in r[1:]:
            s = clean(c)
            if not s:
                continue
            total += 1
            if is_numeric_or_na(s):
                numeric += 1

    if total == 0:
        return False

    density = numeric / total
    return header_signal or density >= 0.55


def looks_like_3col_table(table: List[List[Any]]) -> bool:
    """
    3-column tables:
      Origin | Destination | Rate
    or sometimes 4 columns with a clear "Origin" "Destination" "Rate".
    """
    if not table or len(table) < 2:
        return False
    max_cols = max(len(r) for r in table)
    if max_cols < 3:
        return False

    # try find header row with origin/destination/rate
    for row in table[:3]:
        row_low = " ".join(norm(c) for c in row if clean(c))
        if "origin" in row_low and "destination" in row_low and ("rate" in row_low or "rates" in row_low):
            return True

    # fallback: if exactly 3 cols and last col mostly numeric/na
    if max_cols == 3:
        numeric = 0
        total = 0
        for r in table[1:]:
            if len(r) < 3:
                continue
            s = clean(r[2])
            if not s:
                continue
            total += 1
            if is_numeric_or_na(s):
                numeric += 1
        return total > 0 and (numeric / total) >= 0.6

    return False


def looks_like_2col_table(table: List[List[Any]]) -> bool:
    """
    2-column tables:
      (Key) | (Value)
    Example in these tariffs: sometimes just Origin Destination Rate split oddly,
    or mini tables like "Origin Destination Rate" collapsed.
    We'll treat "2 columns with numeric/na in second col" as candidate.
    """
    if not table or len(table) < 2:
        return False
    max_cols = max(len(r) for r in table)
    if max_cols != 2:
        return False

    numeric = 0
    total = 0
    for r in table[1:]:
        if len(r) < 2:
            continue
        v = clean(r[1])
        if not v:
            continue
        total += 1
        if is_numeric_or_na(v):
            numeric += 1
    return total > 0 and (numeric / total) >= 0.6


# -----------------------------
# Extractors (format implementations)
# -----------------------------
def extract_table_format1_matrix(
    table: List[List[Any]],
    meta: Meta,
    page_number_1based: int
) -> List[Dict[str, Any]]:
    """
    Unpivots a matrix:
      - table[0] = destination header row (or part of it)
      - first column = origin labels
      - body cells = rates (numeric, percent, N/A)
    Output one record per (origin, destination, rate).
    """
    if not looks_like_matrix_table(table):
        raise ValueError("Not a matrix table")

    # Normalize row lengths
    max_cols = max(len(r) for r in table)
    rows = []
    for r in table:
        rr = [clean(c) for c in r]
        if len(rr) < max_cols:
            rr += [""] * (max_cols - len(rr))
        rows.append(rr)

    header = rows[0]
    # Find destination column labels (skip first col)
    dest_cols = header[1:]

    # Forward-fill destination labels if blanks exist (common with wrapped PDF headers)
    last = ""
    for i in range(len(dest_cols)):
        if dest_cols[i]:
            last = dest_cols[i]
        else:
            dest_cols[i] = last

    # Some pages include an extra header row like "Destinations" - merge if useful
    # If row[1] contains 'Located in' fragments where header is blank, we can merge.
    if len(rows) > 1:
        second = rows[1][1:]
        for i in range(len(dest_cols)):
            if second[i] and ("located in" in second[i].lower()) and second[i].lower() not in dest_cols[i].lower():
                dest_cols[i] = clean(f"{dest_cols[i]} {second[i]}")

    out = []
    for r in rows[1:]:
        origin = r[0]
        if not origin:
            continue
        for j, dest in enumerate(dest_cols, start=1):
            rate = r[j] if j < len(r) else ""
            if not rate:
                continue
            if not is_numeric_or_na(rate):
                continue

            # put % rates into Surcharge or keep in LiquidRate? keep in LiquidRate as string
            out.append({
                "Page": page_number_1based,
                "Pipeline Name": meta.pipeline_name,
                "PointfOrigin": origin,
                "PointOfDestination": dest,
                "RateType": meta.rate_type,
                "Effective Date": meta.effective_date,
                "End Date": meta.end_date,
                "RateTier": None,
                "TermYear": "",
                "MinBPD": "",
                "MaxBPD": "",
                "LiquidRateCentsPerBbl": rate,   # may be % for PLA pages; keep as-is
                "SurchargeCentsPerBbl": "",
                "LiquidFuelType": "Crude",
            })
    if not out:
        raise ValueError("Matrix table parsed but produced no rows")
    return out


def extract_table_format2_three_col(
    table: List[List[Any]],
    meta: Meta,
    page_number_1based: int
) -> List[Dict[str, Any]]:
    """
    Extracts common 'Origin | Destination | Rate' style tables.
    Also supports 4+ columns if header indicates rate columns.
    """
    if not looks_like_3col_table(table):
        raise ValueError("Not a 3-column style table")

    # Normalize rows
    max_cols = max(len(r) for r in table)
    rows = []
    for r in table:
        rr = [clean(c) for c in r]
        if len(rr) < max_cols:
            rr += [""] * (max_cols - len(rr))
        rows.append(rr)

    # Find header row
    header_idx = None
    for i, r in enumerate(rows[:4]):
        low = " ".join(norm(c) for c in r if c)
        if "origin" in low and "destination" in low:
            header_idx = i
            break
    if header_idx is None:
        header_idx = 0  # fallback

    header = rows[header_idx]
    header_low = [h.lower() for h in header]

    # Identify columns
    origin_col = None
    dest_col = None
    rate_cols: List[int] = []

    for i, h in enumerate(header_low):
        if "origin" in h:
            origin_col = i
        elif "destination" in h:
            dest_col = i
        elif "rate" in h:
            rate_cols.append(i)

    # Fallback if header didn't label "rate": assume last column is rate
    if origin_col is None or dest_col is None:
        # try assume first=origin second=dest for 3-col tables
        if max_cols >= 3:
            origin_col = 0
            dest_col = 1
            rate_cols = [2]
        else:
            raise ValueError("Could not identify origin/destination columns")

    if not rate_cols:
        # if no explicit rate columns, assume all non origin/dest columns are rates
        rate_cols = [i for i in range(max_cols) if i not in (origin_col, dest_col)]

    out = []
    prev_origin = ""
    prev_dest = ""

    for r in rows[header_idx + 1:]:
        origin = r[origin_col] if origin_col < len(r) else ""
        dest = r[dest_col] if dest_col < len(r) else ""

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

        for rc in rate_cols:
            rate = r[rc] if rc < len(r) else ""
            if not rate:
                continue
            if not is_numeric_or_na(rate):
                continue

            tier = clean(header[rc]) if rc < len(header) else ""
            tier = tier if tier else None

            # if BPD range appears somewhere in row or tier, fill Min/Max
            min_bpd, max_bpd = (None, None)
            # row-wide search for a BPD string (best-effort)
            bpd_blob = " ".join(r)
            if "bpd" in bpd_blob.lower():
                min_bpd, max_bpd = parse_bpd_range(bpd_blob)

            out.append({
                "Page": page_number_1based,
                "Pipeline Name": meta.pipeline_name,
                "PointfOrigin": origin,
                "PointOfDestination": dest,
                "RateType": meta.rate_type,
                "Effective Date": meta.effective_date,
                "End Date": meta.end_date,
                "RateTier": tier,
                "TermYear": "",
                "MinBPD": min_bpd if min_bpd is not None else "",
                "MaxBPD": max_bpd if max_bpd is not None else "",
                "LiquidRateCentsPerBbl": "N/A" if rate.lower() in ("n/a", "na") else rate,
                "SurchargeCentsPerBbl": "",
                "LiquidFuelType": "Crude",
            })

    if not out:
        raise ValueError("3-column table parsed but produced no rows")
    return out


def extract_table_format3_two_col(
    table: List[List[Any]],
    meta: Meta,
    page_number_1based: int
) -> List[Dict[str, Any]]:
    """
    Extracts 2-column tables where col1=label (origin/destination/etc) and col2=value (rate).
    Output is best-effort:
      - Put label in PointOfDestination (or Origin if we can detect)
      - Put rate in LiquidRateCentsPerBbl
    """
    if not looks_like_2col_table(table):
        raise ValueError("Not a 2-column style table")

    max_cols = max(len(r) for r in table)
    if max_cols != 2:
        raise ValueError("Not exactly 2 columns")

    rows = [[clean(c) for c in r[:2]] for r in table if r and (clean(r[0]) or clean(r[1]))]

    # Try detect whether first column is "Origin" or "Destination" labels
    header_low = " ".join(norm(c) for c in rows[0])
    has_origin_word = "origin" in header_low
    has_dest_word = "destination" in header_low

    out = []
    prev_label = ""
    for r in rows[1:]:
        label = r[0]
        val = r[1]
        if label:
            prev_label = label
        else:
            label = prev_label

        if not label or not val:
            continue
        if not is_numeric_or_na(val):
            continue

        rec = {
            "Page": page_number_1based,
            "Pipeline Name": meta.pipeline_name,
            "PointfOrigin": label if has_origin_word and not has_dest_word else "",
            "PointOfDestination": label if has_dest_word or not has_origin_word else label,
            "RateType": meta.rate_type,
            "Effective Date": meta.effective_date,
            "End Date": meta.end_date,
            "RateTier": None,
            "TermYear": "",
            "MinBPD": "",
            "MaxBPD": "",
            "LiquidRateCentsPerBbl": "N/A" if val.lower() in ("n/a", "na") else val,
            "SurchargeCentsPerBbl": "",
            "LiquidFuelType": "Crude",
        }
        out.append(rec)

    if not out:
        raise ValueError("2-column table parsed but produced no rows")
    return out


# -----------------------------
# Main page loop (pages 3..15)
# -----------------------------
def extract_pages_3_to_15(pdf_path: str) -> pd.DataFrame:
    results: List[Dict[str, Any]] = []

    with pdfplumber.open(pdf_path) as pdf:
        base_meta = extract_pipeline_metadata(pdf)

        # pages 3..15 inclusive => 1-based numbers 3..15
        for page_no_1based in range(3, 16):
            page_idx = page_no_1based - 1  # internal index (this is NOT format hardcoding, only navigation)
            if page_idx < 0 or page_idx >= len(pdf.pages):
                continue

            page = pdf.pages[page_idx]
            page_text = page.extract_text() or ""

            meta = Meta(
                pipeline_name=base_meta.pipeline_name,
                effective_date=base_meta.effective_date,
                rate_type=extract_rate_type_from_text(page_text),
                end_date=extract_end_date_from_text(page_text),
            )

            tables = page.extract_tables() or []
            if not tables:
                # no tables -> skip
                continue

            for table in tables:
                # "switch case" style nested try/except requested by you:
                try:
                    # Format 1: Matrix (pages like 3,4,12-15 will match; 5-11 usually won't)
                    rows = extract_table_format1_matrix(table, meta, page_no_1based)
                    results.extend(rows)
                except Exception:
                    try:
                        # Format 2: 3-column style (pages like 5-11 will match; matrix pages won't)
                        rows = extract_table_format2_three_col(table, meta, page_no_1based)
                        results.extend(rows)
                    except Exception:
                        try:
                            # Format 3: 2-column style (rare; fallback)
                            rows = extract_table_format3_two_col(table, meta, page_no_1based)
                            results.extend(rows)
                        except Exception:
                            # Unknown/unsupported table -> skip quietly (production-safe)
                            continue

    df = pd.DataFrame(results)

    # Optional: drop empty origin+destination rows (safety)
    if not df.empty:
        df = df[(df["PointfOrigin"].astype(str).str.strip() != "") | (df["PointOfDestination"].astype(str).str.strip() != "")]
        df.reset_index(drop=True, inplace=True)

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
    "--pdf",
    default=r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF",
    help="Input PDF path"
    )

    parser.add_argument(
    "--out",
    default=r"D:\Project\python_freelance_project\extracted_data.csv",
    help="Output CSV path"
    )
    
    args = parser.parse_args()

    df = extract_pages_3_to_15(args.pdf)
    if df is None or df.empty:
        print("No data extracted from pages 3..15.")
        # Still write an empty CSV for pipeline consistency
        pd.DataFrame().to_csv(args.out, index=False)
        return

    df.to_csv(args.out, index=False)
    print(f"Extracted {len(df)} rows to: {args.out}")


if __name__ == "__main__":
    main()