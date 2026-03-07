import csv
import re
from pathlib import Path

import pdfplumber


PDF_PATH = Path(r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony Express Pipeline.PDF")
OUTPUT_CSV = Path(r"D:\Project\python_freelance_project\reference_files\GasTariffSource\OilTariffFiles\Pony_Express_Pipeline.csv")


def clean_text(value) -> str:
    """
    Clean cell text:
    - convert None/empty to ''
    - remove internal newlines
    - collapse repeated spaces
    - trim whitespace
    """
    if value is None:
        return ""
    text = str(value).replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_empty(value: str) -> bool:
    return clean_text(value) == ""


def normalize_rate(value: str) -> str:
    """
    Keep numeric / % / N/A-like values.
    Empty intersections must become N/A.
    """
    text = clean_text(value)
    if not text:
        return "N/A"

    # Remove small footnote markers like [F1], [E], etc. from rate cells only
    text = re.sub(r"\[[A-Z0-9]+\]", "", text).strip()
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return "N/A"

    # Normalize common NA variants
    if text.lower() in {"n/a", "na", "n.a.", "n.a"}:
        return "N/A"

    return text


def is_header_keyword(text: str) -> bool:
    """
    Words that should not appear as actual Origin/Destination values.
    """
    t = clean_text(text).lower()
    return t in {
        "origin",
        "origins",
        "destination",
        "destinations",
        "rate",
        "minimum volume",
        "volume",
        "production dedication volume",
        "shipper a incentive rate",
        "shipper a extra barrel rate",
        "shipper b incentive rate",
        "shipper b extra barrel rate",
        "committed rate",
        "extra barrel rate",
        "augusta blend incentive rate",
        "light incentive rate",
        "long-term incentive rate",
        "secondary origin barrel rate",
        "buckingham barrel rate",
        "incentive rate",
        "rate tier 1",
        "rate tier 2",
        "tier i",
        "tier ii",
    }


def looks_like_rate(text: str) -> bool:
    """
    Decide whether a cell behaves like a rate value.
    Accepts numeric, decimal, percent, N/A-like, or footnote-prefixed numeric values.
    """
    t = clean_text(text)
    if not t:
        return True  # empty intersections must be treated as N/A

    # Remove footnote markers / embargo markers around values
    t = re.sub(r"\[[A-Z0-9]+\]", "", t).strip()
    if not t:
        return True

    if t.lower() in {"n/a", "na", "n.a.", "n.a"}:
        return True

    return bool(re.fullmatch(r"-?\d+(?:\.\d+)?%?", t))


def pad_rows(rows):
    """
    Make all rows same length.
    """
    max_len = max(len(r) for r in rows) if rows else 0
    return [r + [""] * (max_len - len(r)) for r in rows]


def remove_empty_rows_and_cols(rows):
    """
    Remove fully empty rows and fully empty columns.
    """
    if not rows:
        return rows

    rows = pad_rows(rows)

    # Remove empty rows
    rows = [r for r in rows if any(clean_text(c) for c in r)]
    if not rows:
        return rows

    rows = pad_rows(rows)

    # Remove empty columns
    keep_cols = []
    for col_idx in range(len(rows[0])):
        col_values = [clean_text(r[col_idx]) for r in rows]
        if any(col_values):
            keep_cols.append(col_idx)

    cleaned = [[row[i] for i in keep_cols] for row in rows]
    return cleaned


def merge_header_rows(rows, max_header_scan=4):
    """
    Dynamically detect destination header area.
    For matrix-style tables, destination labels are often split across multiple rows.
    We merge the header rows column-wise until the first clear data row appears.

    Heuristic:
    - find the first row where most cells (excluding col 0) look like rates -> data starts there
    - everything above that becomes header rows
    """
    if not rows:
        return [], []

    rows = pad_rows(rows)

    data_start = None
    scan_limit = min(len(rows), max_header_scan + 2)

    for r_idx in range(scan_limit):
        row = rows[r_idx]
        non_first = row[1:] if len(row) > 1 else []
        non_empty = [clean_text(x) for x in non_first if clean_text(x)]

        if not non_empty:
            continue

        rate_like_count = sum(looks_like_rate(x) for x in non_first)
        ratio = rate_like_count / max(1, len(non_first))

        # If most cells after first column are rate-like, this is likely first data row
        if ratio >= 0.60:
            data_start = r_idx
            break

    if data_start is None:
        # fallback: assume first row is header, remaining rows data
        data_start = 1 if len(rows) > 1 else len(rows)

    header_rows = rows[:data_start]
    data_rows = rows[data_start:]

    return header_rows, data_rows


def build_destinations(header_rows, col_count):
    """
    Build destination names by concatenating each header cell vertically.
    Excludes column 0.
    """
    destinations = []
    for col_idx in range(1, col_count):
        parts = []
        for hrow in header_rows:
            if col_idx < len(hrow):
                cell = clean_text(hrow[col_idx])
                if cell and not is_header_keyword(cell):
                    parts.append(cell)

        dest = clean_text(" ".join(parts))
        destinations.append(dest)

    return destinations


def propagate_origin(previous_origin: str, current_cell: str) -> str:
    """
    Some tables have merged / blank origin cells for sub-rows.
    If current origin cell is blank or generic, reuse previous origin.
    """
    curr = clean_text(current_cell)
    if not curr or is_header_keyword(curr):
        return previous_origin
    return curr


def extract_matrix_records(rows, page_number):
    """
    Extract records from a matrix-style table:
    first column = Origin
    top header rows = Destination(s)
    remaining intersections = Rate
    """
    rows = remove_empty_rows_and_cols(rows)
    if not rows or len(rows) < 2:
        return []

    rows = pad_rows(rows)
    header_rows, data_rows = merge_header_rows(rows)

    if not data_rows:
        return []

    col_count = len(rows[0])
    destinations = build_destinations(header_rows, col_count)

    # Ignore blank / keyword destinations dynamically
    valid_dest_indices = []
    for i, d in enumerate(destinations, start=1):
        if clean_text(d) and not is_header_keyword(d):
            valid_dest_indices.append(i)

    records = []
    current_origin = ""

    for row in data_rows:
        if not row:
            continue

        current_origin = propagate_origin(current_origin, row[0])
        origin = clean_text(current_origin)

        if not origin or is_header_keyword(origin):
            continue

        for col_idx in valid_dest_indices:
            destination = clean_text(destinations[col_idx - 1])

            if not destination or is_header_keyword(destination):
                continue

            rate = normalize_rate(row[col_idx] if col_idx < len(row) else "")

            records.append({
                "Origin": origin,
                "Destination": destination,
                "Rate": rate,
                "Page": page_number,
            })

    return records


def table_to_rows(table):
    """
    Convert raw pdfplumber table into normalized row list.
    """
    if not table:
        return []

    rows = []
    for row in table:
        if row is None:
            continue
        rows.append([clean_text(cell) for cell in row])

    return remove_empty_rows_and_cols(rows)


def dedupe_records(records):
    """
    Remove exact duplicates while preserving order.
    """
    seen = set()
    output = []

    for rec in records:
        key = (rec["Origin"], rec["Destination"], rec["Rate"])
        if key not in seen:
            seen.add(key)
            output.append(rec)

    return output


def extract_tables_from_pdf(pdf_path: Path):
    """
    Read pages 3..15 and extract matrix-like transportation rate tables dynamically.
    """
    all_records = []

    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": 5,
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 20,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
        "text_tolerance": 3,
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page_index in range(2, 15):  # pages 3 through 15 (0-based index)
            page = pdf.pages[page_index]
            page_number = page_index + 1

            tables = page.extract_tables(table_settings=table_settings)

            if not tables:
                continue

            for raw_table in tables:
                rows = table_to_rows(raw_table)
                if not rows:
                    continue

                records = extract_matrix_records(rows, page_number)
                if records:
                    all_records.extend(records)

    return dedupe_records(all_records)


def write_csv(records, output_path: Path):
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Origin", "Destination", "Rate"])
        writer.writeheader()
        for rec in records:
            writer.writerow({
                "Origin": rec["Origin"],
                "Destination": rec["Destination"],
                "Rate": rec["Rate"],
            })


def main():
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    records = extract_tables_from_pdf(PDF_PATH)
    write_csv(records, OUTPUT_CSV)

    print(f"Done. Extracted {len(records)} rows to: {OUTPUT_CSV.resolve()}")


if __name__ == "__main__":
    main()