"""
Task 1 / Step 1.2 - Automated ingestion pipeline
Fairbanks climate CSV  ->  InfluxDB Line Protocol  ->  bucket 'climate_raw'

Key requirement satisfied here: every point is written with the record's
ORIGINAL historical timestamp (derived from the dataset), never the local
system clock.

Install dependency first:
    pip install influxdb-client

Run (after `docker compose up -d` and downloading the CSV next to this file):
    python ingest.py fairbanks_climate.csv
"""

import csv
import sys
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# ---- Connection settings (must match docker-compose.yml) --------------------
URL    = "http://localhost:8086"
TOKEN  = "coventry-bigdata-supersecret-token"
ORG    = "coventry"
BUCKET = "climate_raw"

MONTHS = ["jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec"]


def open_rows(path):
    """Yield dict rows, skipping comment/blank lines before the header."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        lines = [ln for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
    reader = csv.DictReader(lines)
    print(f"[detect] columns: {reader.fieldnames}")
    yield from reader


def norm(name):
    return (name or "").strip().lower()


def mid_year(daterange):
    """'1961-1990' -> 1975 ; '2020' -> 2020. Midpoint of the reported span."""
    parts = [p for p in str(daterange).replace("_", "-").split("-") if p.strip().isdigit()]
    years = [int(p) for p in parts]
    if not years:
        return None
    return (min(years) + max(years)) // 2


def to_float(v):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def build_points(rows):
    """
    Supports both layouts:
      A) SNAP 'wide' community-chart export:
         one row per (scenario, daterange, variable) with Jan..Dec value columns.
         Each month becomes ONE point timestamped at the 1st of that month in the
         midpoint year of the daterange.
      B) 'long' layout: a date/time column plus numeric value columns.
    """
    rows = list(rows)
    if not rows:
        return

    cols = {norm(c): c for c in rows[0].keys()}
    # SNAP community-chart exports use either a bare month column ("jan") or
    # separate min/mean/max columns per month ("janmin"/"janmean"/"janmax").
    month_cols = {m: cols[m] for m in MONTHS if m in cols}
    month_variant_cols = {
        m: {v: cols[m + v] for v in ("min", "mean", "max") if (m + v) in cols}
        for m in MONTHS
    }
    has_variants = any(month_variant_cols[m] for m in MONTHS)

    if month_cols or has_variants:  # ---- layout A (expected for SNAP data) ----
        if has_variants:
            print(f"[detect] wide monthly layout with min/mean/max columns")
        else:
            print(f"[detect] wide monthly layout ({len(month_cols)} month columns)")
        for row in rows:
            year = mid_year(row.get(cols.get("daterange", ""), "") or
                            row.get(cols.get("decade", ""), ""))
            if year is None:
                continue

            # variable name for the field, e.g. Temperature / Precipitation
            var = norm(row.get(cols.get("type", ""), "")) or "value"
            field = var.replace(" ", "_")

            for i, m in enumerate(MONTHS, start=1):
                variants = month_variant_cols.get(m, {})
                if not variants and m not in month_cols:
                    continue
                ts = datetime(year, i, 1, tzinfo=timezone.utc)  # historical time
                p = (
                    Point("climate")
                    .tag("community", row.get(cols.get("community", ""), "Fairbanks") or "Fairbanks")
                    .tag("scenario", row.get(cols.get("scenario", ""), "") or "historical")
                    .tag("resolution", row.get(cols.get("resolution", ""), "") or "n/a")
                    .tag("daterange", row.get(cols.get("daterange", ""), "") or "n/a")
                    .time(ts, WritePrecision.S)
                )
                has_field = False
                if variants:
                    for v, orig in variants.items():
                        val = to_float(row.get(orig))
                        if val is not None:
                            p = p.field(f"{field}_{v}", val)
                            has_field = True
                else:
                    val = to_float(row.get(month_cols[m]))
                    if val is not None:
                        p = p.field(field, val)
                        has_field = True
                if has_field:
                    yield p

    else:  # ---- layout B fallback ----
        date_col = next((cols[c] for c in ("date", "time", "timestamp", "datetime")
                         if c in cols), None)
        if date_col is None:
            sys.exit("[error] Could not find month columns or a date column - "
                     "inspect the CSV header printed above.")
        print(f"[detect] long layout, date column = {date_col}")
        for row in rows:
            raw = str(row[date_col]).strip()
            ts = None
            for fmt in ("%Y-%m-%d", "%Y-%m", "%m/%d/%Y", "%Y"):
                try:
                    ts = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    pass
            if ts is None:
                continue
            p = Point("climate").tag("community", "Fairbanks").time(ts, WritePrecision.S)
            has_field = False
            for c, orig in cols.items():
                if orig == date_col:
                    continue
                val = to_float(row.get(orig))
                if val is not None:
                    p = p.field(c, val)
                    has_field = True
            if has_field:
                yield p


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: python ingest.py <path-to-fairbanks_climate.csv>")

    with InfluxDBClient(url=URL, token=TOKEN, org=ORG, timeout=60_000) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        batch, total = [], 0
        for point in build_points(open_rows(sys.argv[1])):
            batch.append(point)
            if len(batch) >= 500:
                write_api.write(bucket=BUCKET, record=batch)
                total += len(batch)
                print(f"[write] {total} points...")
                batch = []
        if batch:
            write_api.write(bucket=BUCKET, record=batch)
            total += len(batch)
        print(f"[done] {total} points written to bucket '{BUCKET}' "
              f"with historical timestamps.")


if __name__ == "__main__":
    main()
