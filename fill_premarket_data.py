"""
fill_premarket_data.py
──────────────────────────────────────────────────────────────────────────────
Reads the trading tracker Excel file, finds blank cells in the Pre-Trade Data
sheet, queries Alpaca for the missing data, and writes it back.

COLUMNS FILLED
──────────────
Col 13  Prev High ($)              → previous trading day High
Col 14  Prev Low ($)               → previous trading day Low
Col 15  Prev Close ($)             → previous trading day Close
Col 16  PM Open                    → pre-market open (first bar 4:00 AM ET)
Col 17  Pre Market High of Day ($) → highest price 4:00–9:29 AM ET
Col 18  Pre Market Low of Day ($)  → lowest price  4:00–9:29 AM ET
Col 19  Market Open                → regular session open (9:30 AM ET bar)

USAGE
──────
  pip install requests openpyxl
  python fill_premarket_data.py
  python fill_premarket_data.py --file path/to/other_tracker.xlsx

NOTES
──────
- Only fills cells that are currently blank — never overwrites existing data
- Uses Alpaca IEX feed (free tier compatible)
- Looks back up to 7 calendar days to find the previous trading day
- Pre-market bars use 1-minute timeframe, 4:00–9:29 AM ET
"""

import argparse
import time
from datetime import date, datetime, timedelta

import openpyxl
import requests
from openpyxl import load_workbook

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
KEY    = "PKIYGXQT3DGX7B6BDIZFZ6VQWU"
SECRET = "Ekaz5bQHUbbFUQvmidbBSU89wHMtgigik3TsyFD15NA3"

DEFAULT_FILE = "merged_trading_tracker__4_.xlsx"
SHEET_NAME   = "Pre-Trade Data"
HEADER_ROW   = 2   # row 1 is the title, row 2 has column headers
DATA_START   = 3   # data begins at row 3

HEADERS = {
    "APCA-API-KEY-ID":     KEY,
    "APCA-API-SECRET-KEY": SECRET,
}

# Column indices (1-based, matching openpyxl)
COL_DATE      = 1
COL_TICKER    = 2
COL_PREV_HIGH = 13
COL_PREV_LOW  = 14
COL_PREV_CLOSE= 15
COL_PM_OPEN   = 16
COL_PM_HIGH   = 17
COL_PM_LOW    = 18
COL_MKT_OPEN  = 19

# ─────────────────────────────────────────
# DATE PARSING
# ─────────────────────────────────────────

def parse_date(val) -> date | None:
    """Parse whatever date format is in the cell into a date object."""
    if val is None:
        return None
    if isinstance(val, (datetime,)):
        return val.date()
    if isinstance(val, date):
        return val
    # String formats e.g. "May 08, 2026" or "2026-05-08"
    s = str(val).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────
# ALPACA — DAILY BARS (prev high/low/close)
# ─────────────────────────────────────────

def fetch_daily_bars(ticker: str, start: date, end: date) -> list[dict]:
    """Return daily bars for ticker between start and end inclusive."""
    resp = requests.get(
        f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
        headers=HEADERS,
        params={
            "timeframe":  "1Day",
            "start":      start.isoformat(),
            "end":        end.isoformat(),
            "limit":      10,
            "feed":       "iex",
            "adjustment": "raw",
        },
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"    ⚠ Daily bars error {resp.status_code} for {ticker}: {resp.text[:120]}")
        return []
    return resp.json().get("bars", [])


def get_prev_day_bar(ticker: str, trade_date: date) -> dict | None:
    """
    Return the daily bar for the most recent trading day before trade_date.
    Looks back up to 10 calendar days to skip weekends/holidays.
    """
    look_back_start = trade_date - timedelta(days=10)
    look_back_end   = trade_date - timedelta(days=1)
    bars = fetch_daily_bars(ticker, look_back_start, look_back_end)
    if not bars:
        return None
    # Bars are sorted ascending — return the last one (closest to trade_date)
    return bars[-1]


# ─────────────────────────────────────────
# ALPACA — MINUTE BARS (pre-market)
# ─────────────────────────────────────────

def fetch_minute_bars(ticker: str, day: date, start_hour: int, start_min: int,
                      end_hour: int, end_min: int) -> list[dict]:
    """
    Return 1-minute bars for ticker on day between start and end time (ET).
    Times are expressed as ET hours/minutes; Alpaca expects RFC3339 UTC.
    We pass the ET offset as -04:00 (EDT, US markets Apr–Oct) or -05:00 (EST).
    Alpaca accepts timezone-aware ISO strings directly.
    """
    # Build timezone-aware strings using ET offset
    # EDT (UTC-4) covers all of May/June which is the current date range
    tz_offset = "-04:00"
    start_str = f"{day.isoformat()}T{start_hour:02d}:{start_min:02d}:00{tz_offset}"
    end_str   = f"{day.isoformat()}T{end_hour:02d}:{end_min:02d}:00{tz_offset}"

    resp = requests.get(
        f"https://data.alpaca.markets/v2/stocks/{ticker}/bars",
        headers=HEADERS,
        params={
            "timeframe": "1Min",
            "start":     start_str,
            "end":       end_str,
            "limit":     500,
            "feed":      "sip",
        },
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"    ⚠ Minute bars error {resp.status_code} for {ticker}: {resp.text[:120]}")
        return []
    return resp.json().get("bars", [])


def get_premarket_data(ticker: str, day: date) -> dict:
    """
    Return pre-market open, high, low for 4:00–9:29 AM ET on day.
    Also return the 9:30 AM bar open as market_open.
    """
    result = {
        "pm_open":    None,
        "pm_high":    None,
        "pm_low":     None,
        "mkt_open":   None,
    }

    # Pre-market: 4:00 AM – 9:29 AM ET
    pm_bars = fetch_minute_bars(ticker, day, 4, 0, 9, 29)
    if pm_bars:
        result["pm_open"] = round(pm_bars[0]["o"], 4)   # open of first bar
        result["pm_high"] = round(max(b["h"] for b in pm_bars), 4)
        result["pm_low"]  = round(min(b["l"] for b in pm_bars), 4)

    # Market open: 9:30 AM single bar
    mkt_bars = fetch_minute_bars(ticker, day, 9, 30, 9, 30)
    if mkt_bars:
        result["mkt_open"] = round(mkt_bars[0]["o"], 4)

    return result


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main(filepath: str):
    print(f"\nLoading: {filepath}")
    wb = load_workbook(filepath)

    if SHEET_NAME not in wb.sheetnames:
        print(f"❌ Sheet '{SHEET_NAME}' not found. Available: {wb.sheetnames}")
        return

    ws = wb[SHEET_NAME]
    max_row = ws.max_row

    # Build list of rows that need at least one blank filled
    target_cols = [COL_PREV_HIGH, COL_PREV_LOW, COL_PREV_CLOSE,
                   COL_PM_OPEN, COL_PM_HIGH, COL_PM_LOW, COL_MKT_OPEN]

    rows_to_process = []
    for row_idx in range(DATA_START, max_row + 1):
        date_val   = ws.cell(row=row_idx, column=COL_DATE).value
        ticker_val = ws.cell(row=row_idx, column=COL_TICKER).value
        if not date_val or not ticker_val:
            continue
        # Check if any target column is blank
        blanks = [c for c in target_cols
                  if ws.cell(row=row_idx, column=c).value is None]
        if blanks:
            rows_to_process.append((row_idx, str(ticker_val).strip(), date_val, blanks))

    if not rows_to_process:
        print("✓ No blank cells found — nothing to fill.")
        return

    print(f"  {len(rows_to_process)} rows need data.\n")

    filled_count = 0

    for row_idx, ticker, date_raw, blanks in rows_to_process:
        trade_date = parse_date(date_raw)
        if trade_date is None:
            print(f"  Row {row_idx}: could not parse date '{date_raw}' — skipping.")
            continue

        print(f"  Row {row_idx} | {trade_date} | {ticker} | blanks: {[ws.cell(2,c).value for c in blanks]}")

        needs_prev  = any(c in blanks for c in [COL_PREV_HIGH, COL_PREV_LOW, COL_PREV_CLOSE])
        needs_pm    = any(c in blanks for c in [COL_PM_OPEN, COL_PM_HIGH, COL_PM_LOW, COL_MKT_OPEN])

        # ── Previous day bars
        if needs_prev:
            prev_bar = get_prev_day_bar(ticker, trade_date)
            if prev_bar:
                if ws.cell(row=row_idx, column=COL_PREV_HIGH).value is None:
                    ws.cell(row=row_idx, column=COL_PREV_HIGH).value = round(prev_bar["h"], 4)
                    filled_count += 1
                if ws.cell(row=row_idx, column=COL_PREV_LOW).value is None:
                    ws.cell(row=row_idx, column=COL_PREV_LOW).value = round(prev_bar["l"], 4)
                    filled_count += 1
                if ws.cell(row=row_idx, column=COL_PREV_CLOSE).value is None:
                    ws.cell(row=row_idx, column=COL_PREV_CLOSE).value = round(prev_bar["c"], 4)
                    filled_count += 1
                print(f"    ✓ Prev day: H={prev_bar['h']} L={prev_bar['l']} C={prev_bar['c']}")
            else:
                print(f"    ⚠ No previous day bar found for {ticker}")

        # ── Pre-market + market open bars
        if needs_pm:
            pm = get_premarket_data(ticker, trade_date)
            if ws.cell(row=row_idx, column=COL_PM_OPEN).value is None and pm["pm_open"]:
                ws.cell(row=row_idx, column=COL_PM_OPEN).value = pm["pm_open"]
                filled_count += 1
            if ws.cell(row=row_idx, column=COL_PM_HIGH).value is None and pm["pm_high"]:
                ws.cell(row=row_idx, column=COL_PM_HIGH).value = pm["pm_high"]
                filled_count += 1
            if ws.cell(row=row_idx, column=COL_PM_LOW).value is None and pm["pm_low"]:
                ws.cell(row=row_idx, column=COL_PM_LOW).value = pm["pm_low"]
                filled_count += 1
            if ws.cell(row=row_idx, column=COL_MKT_OPEN).value is None and pm["mkt_open"]:
                ws.cell(row=row_idx, column=COL_MKT_OPEN).value = pm["mkt_open"]
                filled_count += 1
            print(f"    ✓ Pre-mkt: open={pm['pm_open']} H={pm['pm_high']} L={pm['pm_low']} | Mkt open={pm['mkt_open']}")

        # Be polite to Alpaca — small delay between rows
        time.sleep(0.3)

    # Save
    wb.save(filepath)
    print(f"\n✓ Done. {filled_count} cells filled. Saved to: {filepath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill blank Pre-Trade Data cells from Alpaca")
    parser.add_argument(
        "--file",
        default=DEFAULT_FILE,
        help=f"Path to the tracker Excel file (default: {DEFAULT_FILE})"
    )
    args = parser.parse_args()
    main(args.file)
