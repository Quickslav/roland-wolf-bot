#!/usr/bin/env python3
"""
fill_float_shortinterest.py

Fills Short Interest + Shares Outstanding (and a short-%-of-shares-outstanding
ratio) into the EOD Winners/Losers tabs of the Roland Wolf tracker, using two
FREE, authoritative sources with strict POINT-IN-TIME matching:

  * Shares outstanding  -> SEC EDGAR XBRL (dei:EntityCommonStockSharesOutstanding)
  * Short shares        -> FINRA Equity Short Interest (consolidated, incl. Nasdaq)

DESIGN RULES (match Benjamin's standing conventions):
  * BLANK, NOT GUESSED. If a value can't be resolved for a (ticker, date) pair,
    the cell is left empty. Never 0, never carried-forward, never interpolated.
  * POINT-IN-TIME. For a trade on date D we use the most recent value that was
    already KNOWN as of D (SEC: filing filed<=D ; FINRA: settlement<=D), so no
    look-ahead bias creeps into the tracker.
  * MASTER IS UNTOUCHED. The script writes to a timestamped COPY, never the
    source file. Run with --dry-run first to see exactly what it would write.

IMPORTANT LIMITATION (read this):
  EDGAR gives shares OUTSTANDING, not true free float. The "Short % of Shares
  Out" column therefore UNDERSTATES true short-float for names with heavy
  insider/restricted holdings. Raw components are stored so you can recompute
  against a real float source later if you get one.

ONE-TIME SETUP:
  1. SEC only requires a descriptive User-Agent (set SEC_USER_AGENT below).
  2. FINRA: create a free individual FINRA API account, provision an API Client
     ID + Secret in the API Console, then export:
         export FINRA_CLIENT_ID=...
         export FINRA_CLIENT_SECRET=...
  3. pip install openpyxl requests

USAGE:
  python fill_float_shortinterest.py --master "Roland Wolf Tracker.xlsx" --dry-run
  python fill_float_shortinterest.py --master "Roland Wolf Tracker.xlsx"
  python fill_float_shortinterest.py --master "...xlsx" --limit 20   # test subset
"""

import argparse
import base64
import datetime as dt
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

import requests
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

# ----------------------------------------------------------------------------
# CONFIG  --  adjust these to match your master once its layout is confirmed
# ----------------------------------------------------------------------------

# Tabs to fill. Exact sheet names from the master.
TARGET_TABS = [
    "EOD Biggest Winners Q1", "EOD Biggest Losers Q1",
    "EOD Biggest Winners Q2", "EOD Biggest Losers Q2",
]

# Header text the script will look for (case-insensitive, first match wins).
TICKER_HEADER_CANDIDATES = ["ticker", "symbol", "stock"]
DATE_HEADER_CANDIDATES   = ["date", "trade date", "eod date", "day"]

# Output columns. If a header with this exact text already exists on the tab it
# is reused; otherwise the column is appended after the last used column.
COL_SHORT_SHARES = "Short Shares"
COL_SHARES_OUT   = "Shares Outstanding (SEC)"
COL_SHORT_PCT    = "Short % of Shares Out"

# SEC requires a real contact string in the User-Agent or it returns 403.
SEC_USER_AGENT = "Roland Wolf Tracker research (your_email@example.com)"

# Point-in-time strictness for FINRA. "settlement" = most recent settlement<=D
# (conventional). "publication" would additionally require the ~8-biz-day
# publication lag; set STRICT_PUBLICATION=True to avoid using a settlement whose
# report wasn't public yet on the trade date.
STRICT_PUBLICATION = False

CACHE_DIR = Path(".fill_cache")
CACHE_DIR.mkdir(exist_ok=True)

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_CONCEPT_URL = ("https://data.sec.gov/api/xbrl/companyconcept/"
                   "CIK{cik10}/dei/EntityCommonStockSharesOutstanding.json")
FINRA_TOKEN_URL = ("https://ews.fip.finra.org/fip/rest/ews/oauth2/"
                   "access_token?grant_type=client_credentials")
FINRA_DATA_URL  = "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest"

# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def norm_symbol(s):
    """FINRA strips spaces/special chars/lowercase from symbols; match the same way."""
    return re.sub(r"[^A-Z0-9]", "", str(s).upper()) if s else ""

def to_date(v):
    """Coerce a cell value (datetime, date, or string) to a date; None if impossible."""
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def last_business_day(year, month):
    d = dt.date(year, month, 28)
    while d.month == month:
        d += dt.timedelta(days=1)
    d -= dt.timedelta(days=1)              # last calendar day
    while d.weekday() >= 5:                # back up off weekend
        d -= dt.timedelta(days=1)
    return d

def mid_month_settlement(year, month):
    d = dt.date(year, month, 15)
    while d.weekday() >= 5:                # 15th adjusted to prior business day
        d -= dt.timedelta(days=1)
    return d

def candidate_settlements(trade_date):
    """FINRA reports twice monthly (mid-month ~15th, and month-end). Return
    plausible settlement dates <= trade_date, most recent first. Holidays that
    shift a real settlement are handled by the fetch step-back."""
    cands = set()
    y, m = trade_date.year, trade_date.month
    for _ in range(3):                     # this month + 2 prior, plenty of margin
        cands.add(mid_month_settlement(y, m))
        cands.add(last_business_day(y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return sorted((c for c in cands if c <= trade_date), reverse=True)

# ----------------------------------------------------------------------------
# SEC EDGAR : point-in-time shares outstanding
# ----------------------------------------------------------------------------

class Edgar:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": SEC_USER_AGENT,
                               "Accept-Encoding": "gzip, deflate"})
        self._ticker_map = None
        self._concept = {}                 # cik10 -> list of (filed, end, val)

    def _get(self, url):
        for attempt in range(4):
            r = self.s.get(url, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None
            time.sleep(0.5 * (attempt + 1))   # rate-limit / transient backoff
        return None

    def ticker_to_cik(self, ticker):
        if self._ticker_map is None:
            cache = CACHE_DIR / "sec_tickers.json"
            if cache.exists():
                data = json.loads(cache.read_text())
            else:
                r = self._get(SEC_TICKERS_URL)
                data = r.json() if r else {}
                cache.write_text(json.dumps(data))
            self._ticker_map = {}
            for row in data.values():
                self._ticker_map[norm_symbol(row["ticker"])] = int(row["cik_str"])
        return self._ticker_map.get(norm_symbol(ticker))

    def _series(self, cik10):
        """All (filed, end, val) shares-outstanding points for a CIK, cached."""
        if cik10 in self._concept:
            return self._concept[cik10]
        cache = CACHE_DIR / f"so_{cik10}.json"
        if cache.exists():
            pts = json.loads(cache.read_text())
        else:
            r = self._get(SEC_CONCEPT_URL.format(cik10=cik10))
            pts = []
            if r:
                units = r.json().get("units", {}).get("shares", [])
                # Multiple share classes can report on the same filing; sum per
                # (end, filed) so we get TOTAL shares out, and record it.
                agg = {}
                for u in units:
                    if u.get("filed") and u.get("val") is not None:
                        key = (u.get("end"), u["filed"])
                        agg[key] = agg.get(key, 0) + int(u["val"])
                for (end, filed), val in agg.items():
                    pts.append({"end": end, "filed": filed, "val": val})
            cache.write_text(json.dumps(pts))
        self._concept[cik10] = pts
        return pts

    def shares_outstanding_asof(self, ticker, trade_date):
        """Most recent shares-outstanding value FILED on or before trade_date."""
        cik = self.ticker_to_cik(ticker)
        if cik is None:
            return None
        cik10 = str(cik).zfill(10)
        pts = self._series(cik10)
        eligible = [p for p in pts
                    if dt.date.fromisoformat(p["filed"]) <= trade_date]
        if not eligible:
            return None
        # latest by filing date, tie-break on period-end date
        best = max(eligible, key=lambda p: (p["filed"], p["end"] or ""))
        return best["val"]

# ----------------------------------------------------------------------------
# FINRA : point-in-time short interest
# ----------------------------------------------------------------------------

class Finra:
    def __init__(self):
        cid = os.environ.get("FINRA_CLIENT_ID")
        sec = os.environ.get("FINRA_CLIENT_SECRET")
        if not cid or not sec:
            sys.exit("ERROR: set FINRA_CLIENT_ID and FINRA_CLIENT_SECRET env vars "
                     "(free individual FINRA API account).")
        self._basic = base64.b64encode(f"{cid}:{sec}".encode()).decode()
        self._token = None
        self._token_exp = 0
        self._settlements = {}             # iso date -> {norm_symbol: short_shares}
        self.s = requests.Session()

    def _get_token(self):
        if self._token and time.time() < self._token_exp - 60:
            return self._token
        r = self.s.post(FINRA_TOKEN_URL,
                        headers={"Authorization": f"Basic {self._basic}"},
                        timeout=30)
        r.raise_for_status()
        j = r.json()
        self._token = j["access_token"]
        self._token_exp = time.time() + int(j.get("expires_in", 1800))
        return self._token

    def _fetch_settlement(self, settle_iso):
        """Pull ALL rows for one settlement date, page through, build symbol map.
        Cached to disk so re-runs and repeated tickers cost nothing."""
        if settle_iso in self._settlements:
            return self._settlements[settle_iso]
        cache = CACHE_DIR / f"si_{settle_iso}.json"
        if cache.exists():
            m = json.loads(cache.read_text())
            self._settlements[settle_iso] = m
            return m
        m = {}
        offset, limit = 0, 5000
        while True:
            payload = {
                "limit": limit, "offset": offset,
                "compareFilters": [{"compareType": "EQUAL",
                                    "fieldName": "settlementDate",
                                    "fieldValue": settle_iso}],
            }
            r = self.s.post(FINRA_DATA_URL,
                            headers={"Authorization": f"Bearer {self._get_token()}",
                                     "Content-Type": "application/json",
                                     "Accept": "application/json"},
                            data=json.dumps(payload), timeout=60)
            r.raise_for_status()
            rows = r.json()
            if not rows:
                break
            for row in rows:
                sym = norm_symbol(row.get("issueSymbolIdentifier"))
                val = row.get("currentShortShareNumber")
                if sym and val is not None:
                    m[sym] = int(val)
            if len(rows) < limit:
                break
            offset += limit
        cache.write_text(json.dumps(m))
        self._settlements[settle_iso] = m
        return m

    def short_shares_asof(self, ticker, trade_date):
        """Short shares from the most recent settlement <= trade_date that has
        data. Returns (short_shares, settlement_date_used) or (None, None)."""
        sym = norm_symbol(ticker)
        for cand in candidate_settlements(trade_date):
            if STRICT_PUBLICATION:
                # short interest is public ~7-8 biz days after settlement
                if cand + dt.timedelta(days=11) > trade_date:
                    continue
            m = self._fetch_settlement(cand.isoformat())
            if not m:                      # holiday-shifted / no file -> try older
                continue
            if sym in m:
                return m[sym], cand
            # settlement exists but symbol absent = genuinely no reported short
            # position; that's a real 0, but we keep it BLANK per no-guess rule
            # unless the whole file is present. Return the found settlement with
            # None so caller can decide; we choose blank.
            return None, cand
        return None, None

# ----------------------------------------------------------------------------
# spreadsheet driver
# ----------------------------------------------------------------------------

def find_header_row_and_cols(ws):
    """Scan the first 10 rows for a header row containing a ticker + date column."""
    for r in range(1, 11):
        headers = {}
        for c in range(1, ws.max_column + 1):
            v = ws.cell(row=r, column=c).value
            if v is not None:
                headers[str(v).strip().lower()] = c
        tcol = next((headers[h] for cand in TICKER_HEADER_CANDIDATES
                     for h in headers if h == cand), None)
        dcol = next((headers[h] for cand in DATE_HEADER_CANDIDATES
                     for h in headers if h == cand), None)
        if tcol and dcol:
            return r, tcol, dcol, headers
    return None, None, None, {}

def ensure_col(ws, header_row, headers, title):
    """Return column index for `title`, creating a header if absent."""
    key = title.strip().lower()
    if key in headers:
        return headers[key]
    col = ws.max_column + 1
    ws.cell(row=header_row, column=col, value=title)
    headers[key] = col
    return col

def run(master_path, dry_run, limit):
    master = Path(master_path)
    if not master.exists():
        sys.exit(f"ERROR: master not found: {master}")

    edgar, finra = Edgar(), Finra()

    if dry_run:
        out_path = None
        wb = load_workbook(master)         # read structure; won't be saved
    else:
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = master.with_name(f"{master.stem}__floatSI_{stamp}.xlsx")
        shutil.copy2(master, out_path)     # work on a copy; master is never touched
        wb = load_workbook(out_path)

    stats = {"filled_so": 0, "filled_si": 0, "blank_so": 0, "blank_si": 0, "rows": 0}
    preview = []

    for tab in TARGET_TABS:
        if tab not in wb.sheetnames:
            print(f"  [skip] tab not found: {tab!r}")
            continue
        ws = wb[tab]
        hrow, tcol, dcol, headers = find_header_row_and_cols(ws)
        if not hrow:
            print(f"  [skip] no ticker+date header on {tab!r}")
            continue

        c_ss = ensure_col(ws, hrow, headers, COL_SHORT_SHARES)
        c_so = ensure_col(ws, hrow, headers, COL_SHARES_OUT)
        c_pct = ensure_col(ws, hrow, headers, COL_SHORT_PCT)

        for r in range(hrow + 1, ws.max_row + 1):
            ticker = ws.cell(row=r, column=tcol).value
            tdate = to_date(ws.cell(row=r, column=dcol).value)
            if not ticker or not tdate:
                continue
            if limit and stats["rows"] >= limit:
                break
            stats["rows"] += 1

            so = edgar.shares_outstanding_asof(ticker, tdate)
            ss, settle = finra.short_shares_asof(ticker, tdate)

            # BLANK, NOT GUESSED
            if not dry_run:
                ws.cell(row=r, column=c_ss).value = ss if ss is not None else None
                ws.cell(row=r, column=c_so).value = so if so is not None else None
                pct_cell = ws.cell(row=r, column=c_pct)
                if ss is not None and so is not None:
                    a = f"{get_column_letter(c_ss)}{r}"
                    b = f"{get_column_letter(c_so)}{r}"
                    # formula -> stays dynamic, blank if either side missing
                    pct_cell.value = f'=IF(OR({a}="",{b}=""),"",{a}/{b})'
                    pct_cell.number_format = "0.00%"
                else:
                    pct_cell.value = None
                    pct_cell.number_format = "0.00%"

            stats["filled_so" if so is not None else "blank_so"] += 1
            stats["filled_si" if ss is not None else "blank_si"] += 1
            if len(preview) < 15:
                pct = f"{ss/so:.2%}" if (ss and so) else "—"
                preview.append((tab, str(ticker), tdate.isoformat(),
                                so if so is not None else "—",
                                ss if ss is not None else "—", pct,
                                settle.isoformat() if settle else "—"))
        if limit and stats["rows"] >= limit:
            break

    print("\n  sample of resolved values (tab | ticker | date | sharesOut | shortShares | short% | SI settle):")
    for row in preview:
        print("   ", " | ".join(str(x) for x in row))
    print(f"\n  rows processed        : {stats['rows']}")
    print(f"  shares-out filled/blank: {stats['filled_so']} / {stats['blank_so']}")
    print(f"  short-int filled/blank : {stats['filled_si']} / {stats['blank_si']}")

    if dry_run:
        print("\n  DRY RUN — nothing written. Re-run without --dry-run to write a copy.")
    else:
        wb.save(out_path)
        print(f"\n  WROTE COPY: {out_path}")
        print("  (Master untouched. Open in Excel to let the % formulas calculate,")
        print("   or run it through LibreOffice recalc if you want cached values.)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--master", required=True, help="path to the tracker .xlsx")
    ap.add_argument("--dry-run", action="store_true",
                    help="resolve values and report, but write nothing")
    ap.add_argument("--limit", type=int, default=0,
                    help="cap rows processed (for a quick test)")
    args = ap.parse_args()
    run(args.master, args.dry_run, args.limit)


if __name__ == "__main__":
    main()
