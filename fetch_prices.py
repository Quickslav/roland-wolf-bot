import requests
import openpyxl
from openpyxl.styles import Font, Border, Side
from datetime import datetime, date

KEY    = "PKIYGXQT3DGX7B6BDIZFZ6VQWU"
SECRET = "Ekaz5bQHUbbFUQvmidbBSU89wHMtgigik3TsyFD15NA3"

SPREADSHEET_PATH = "merged_trading_tracker__3_.xlsx"  # adjust path if needed

HEADERS = {
    "APCA-API-KEY-ID":     KEY,
    "APCA-API-SECRET-KEY": SECRET,
}

# ── All rows needing H/L/C — original list + new June 3/4/5 rows ─────
ROWS = [
   
    # ── New: June 3 ───────────────────────────────────────────────────
    ["2026-06-03","XOS"],["2026-06-03","LASE"],["2026-06-03","PMI"],["2026-06-03","SVCO"],
    ["2026-06-03","SINT"],["2026-06-03","NEOV"],["2026-06-03","LFVN"],["2026-06-03","CNTB"],
    ["2026-06-03","BRUN"],["2026-06-03","BNKK"],["2026-06-03","APVO"],["2026-06-03","AIRJ"],
    ["2026-06-03","SOAR"],["2026-06-03","SDOT"],
    # ── New: June 4 ───────────────────────────────────────────────────
    ["2026-06-04","BNKK"],["2026-06-04","GENK"],["2026-06-04","TWAV"],["2026-06-04","SBEV"],
    ["2026-06-04","ONFO"],["2026-06-04","TLYS"],["2026-06-04","MOBX"],["2026-06-04","ROLR"],
    ["2026-06-04","LGIH"],["2026-06-04","CAL"],
    # ── New: June 5 ───────────────────────────────────────────────────
    ["2026-06-05","MRLN"],["2026-06-05","RMSG"],["2026-06-05","BKSY"],["2026-06-05","MCRB"],
    ["2026-06-05","MNTS"],["2026-06-05","STI"],["2026-06-05","DEVS"],
]

# ── Fetch all bars in one request ────────────────────────────────────
tickers = list(set(r[1] for r in ROWS))
dates   = [r[0] for r in ROWS]
start   = min(dates)
end     = max(dates)

print(f"Fetching {len(tickers)} tickers from {start} to {end} ...")

resp = requests.get(
    "https://data.alpaca.markets/v2/stocks/bars",
    headers=HEADERS,
    params={
        "symbols":    ",".join(tickers),
        "timeframe":  "1Day",
        "start":      start,
        "end":        end,
        "limit":      10000,
        "feed":       "iex",
        "adjustment": "raw",
    }
)

bars = resp.json().get("bars", {})
data = {}
for ticker, bar_list in bars.items():
    for bar in bar_list:
        key = ticker + "|" + bar["t"][:10]
        data[key] = {"h": bar["h"], "l": bar["l"], "c": bar["c"]}

print(f"Fetched {len(data)} bar records.\n")

# ── Write into spreadsheet ────────────────────────────────────────────
wb  = openpyxl.load_workbook(SPREADSHEET_PATH)
ws  = wb["Pre-Trade Data"]

thin   = Side(style="thin")
border = Border(top=thin, bottom=thin, left=thin, right=thin)
font   = Font(name="Calibri", size=11)

# Build lookup: (ticker, date_str) -> row cells
def to_date_str(val):
    if isinstance(val, datetime): return val.strftime("%Y-%m-%d")
    if isinstance(val, date):     return val.strftime("%Y-%m-%d")
    if isinstance(val, str):
        for fmt in ("%B %d, %Y", "%Y-%m-%d"):
            try: return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except: pass
    return None

updated = 0
not_found = []

for row in ws.iter_rows(min_row=3, values_only=False):
    ticker = row[1].value
    if not ticker:
        continue

    # Skip if all three already filled
    if row[16].value is not None and row[17].value is not None and row[18].value is not None:
        continue

    date_str = to_date_str(row[0].value)
    if not date_str:
        continue

    key = f"{ticker}|{date_str}"
    if key not in data:
        not_found.append(f"{date_str} {ticker}")
        continue

    d = data[key]
    for cell, val in [(row[16], d["h"]), (row[17], d["l"]), (row[18], d["c"])]:
        cell.value         = round(val, 4)
        cell.number_format = '"$"#,##0.00'
        cell.font          = font
        cell.border        = border

    print(f"  ✅ {date_str} {ticker:6s}  H={d['h']}  L={d['l']}  C={d['c']}")
    updated += 1

wb.save(SPREADSHEET_PATH)

print(f"\n✅ Done — {updated} rows updated.")
if not_found:
    print(f"⚠  No data found for {len(not_found)} rows:")
    for x in not_found:
        print(f"   {x}")
