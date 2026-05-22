"""
Winner Trade Analyzer
Pulls 1-minute Alpaca data for all winning trades
to compare characteristics vs losing trades
"""

import os
import csv
import json
import time
import urllib.request
import urllib.error
from datetime import datetime
import pytz

API_KEY    = os.environ.get("ALPACA_API_KEY", "")
API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
DATA_URL   = "https://data.alpaca.markets"
NY_TZ      = pytz.timezone("America/New_York")

# All winning trades from Benjamin's log
WINNERS = [
    # Date         Ticker   Buy $     Sell $    Exit NY   PnL $
    ("2026-05-05", "AVTX",  18.41,   21.73,   "11:28",  1802.76),
    ("2026-05-05", "CLYM",  11.72,   11.92,   "11:28",   166.20),
    ("2026-05-05", "HNRG",  18.19,   18.45,   "11:28",   143.26),
    ("2026-05-05", "PENG",  37.14,   39.10,   "11:28",   527.24),
    ("2026-05-05", "SDOT",   0.3428,  0.3690,  "11:28",   770.26),
    ("2026-05-05", "STRL", 854.00,  866.58,   "11:28",   138.38),
    ("2026-05-06", "ANY",    1.95,    2.05,   "11:28",   515.40),
    ("2026-05-06", "ERNA",   6.49,    7.13,   "11:28",   990.08),
    ("2026-05-06", "GRAL",  62.62,   63.29,   "11:28",   109.21),
    ("2026-05-06", "MRAM",  20.89,   21.66,   "11:28",   369.60),
    ("2026-05-06", "NNE",   26.66,   27.04,   "11:28",   143.64),
    ("2026-05-06", "OSS",   14.77,   14.81,   "11:28",    27.66),
    ("2026-05-06", "SST",    4.04,    4.10,   "11:28",   133.32),
    ("2026-05-07", "ARQ",    2.56,    2.73,   "11:28",   665.21),
    ("2026-05-07", "BOBS",  13.05,   13.10,   "11:28",    38.30),
    ("2026-05-07", "DXYZ",  48.26,   52.52,   "11:28",   894.93),
    ("2026-05-07", "NSP",   31.84,   32.31,   "11:28",   148.05),
    ("2026-05-07", "WGS",   38.69,   40.81,   "11:28",   553.32),
    ("2026-05-11", "BNAI",  22.54,   24.92,   "11:28",  1080.52),
    ("2026-05-11", "CEVA",  35.40,   35.77,   "11:28",   105.08),
    ("2026-05-11", "DXYZ",  61.97,   65.30,   "11:28",   545.46),
    ("2026-05-11", "GSIT",  10.83,   11.83,   "11:28",   903.00),
    ("2026-05-11", "PRSO",   1.19,    1.47,   "11:22",  2413.60),
    ("2026-05-11", "WYFI",  25.82,   28.12,   "11:28",   901.60),
    ("2026-05-12", "SIBN",  13.12,   13.34,   "11:28",   169.18),
    ("2026-05-12", "ZBRA", 249.53,  251.65,   "11:28",    84.80),
    ("2026-05-13", "MNTS",   5.45,    5.59,   "11:28",   266.14),
    ("2026-05-13", "VELO",  17.83,   20.32,   "11:28",  1409.16),
    ("2026-05-14", "ALP",    0.376,   0.383,  "10:03",   177.42),
    ("2026-05-14", "EDBL",   0.430,   0.434,  "11:28",   100.00),
    ("2026-05-14", "LESL",   1.90,    2.36,   "10:50",  2408.10),
    ("2026-05-14", "MOBX",   3.62,    3.68,   "09:48",   167.10),
    ("2026-05-14", "STAA",  32.19,   32.68,   "11:28",   152.88),
    ("2026-05-15", "HUBC",   0.187,   0.219,  "10:57",  1689.46),
    ("2026-05-18", "AIIO",   4.41,    5.70,   "10:12",  3006.99),
    ("2026-05-18", "CREG",   0.770,   0.807,  "10:32",   484.36),
    ("2026-05-18", "PMI",    0.1363,  0.2190, "11:20",  6161.94),
    ("2026-05-19", "CODX",   2.329,   2.570,  "09:47",  1054.72),
    ("2026-05-19", "GIPR",   0.356,   0.386,  "09:54",   839.97),
    ("2026-05-19", "WNW",    4.87,    5.030,  "10:04",   330.45),
    ("2026-05-20", "BLNE",   1.16,    1.20,   "02:01",   347.80),
    ("2026-05-20", "LGVN",   0.701,   0.715,  "02:02",   205.94),
    ("2026-05-20", "RVI",   55.00,   58.00,   "02:01",   549.00),
    ("2026-05-20", "TLN",  338.59,  342.81,   "02:01",   122.38),
    ("2026-05-21", "MRAM",  30.38,   32.25,   "02:01",   617.10),
    ("2026-05-21", "RL",   365.00,  377.51,   "02:01",   337.77),
]

def fetch_bars(ticker, date_str):
    start = f"{date_str}T13:30:00Z"
    end   = f"{date_str}T20:00:00Z"
    url = (
        f"{DATA_URL}/v2/stocks/{ticker}/bars"
        f"?timeframe=1Min&start={start}&end={end}&limit=400&feed=iex"
    )
    req = urllib.request.Request(url, headers={
        "APCA-API-KEY-ID":     API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("bars", [])
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return []

def parse_bar_time(t_str):
    dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    return dt.astimezone(NY_TZ)

def analyze_winner(date_str, ticker, buy_price, sell_price, exit_time_ny, pnl):
    print(f"\n{'─'*55}")
    print(f"✅ {ticker} | {date_str} | Buy: ${buy_price:.4f} | Exit: {exit_time_ny} NY | PnL: ${pnl:.2f}")

    bars = fetch_bars(ticker, date_str)
    if not bars:
        print(f"  No data")
        return None

    parsed = []
    for b in bars:
        dt = parse_bar_time(b["t"])
        parsed.append({
            "dt": dt, "time": dt.strftime("%H:%M"),
            "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"],
        })

    # 9:30-9:44 opening range high/low
    open_range = [b for b in parsed if "09:30" <= b["time"] <= "09:44"]
    open_range_high = max((b["high"] for b in open_range), default=None)
    open_range_low  = min((b["low"]  for b in open_range), default=None)

    # 9:44-10:01 window low
    window = [b for b in parsed if "09:44" <= b["time"] <= "10:01"]
    window_low      = min((b["low"] for b in window), default=None)
    window_low_time = min(window, key=lambda b: b["low"])["time"] if window else "N/A"

    # Day high
    all_bars = [b for b in parsed if b["time"] >= "09:30"]
    day_high_bar  = max(all_bars, key=lambda b: b["high"]) if all_bars else None
    day_high      = day_high_bar["high"] if day_high_bar else None
    day_high_time = day_high_bar["time"] if day_high_bar else "N/A"

    # Did price dip to window low before running?
    touched_window_low = False
    if window_low and window:
        # was the low tested at any point in 9:44-10:01?
        min_bar = min(window, key=lambda b: b["low"])
        touched_window_low = min_bar["low"] <= (buy_price * 0.99)  # dipped >1% below entry

    # Trend from open: was stock trending up from 9:30?
    first_bar = parsed[0] if parsed else None
    open_price = first_bar["open"] if first_bar else None

    # How far from entry to day high
    if day_high:
        upside_from_entry = ((day_high - buy_price) / buy_price) * 100
    else:
        upside_from_entry = None

    pnl_pct = ((sell_price - buy_price) / buy_price) * 100

    # Time from open to day high
    if day_high_time:
        h, m = map(int, day_high_time.split(":"))
        mins_to_high = (h * 60 + m) - (9 * 60 + 30)
    else:
        mins_to_high = None

    print(f"  Open price      : ${open_price:.4f}" if open_price else "  Open price: N/A")
    print(f"  Entry vs open   : {((buy_price - open_price)/open_price*100):+.2f}%" if open_price else "")
    print(f"  Window low      : ${window_low:.4f} at {window_low_time}" if window_low else "  Window low: N/A")
    print(f"  Dipped >1% below entry before running? : {'YES' if touched_window_low else 'NO'}")
    print(f"  Day High        : ${day_high:.4f} at {day_high_time}" if day_high else "  Day High: N/A")
    print(f"  Upside from entry: {upside_from_entry:+.2f}%" if upside_from_entry else "")
    print(f"  Actual P&L      : {pnl_pct:+.2f}%")
    print(f"  Left on table   : {(upside_from_entry - pnl_pct):+.2f}%" if upside_from_entry else "")

    return {
        "Date": date_str,
        "Ticker": ticker,
        "Buy Price": round(buy_price, 4),
        "Sell Price": round(sell_price, 4),
        "Exit Time NY": exit_time_ny,
        "PnL $": round(pnl, 2),
        "PnL %": round(pnl_pct, 2),
        "Open Price": round(open_price, 4) if open_price else "",
        "Entry vs Open %": round(((buy_price - open_price)/open_price*100), 2) if open_price else "",
        "Window Low (9:44-10:01)": round(window_low, 4) if window_low else "",
        "Window Low Time": window_low_time,
        "Dipped Below Entry Before Run": "YES" if touched_window_low else "NO",
        "Day High": round(day_high, 4) if day_high else "",
        "Day High Time": day_high_time,
        "Upside from Entry %": round(upside_from_entry, 2) if upside_from_entry else "",
        "Left on Table %": round((upside_from_entry - pnl_pct), 2) if upside_from_entry else "",
        "Mins to Day High from Open": mins_to_high,
    }

def main():
    print("=" * 55)
    print("  WINNER TRADE ANALYSIS — Benjamin's Bot")
    print("=" * 55)

    results = []
    for trade in WINNERS:
        row = analyze_winner(*trade)
        if row:
            results.append(row)
        time.sleep(0.3)

    if results:
        with open("winner_analysis.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ CSV saved to winner_analysis.csv")

    # Summary stats
    pnl_pcts     = [r["PnL %"] for r in results if r["PnL %"]]
    upsides      = [r["Upside from Entry %"] for r in results if r["Upside from Entry %"]]
    left_on_table = [r["Left on Table %"] for r in results if r["Left on Table %"]]
    dipped       = sum(1 for r in results if r["Dipped Below Entry Before Run"] == "YES")
    mins_to_high = [r["Mins to Day High from Open"] for r in results if r["Mins to Day High from Open"]]

    print("\n" + "=" * 55)
    print("  WINNER SUMMARY STATS")
    print("=" * 55)
    print(f"  Total winners analyzed    : {len(results)}")
    print(f"  Avg actual PnL %          : {sum(pnl_pcts)/len(pnl_pcts):+.2f}%")
    print(f"  Avg upside from entry     : {sum(upsides)/len(upsides):+.2f}%")
    print(f"  Avg left on table         : {sum(left_on_table)/len(left_on_table):+.2f}%")
    print(f"  Dipped >1% before running : {dipped} of {len(results)}")
    print(f"  Avg mins to day high      : {sum(mins_to_high)/len(mins_to_high):.0f} mins after open")
    print("=" * 55)

if __name__ == "__main__":
    main()
