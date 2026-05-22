"""
Trade Analyzer - Benjamin's Stop Loss Analysis
=============================================
Pulls 1-minute intraday data from Alpaca for each trade,
analyzes the 9:44-10:01 AM NY window low vs actual exit,
and finds the daily high + time from market open.

Deploy on Render.com as a one-off job or cron job.
Requires environment variables:
  ALPACA_API_KEY
  ALPACA_API_SECRET

Output: Render logs + trade_analysis.csv
"""

import os
import csv
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
import pytz

# ── Config ────────────────────────────────────────────────────────────────────

API_KEY    = os.environ.get("ALPACA_API_KEY", "")
API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
DATA_URL   = "https://data.alpaca.markets"
NY_TZ      = pytz.timezone("America/New_York")

# ── Your Trade Log ─────────────────────────────────────────────────────────────
# Exit times already converted to NY time (Perth - 12 hours)
# Only including trades exited at or before ~10:40 AM NY — the ones most
# affected by early stop placement. Extend list as needed.

TRADES = [
    # Date        Ticker   Buy $     Sell $   Exit NY    Result
    # ── May 5 ──
    ("2026-05-05", "ALMU",  25.75,   24.86,  "09:59",   "Loss"),
    ("2026-05-05", "BCG",    2.44,    2.27,  "10:17",   "Loss"),
    ("2026-05-05", "CLRB",   3.03,    2.93,  "10:01",   "Loss"),
    ("2026-05-05", "CUE",   37.90,   36.01,  "09:57",   "Loss"),
    ("2026-05-05", "TRT",   12.78,   12.10,  "10:32",   "Loss"),
    ("2026-05-05", "UCTT",  84.69,   81.50,  "10:07",   "Loss"),
    # ── May 6 ──
    ("2026-05-06", "AOSL",  38.51,   37.24,  "10:20",   "Loss"),
    ("2026-05-06", "BKSY",  31.89,   31.37,  "09:50",   "Loss"),
    ("2026-05-06", "GCTK",   0.869,   0.840, "10:31",   "Loss"),
    ("2026-05-06", "SNBR",   3.11,    2.96,  "10:10",   "Loss"),
    ("2026-05-06", "VSEC", 208.37,  202.87,  "10:27",   "Loss"),
    # ── May 7 ──
    ("2026-05-07", "AMZE",   0.139,   0.134, "10:04",   "Loss"),
    ("2026-05-07", "ANY",    1.98,    1.90,  "10:35",   "Loss"),
    ("2026-05-07", "FABC",   4.60,    4.39,  "10:00",   "Loss"),
    ("2026-05-07", "OSRH",   0.626,   0.611, "09:56",   "Loss"),
    ("2026-05-07", "SOBR",   1.77,    1.685, "09:58",   "Loss"),
    ("2026-05-07", "SEZL",  96.95,   93.50,  "10:37",   "Loss"),
    # ── May 11 ──
    ("2026-05-11", "MRAM",  34.81,   34.60,  "10:00",   "Loss"),
    # ── May 12 ──
    ("2026-05-12", "PACS",  40.95,   37.82,  "10:37",   "Loss"),
    # ── May 13 ──
    ("2026-05-13", "DXYZ",  55.43,   52.88,  "10:09",   "Loss"),
    ("2026-05-13", "MRAM",  45.76,   44.03,  "10:09",   "Loss"),
    ("2026-05-13", "SNAL",   0.560,   0.522, "10:04",   "Loss"),
    ("2026-05-13", "WOLF",  68.08,   63.61,  "09:59",   "Loss"),
    # ── May 14 ──
    ("2026-05-14", "AIIO",   4.138,   3.943, "09:51",   "Loss"),
    ("2026-05-14", "BNKK",   2.74,    2.408, "10:03",   "Loss"),
    ("2026-05-14", "GTBP",   0.391,   0.374, "09:56",   "Loss"),
    ("2026-05-14", "IPST",   6.75,    6.56,  "09:49",   "Loss"),
    ("2026-05-14", "SNAL",   1.38,    1.23,  "09:50",   "Loss"),
    # ── May 15 ──
    ("2026-05-15", "BRUN",  27.63,   27.05,  "09:50",   "Loss"),
    ("2026-05-15", "GEMI",   6.66,    6.343, "09:59",   "Loss"),
    ("2026-05-15", "HCWB",   1.290,   1.09,  "10:12",   "Loss"),
    ("2026-05-15", "MRNO",   0.49,    0.449, "10:13",   "Loss"),
    # ── May 18 ──
    ("2026-05-18", "DXYZ",  53.40,   50.22,  "10:34",   "Loss"),
    ("2026-05-18", "MNTS",   5.85,    5.55,  "10:19",   "Loss"),
    ("2026-05-18", "QUCY",   3.87,    3.82,  "09:48",   "Loss"),
    # ── May 19 ──
    ("2026-05-19", "AMST",   2.27,    2.21,  "09:47",   "Loss"),
    ("2026-05-19", "MGN",    0.186,   0.173, "10:12",   "Loss"),
    ("2026-05-19", "VRAX",   0.315,   0.303, "09:52",   "Loss"),
    # ── May 20 ──
    ("2026-05-20", "EDSA",  11.44,   10.88,  "10:26",   "Loss"),
    ("2026-05-20", "SLXN",   0.589,   0.561, "09:52",   "Loss"),
    ("2026-05-20", "TDIC",   0.640,   0.610, "09:52",   "Loss"),
    # ── May 21 ──
    ("2026-05-21", "CODX",   3.27,    3.18,  "09:49",   "Loss"),
    ("2026-05-21", "QUCY",   4.07,    3.881, "10:12",   "Loss"),
    ("2026-05-21", "SBFM",   0.516,   0.488, "09:48",   "Loss"),
    ("2026-05-21", "VIDA",   4.28,    4.09,  "09:55",   "Loss"),
]

# ── Alpaca API Helper ──────────────────────────────────────────────────────────

def fetch_bars(ticker, date_str):
    """Fetch 1-minute bars for a ticker on a given date (market hours only)."""
    # Market open 9:30 AM, close 4:00 PM NY = UTC 13:30-20:00
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
            bars = data.get("bars", [])
            return bars
    except urllib.error.HTTPError as e:
        print(f"  ⚠️  HTTP {e.code} for {ticker} on {date_str}")
        return []
    except Exception as e:
        print(f"  ⚠️  Error fetching {ticker}: {e}")
        return []


def parse_bar_time(t_str):
    """Parse Alpaca bar timestamp to NY datetime."""
    dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    return dt.astimezone(NY_TZ)


# ── Main Analysis ──────────────────────────────────────────────────────────────

def analyze_trade(date_str, ticker, buy_price, sell_price, exit_time_ny, result):
    """
    For one trade, return a dict of analysis fields.
    """
    print(f"\n{'─'*55}")
    print(f"📊 {ticker} | {date_str} | Buy: ${buy_price:.4f} | Exit: {exit_time_ny} NY")

    bars = fetch_bars(ticker, date_str)
    if not bars:
        print(f"  ❌ No bar data available")
        return None

    # Parse all bars into (ny_datetime, open, high, low, close) tuples
    parsed = []
    for b in bars:
        dt = parse_bar_time(b["t"])
        parsed.append({
            "dt":    dt,
            "time":  dt.strftime("%H:%M"),
            "open":  b["o"],
            "high":  b["h"],
            "low":   b["l"],
            "close": b["c"],
        })

    # ── 1. Low of 9:44–10:01 AM window ────────────────────────────────────────
    window = [b for b in parsed if "09:44" <= b["time"] <= "10:01"]
    if window:
        window_low     = min(b["low"] for b in window)
        window_low_bar = min(window, key=lambda b: b["low"])
        window_low_time = window_low_bar["time"]
    else:
        window_low      = None
        window_low_time = "N/A"

    # ── 2. Daily high from open (9:30 AM onward) ──────────────────────────────
    all_bars = [b for b in parsed if b["time"] >= "09:30"]
    if all_bars:
        day_high_bar  = max(all_bars, key=lambda b: b["high"])
        day_high      = day_high_bar["high"]
        day_high_time = day_high_bar["time"]
    else:
        day_high      = None
        day_high_time = "N/A"

    # ── 3. Was actual exit above/below the window low? ────────────────────────
    shaken_out = None
    if window_low:
        shaken_out = sell_price < window_low  # stopped below the window low

    # ── 4. What was price doing after 10:01 AM? ───────────────────────────────
    after_bars = [b for b in parsed if b["time"] > "10:01"]
    high_after_1001      = max((b["high"] for b in after_bars), default=None)
    high_after_1001_time = max(after_bars, key=lambda b: b["high"])["time"] if after_bars else "N/A"

    # ── 5. Would you have been stopped under new rule? ────────────────────────
    # New rule: stop = window_low. Would price have broken it AFTER 10:01?
    broke_window_low_after = False
    first_break_time = "Never"
    if window_low:
        breaks = [b for b in after_bars if b["low"] < window_low]
        if breaks:
            broke_window_low_after = True
            first_break_time = breaks[0]["time"]

    # ── 6. P&L calc ───────────────────────────────────────────────────────────
    actual_pnl_pct = ((sell_price - buy_price) / buy_price) * 100
    if high_after_1001:
        best_case_pct  = ((high_after_1001 - buy_price) / buy_price) * 100
    else:
        best_case_pct  = None

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f"  9:44–10:01 Low : ${window_low:.4f} at {window_low_time}" if window_low else "  9:44–10:01 Low : N/A")
    print(f"  You sold at    : ${sell_price:.4f} at {exit_time_ny} NY")
    print(f"  Shaken out?    : {'⚠️  YES — sold BELOW window low' if shaken_out else '✅ No — sold above window low'}")
    print(f"  Day High       : ${day_high:.4f} at {day_high_time}" if day_high else "  Day High       : N/A")
    print(f"  High after 10:01: ${high_after_1001:.4f} at {high_after_1001_time}" if high_after_1001 else "  High after 10:01: N/A")
    print(f"  New stop broken after 10:01? : {'YES at ' + first_break_time if broke_window_low_after else 'NO — stock held above window low'}")
    print(f"  Actual P&L     : {actual_pnl_pct:+.2f}%")
    if best_case_pct:
        print(f"  Best case P&L  : {best_case_pct:+.2f}% (if held to day high)")

    return {
        "Date":               date_str,
        "Ticker":             ticker,
        "Buy Price":          round(buy_price, 4),
        "Sell Price":         round(sell_price, 4),
        "Exit Time NY":       exit_time_ny,
        "Result":             result,
        "Window Low (9:44-10:01)": round(window_low, 4) if window_low else "",
        "Window Low Time":    window_low_time,
        "Sold Below Window Low": "YES" if shaken_out else "NO",
        "Day High":           round(day_high, 4) if day_high else "",
        "Day High Time":      day_high_time,
        "High After 10:01":   round(high_after_1001, 4) if high_after_1001 else "",
        "High After 10:01 Time": high_after_1001_time,
        "New Stop Broken After 10:01": "YES" if broke_window_low_after else "NO",
        "First Break Time":   first_break_time,
        "Actual PnL %":       round(actual_pnl_pct, 2),
        "Best Case PnL %":    round(best_case_pct, 2) if best_case_pct else "",
    }


def main():
    print("=" * 55)
    print("  TRADE STOP LOSS ANALYSIS — Benjamin's Bot")
    print("  Alpaca 1-Min Data | NY Time")
    print("=" * 55)

    if not API_KEY or not API_SECRET:
        print("❌ ERROR: ALPACA_API_KEY and ALPACA_API_SECRET environment variables not set!")
        return

    results = []
    for trade in TRADES:
        row = analyze_trade(*trade)
        if row:
            results.append(row)
        time.sleep(0.3)  # be kind to the API

    # ── Write CSV ──────────────────────────────────────────────────────────────
    if results:
        csv_path = "trade_analysis.csv"
        fieldnames = list(results[0].keys())
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ CSV saved to {csv_path}")

    # ── Summary Stats ──────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  SUMMARY")
    print("=" * 55)

    shaken_count = sum(1 for r in results if r["Sold Below Window Low"] == "YES")
    held_would_win = sum(
        1 for r in results
        if r["Sold Below Window Low"] == "YES"
        and r["New Stop Broken After 10:01"] == "NO"
        and r["High After 10:01"] != ""
        and float(r["High After 10:01"]) > float(r["Buy Price"])
    )

    print(f"  Total early exits analyzed : {len(results)}")
    print(f"  Shaken out below window low: {shaken_count}")
    print(f"  Of those — stock recovered : {held_would_win} (potential saved losses)")
    print(f"  New stop would also stop   : {shaken_count - held_would_win}")
    print("=" * 55)


if __name__ == "__main__":
    main()
