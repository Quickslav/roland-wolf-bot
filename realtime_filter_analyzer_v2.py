"""
Real-Time Filter Analyzer v2
=================
Only uses information available AT 9:44 AM — no hindsight.
Fixed: robust error handling for None returns from API
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

ALL_TRADES = [
    ("2026-05-05", "ALMU",   25.75,    24.86,   "Loss"),
    ("2026-05-05", "AVTX",   18.41,    21.73,   "Win"),
    ("2026-05-05", "BCG",     2.44,     2.27,   "Loss"),
    ("2026-05-05", "CLRB",    3.03,     2.93,   "Loss"),
    ("2026-05-05", "CLYM",   11.72,    11.92,   "Win"),
    ("2026-05-05", "CUE",    37.90,    36.01,   "Loss"),
    ("2026-05-05", "HNRG",   18.19,    18.45,   "Win"),
    ("2026-05-05", "PENG",   37.14,    39.10,   "Win"),
    ("2026-05-05", "SDOT",    0.3428,   0.3690,  "Win"),
    ("2026-05-05", "STRL",  854.00,   866.58,   "Win"),
    ("2026-05-05", "THR",    69.59,    69.18,   "Loss"),
    ("2026-05-05", "TRT",    12.78,    12.10,   "Loss"),
    ("2026-05-05", "TYGO",    4.87,     4.60,   "Loss"),
    ("2026-05-05", "UCTT",   84.69,    81.50,   "Loss"),
    ("2026-05-05", "VVX",    76.12,    75.68,   "Loss"),
    ("2026-05-06", "ACLS",  162.78,   160.06,   "Loss"),
    ("2026-05-06", "ANY",     1.95,     2.05,   "Win"),
    ("2026-05-06", "AOSL",   38.51,    37.24,   "Loss"),
    ("2026-05-06", "AVTX",   23.11,    22.45,   "Loss"),
    ("2026-05-06", "BKSY",   31.89,    31.37,   "Loss"),
    ("2026-05-06", "ERNA",    6.49,     7.13,   "Win"),
    ("2026-05-06", "GCTK",    0.869,    0.840,  "Loss"),
    ("2026-05-06", "GRAL",   62.62,    63.29,   "Win"),
    ("2026-05-06", "MRAM",   20.89,    21.66,   "Win"),
    ("2026-05-06", "NNE",    26.66,    27.04,   "Win"),
    ("2026-05-06", "OSS",    14.77,    14.81,   "Win"),
    ("2026-05-06", "SNBR",    3.11,     2.96,   "Loss"),
    ("2026-05-06", "SST",     4.04,     4.10,   "Win"),
    ("2026-05-06", "VSEC",  208.37,   202.87,   "Loss"),
    ("2026-05-07", "AIM",     0.373,    0.370,  "Loss"),
    ("2026-05-07", "AMZE",    0.139,    0.134,  "Loss"),
    ("2026-05-07", "ANY",     1.98,     1.90,   "Loss"),
    ("2026-05-07", "ARQ",     2.56,     2.73,   "Win"),
    ("2026-05-07", "ATRA",    9.25,     8.40,   "Loss"),
    ("2026-05-07", "BOBS",   13.05,    13.10,   "Win"),
    ("2026-05-07", "DXYZ",   48.26,    52.52,   "Win"),
    ("2026-05-07", "FABC",    4.60,     4.39,   "Loss"),
    ("2026-05-07", "NSP",    31.84,    32.31,   "Win"),
    ("2026-05-07", "OSRH",    0.626,    0.611,  "Loss"),
    ("2026-05-07", "SOBR",    1.77,     1.685,  "Loss"),
    ("2026-05-07", "SEZL",   96.95,    93.50,   "Loss"),
    ("2026-05-07", "WGS",    38.69,    40.81,   "Win"),
    ("2026-05-11", "BNAI",   22.54,    24.92,   "Win"),
    ("2026-05-11", "CEVA",   35.40,    35.77,   "Win"),
    ("2026-05-11", "DXYZ",   61.97,    65.30,   "Win"),
    ("2026-05-11", "GSIT",   10.83,    11.83,   "Win"),
    ("2026-05-11", "MRAM",   34.81,    34.60,   "Loss"),
    ("2026-05-11", "PRSO",    1.19,     1.47,   "Win"),
    ("2026-05-11", "TNET",   43.03,    42.22,   "Loss"),
    ("2026-05-11", "WYFI",   25.82,    28.12,   "Win"),
    ("2026-05-12", "PACS",   40.95,    37.82,   "Loss"),
    ("2026-05-12", "SIBN",   13.12,    13.34,   "Win"),
    ("2026-05-12", "ZBRA",  249.53,   251.65,   "Win"),
    ("2026-05-13", "DXYZ",   55.43,    52.88,   "Loss"),
    ("2026-05-13", "MNTS",    5.45,     5.59,   "Win"),
    ("2026-05-13", "MRAM",   45.76,    44.03,   "Loss"),
    ("2026-05-13", "PENG",   49.65,    48.34,   "Loss"),
    ("2026-05-13", "SNAL",    0.560,    0.522,  "Loss"),
    ("2026-05-13", "VELO",   17.83,    20.32,   "Win"),
    ("2026-05-13", "WOLF",   68.08,    63.61,   "Loss"),
    ("2026-05-14", "AIIO",    4.138,    3.943,  "Loss"),
    ("2026-05-14", "ALP",     0.376,    0.383,  "Win"),
    ("2026-05-14", "BNKK",    2.74,     2.408,  "Loss"),
    ("2026-05-14", "EDBL",    0.430,    0.434,  "Win"),
    ("2026-05-14", "GTBP",    0.391,    0.374,  "Loss"),
    ("2026-05-14", "IPST",    6.75,     6.56,   "Loss"),
    ("2026-05-14", "LESL",    1.90,     2.36,   "Win"),
    ("2026-05-14", "MOBX",    3.62,     3.68,   "Win"),
    ("2026-05-14", "SNAL",    1.38,     1.23,   "Loss"),
    ("2026-05-14", "STAA",   32.19,    32.68,   "Win"),
    ("2026-05-15", "BIYA",    1.24,     1.103,  "Loss"),
    ("2026-05-15", "BRUN",   27.63,    27.05,   "Loss"),
    ("2026-05-15", "GEMI",    6.66,     6.343,  "Loss"),
    ("2026-05-15", "HCWB",    1.290,    1.09,   "Loss"),
    ("2026-05-15", "HUBC",    0.187,    0.219,  "Win"),
    ("2026-05-15", "MRNO",    0.49,     0.449,  "Loss"),
    ("2026-05-15", "SNAL",    1.30,     1.27,   "Loss"),
    ("2026-05-15", "TDIC",    1.66,     1.20,   "Loss"),
    ("2026-05-18", "AIIO",    4.41,     5.70,   "Win"),
    ("2026-05-18", "CREG",    0.770,    0.807,  "Win"),
    ("2026-05-18", "DXYZ",   53.40,    50.22,   "Loss"),
    ("2026-05-18", "MNTS",    5.85,     5.55,   "Loss"),
    ("2026-05-18", "PMI",     0.1363,   0.219,  "Win"),
    ("2026-05-18", "QUCY",    3.87,     3.82,   "Loss"),
    ("2026-05-19", "AMST",    2.27,     2.21,   "Loss"),
    ("2026-05-19", "CODX",    2.329,    2.570,  "Win"),
    ("2026-05-19", "GIPR",    0.356,    0.386,  "Win"),
    ("2026-05-19", "MGN",     0.186,    0.173,  "Loss"),
    ("2026-05-19", "VRAX",    0.315,    0.303,  "Loss"),
    ("2026-05-19", "WNW",     4.87,     5.030,  "Win"),
    ("2026-05-20", "BLNE",    1.16,     1.20,   "Win"),
    ("2026-05-20", "EDSA",   11.44,    10.88,   "Loss"),
    ("2026-05-20", "LGVN",    0.701,    0.715,  "Win"),
    ("2026-05-20", "RVI",    55.00,    58.00,   "Win"),
    ("2026-05-20", "SLXN",    0.589,    0.561,  "Loss"),
    ("2026-05-20", "TDIC",    0.640,    0.610,  "Loss"),
    ("2026-05-20", "TLN",   338.59,   342.81,   "Win"),
    ("2026-05-20", "WNW",     5.618,    5.088,  "Loss"),
    ("2026-05-21", "AVEX",   25.28,    25.18,   "Loss"),
    ("2026-05-21", "CODX",    3.27,     3.18,   "Loss"),
    ("2026-05-21", "MRAM",   30.38,    32.25,   "Win"),
    ("2026-05-21", "QUCY",    4.07,     3.881,  "Loss"),
    ("2026-05-21", "RL",    365.00,   377.51,   "Win"),
    ("2026-05-21", "SBFM",    0.516,    0.488,  "Loss"),
    ("2026-05-21", "VIDA",    4.28,     4.09,   "Loss"),
    ("2026-05-21", "WNW",     4.27,     4.05,   "Loss"),
]

def fetch_bars(ticker, date_str, start_utc, end_utc):
    """Fetch bars — always returns a list, never None"""
    url = (
        f"{DATA_URL}/v2/stocks/{ticker}/bars"
        f"?timeframe=1Min&start={start_utc}&end={end_utc}&limit=400&feed=iex"
    )
    req = urllib.request.Request(url, headers={
        "APCA-API-KEY-ID":     API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return data.get("bars") or []
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code} for {ticker}")
        return []
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return []

def parse_bar_time(t_str):
    dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    return dt.astimezone(NY_TZ)

def safe_pct(a, b):
    """Safe percentage calculation"""
    if a is None or b is None or b == 0:
        return None
    return ((a - b) / b) * 100

def analyze_trade(date_str, ticker, buy_price, sell_price, result):
    print(f"\n{'─'*55}")
    print(f"{'✅' if result=='Win' else '❌'} {ticker} | {date_str} | Buy: ${buy_price:.4f} | {result}")

    # Pre-market: 4:00 AM - 9:30 AM NY (08:00-13:30 UTC)
    pm_bars_raw = fetch_bars(ticker, date_str,
                             f"{date_str}T08:00:00Z",
                             f"{date_str}T13:29:00Z")
    time.sleep(0.2)

    # Market open: 9:30-10:30 AM NY (13:30-14:30 UTC)
    mkt_bars_raw = fetch_bars(ticker, date_str,
                              f"{date_str}T13:30:00Z",
                              f"{date_str}T14:30:00Z")
    time.sleep(0.2)

    # Parse pre-market bars
    pm_bars = []
    for b in (pm_bars_raw or []):
        try:
            dt = parse_bar_time(b["t"])
            pm_bars.append({
                "time": dt.strftime("%H:%M"),
                "high": b["h"], "low": b["l"],
                "open": b["o"], "close": b["c"],
                "volume": b.get("v", 0)
            })
        except Exception:
            continue

    # Parse market bars
    mkt_bars = []
    for b in (mkt_bars_raw or []):
        try:
            dt = parse_bar_time(b["t"])
            mkt_bars.append({
                "time": dt.strftime("%H:%M"),
                "high": b["h"], "low": b["l"],
                "open": b["o"], "close": b["c"],
                "volume": b.get("v", 0)
            })
        except Exception:
            continue

    # ── Pre-market stats ───────────────────────────────────────────────────────
    pm_high   = max((b["high"] for b in pm_bars), default=None)
    pm_low    = min((b["low"]  for b in pm_bars), default=None)
    pm_volume = sum(b["volume"] for b in pm_bars)

    # ── Opening range 9:30–9:44 ────────────────────────────────────────────────
    or_bars  = [b for b in mkt_bars if "09:30" <= b["time"] <= "09:44"]
    or_high  = max((b["high"]  for b in or_bars), default=None)
    or_low   = min((b["low"]   for b in or_bars), default=None)
    or_open  = or_bars[0]["open"]   if or_bars else None
    or_close = or_bars[-1]["close"] if or_bars else None

    # ── Signals at 9:44 AM ─────────────────────────────────────────────────────
    fading       = (or_close < or_open) if (or_open and or_close) else None
    or_direction = ("Up" if or_close > or_open else "Down") if (or_open and or_close) else None

    entry_vs_or_high  = safe_pct(buy_price, or_high)
    entry_vs_pm_high  = safe_pct(buy_price, pm_high)
    entry_vs_or_low   = safe_pct(buy_price, or_low)

    # How much did OR move from open? (volatility measure)
    or_range_pct = safe_pct(or_high, or_low) if (or_high and or_low) else None

    # Is entry above OR high? (chasing breakout) or below (pullback buy)?
    buying_breakout = (buy_price > or_high) if or_high else None

    # ── Outcome ────────────────────────────────────────────────────────────────
    pnl_pct = safe_pct(sell_price, buy_price)

    # ── Print ──────────────────────────────────────────────────────────────────
    print(f"  --- KNOWN AT 9:44 AM ---")
    print(f"  Pre-mkt high        : ${pm_high:.4f}" if pm_high else "  Pre-mkt high        : N/A")
    print(f"  Pre-mkt volume      : {pm_volume:,}")
    print(f"  OR open  (9:30)     : ${or_open:.4f}"  if or_open  else "  OR open             : N/A")
    print(f"  OR high  (9:30-9:44): ${or_high:.4f}"  if or_high  else "  OR high             : N/A")
    print(f"  OR low   (9:30-9:44): ${or_low:.4f}"   if or_low   else "  OR low              : N/A")
    print(f"  Price at 9:44       : ${or_close:.4f}" if or_close else "  Price at 9:44       : N/A")
    print(f"  OR direction        : {or_direction}"  if or_direction else "  OR direction        : N/A")
    print(f"  OR range            : {or_range_pct:+.2f}%" if or_range_pct else "  OR range            : N/A")
    print(f"  Fading from open?   : {'YES ⚠️' if fading else 'NO ✅'}" if fading is not None else "  Fading              : N/A")
    print(f"  Buying breakout?    : {'YES — above OR high' if buying_breakout else 'NO — below OR high'}" if buying_breakout is not None else "  Buying breakout     : N/A")
    print(f"  Entry vs OR high    : {entry_vs_or_high:+.2f}%" if entry_vs_or_high is not None else "  Entry vs OR high    : N/A")
    print(f"  Entry vs PM high    : {entry_vs_pm_high:+.2f}%" if entry_vs_pm_high is not None else "  Entry vs PM high    : N/A")
    print(f"  Entry vs OR low     : {entry_vs_or_low:+.2f}% above OR low" if entry_vs_or_low is not None else "  Entry vs OR low     : N/A")
    print(f"  --- OUTCOME ---")
    print(f"  Result              : {result} ({pnl_pct:+.2f}%)" if pnl_pct else f"  Result              : {result}")

    return {
        "Date":                  date_str,
        "Ticker":                ticker,
        "Result":                result,
        "Buy Price":             round(buy_price, 4),
        "Sell Price":            round(sell_price, 4),
        "PnL %":                 round(pnl_pct, 2) if pnl_pct else "",
        "PM High":               round(pm_high, 4) if pm_high else "",
        "PM Volume":             pm_volume,
        "OR Open":               round(or_open, 4) if or_open else "",
        "OR High":               round(or_high, 4) if or_high else "",
        "OR Low":                round(or_low, 4) if or_low else "",
        "Price at 9:44":         round(or_close, 4) if or_close else "",
        "OR Direction":          or_direction or "",
        "OR Range %":            round(or_range_pct, 2) if or_range_pct else "",
        "Fading from Open":      ("YES" if fading else "NO") if fading is not None else "",
        "Buying Breakout":       ("YES" if buying_breakout else "NO") if buying_breakout is not None else "",
        "Entry vs OR High %":    round(entry_vs_or_high, 2) if entry_vs_or_high is not None else "",
        "Entry vs PM High %":    round(entry_vs_pm_high, 2) if entry_vs_pm_high is not None else "",
        "Entry vs OR Low %":     round(entry_vs_or_low, 2) if entry_vs_or_low is not None else "",
    }

def main():
    print("=" * 55)
    print("  REAL-TIME FILTER ANALYZER v2 — 9:44 AM Only")
    print("  No hindsight — only what bot can see at entry")
    print("=" * 55)

    if not API_KEY or not API_SECRET:
        print("ERROR: Missing API keys in environment variables")
        return

    results = []
    for trade in ALL_TRADES:
        row = analyze_trade(*trade)
        if row:
            results.append(row)
        time.sleep(0.1)

    if results:
        with open("realtime_filter_v2.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ CSV saved to realtime_filter_v2.csv")

    # ── Summary stats ──────────────────────────────────────────────────────────
    wins   = [r for r in results if r["Result"] == "Win"]
    losses = [r for r in results if r["Result"] == "Loss"]

    def avg(lst):
        clean = [x for x in lst if x != "" and x is not None]
        return sum(clean) / len(clean) if clean else None

    print("\n" + "=" * 55)
    print("  SIGNAL SUMMARY — Winners vs Losers at 9:44 AM")
    print("=" * 55)

    # Fading signal
    fading_wins   = sum(1 for r in wins   if r["Fading from Open"] == "YES")
    fading_losses = sum(1 for r in losses if r["Fading from Open"] == "YES")
    up_wins       = sum(1 for r in wins   if r["OR Direction"] == "Up")
    up_losses     = sum(1 for r in losses if r["OR Direction"] == "Up")
    bo_wins       = sum(1 for r in wins   if r["Buying Breakout"] == "YES")
    bo_losses     = sum(1 for r in losses if r["Buying Breakout"] == "YES")

    print(f"\n  FADING FROM OPEN at 9:44:")
    print(f"  Winners fading  : {fading_wins}/{len(wins)} ({fading_wins/len(wins)*100:.0f}%)")
    print(f"  Losers fading   : {fading_losses}/{len(losses)} ({fading_losses/len(losses)*100:.0f}%)")

    print(f"\n  OR DIRECTION UP (9:30–9:44):")
    print(f"  Winners up      : {up_wins}/{len(wins)} ({up_wins/len(wins)*100:.0f}%)")
    print(f"  Losers up       : {up_losses}/{len(losses)} ({up_losses/len(losses)*100:.0f}%)")

    print(f"\n  BUYING BREAKOUT (entry above OR high):")
    print(f"  Winners above   : {bo_wins}/{len(wins)} ({bo_wins/len(wins)*100:.0f}%)")
    print(f"  Losers above    : {bo_losses}/{len(losses)} ({bo_losses/len(losses)*100:.0f}%)")

    # Entry vs OR high
    print(f"\n  ENTRY VS OR HIGH (avg %):")
    print(f"  Winners avg     : {avg([r['Entry vs OR High %'] for r in wins]):+.2f}%")
    print(f"  Losers avg      : {avg([r['Entry vs OR High %'] for r in losses]):+.2f}%")

    # Entry vs PM high
    print(f"\n  ENTRY VS PRE-MARKET HIGH (avg %):")
    print(f"  Winners avg     : {avg([r['Entry vs PM High %'] for r in wins]):+.2f}%")
    print(f"  Losers avg      : {avg([r['Entry vs PM High %'] for r in losses]):+.2f}%")

    # OR range
    print(f"\n  OPENING RANGE SIZE (avg %):")
    print(f"  Winners avg     : {avg([r['OR Range %'] for r in wins]):+.2f}%")
    print(f"  Losers avg      : {avg([r['OR Range %'] for r in losses]):+.2f}%")

    # Pre-market volume
    print(f"\n  PRE-MARKET VOLUME (avg):")
    w_vol = avg([r['PM Volume'] for r in wins])
    l_vol = avg([r['PM Volume'] for r in losses])
    print(f"  Winners avg     : {w_vol:,.0f}" if w_vol else "  Winners avg     : N/A")
    print(f"  Losers avg      : {l_vol:,.0f}" if l_vol else "  Losers avg      : N/A")

    print("=" * 55)

if __name__ == "__main__":
    main()
