"""
30% Rule Analyzer
=================
Tests the rule: if price hits 30% above the 9:30 AM open
at any point during market hours (9:30-4:00 PM), sell.

For each trade checks:
  1. What was the 9:30 AM open?
  2. What is the 30% target price?
  3. Did price ever hit that target?
  4. What time did it hit?
  5. What was the actual exit P&L vs the 30% rule P&L?
  6. Did the rule help or hurt?
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
    # Date         Ticker    Buy $      Sell $   Exit NY   Result
    # ── May 5 ──
    ("2026-05-05", "ALMU",   25.75,    24.86,   "09:59",  "Loss"),
    ("2026-05-05", "AVTX",   18.41,    21.73,   "11:28",  "Win"),
    ("2026-05-05", "BCG",     2.44,     2.27,   "10:17",  "Loss"),
    ("2026-05-05", "CLRB",    3.03,     2.93,   "10:01",  "Loss"),
    ("2026-05-05", "CLYM",   11.72,    11.92,   "11:28",  "Win"),
    ("2026-05-05", "CUE",    37.90,    36.01,   "09:57",  "Loss"),
    ("2026-05-05", "HNRG",   18.19,    18.45,   "11:28",  "Win"),
    ("2026-05-05", "PENG",   37.14,    39.10,   "11:28",  "Win"),
    ("2026-05-05", "SDOT",    0.3428,   0.3690,  "11:28",  "Win"),
    ("2026-05-05", "STRL",  854.00,   866.58,   "11:28",  "Win"),
    ("2026-05-05", "THR",    69.59,    69.18,   "11:28",  "Loss"),
    ("2026-05-05", "TRT",    12.78,    12.10,   "10:32",  "Loss"),
    ("2026-05-05", "TYGO",    4.87,     4.60,   "11:28",  "Loss"),
    ("2026-05-05", "UCTT",   84.69,    81.50,   "10:07",  "Loss"),
    ("2026-05-05", "VVX",    76.12,    75.68,   "11:28",  "Loss"),
    # ── May 6 ──
    ("2026-05-06", "ACLS",  162.78,   160.06,   "11:28",  "Loss"),
    ("2026-05-06", "ANY",     1.95,     2.05,   "11:28",  "Win"),
    ("2026-05-06", "AOSL",   38.51,    37.24,   "10:20",  "Loss"),
    ("2026-05-06", "AVTX",   23.11,    22.45,   "11:28",  "Loss"),
    ("2026-05-06", "BKSY",   31.89,    31.37,   "09:50",  "Loss"),
    ("2026-05-06", "ERNA",    6.49,     7.13,   "11:28",  "Win"),
    ("2026-05-06", "GCTK",    0.869,    0.840,  "10:31",  "Loss"),
    ("2026-05-06", "GRAL",   62.62,    63.29,   "11:28",  "Win"),
    ("2026-05-06", "MRAM",   20.89,    21.66,   "11:28",  "Win"),
    ("2026-05-06", "NNE",    26.66,    27.04,   "11:28",  "Win"),
    ("2026-05-06", "OSS",    14.77,    14.81,   "11:28",  "Win"),
    ("2026-05-06", "SNBR",    3.11,     2.96,   "10:10",  "Loss"),
    ("2026-05-06", "SST",     4.04,     4.10,   "11:28",  "Win"),
    ("2026-05-06", "VSEC",  208.37,   202.87,   "10:27",  "Loss"),
    # ── May 7 ──
    ("2026-05-07", "AIM",     0.373,    0.370,  "11:28",  "Loss"),
    ("2026-05-07", "AMZE",    0.139,    0.134,  "10:04",  "Loss"),
    ("2026-05-07", "ANY",     1.98,     1.90,   "10:35",  "Loss"),
    ("2026-05-07", "ARQ",     2.56,     2.73,   "11:28",  "Win"),
    ("2026-05-07", "ATRA",    9.25,     8.40,   "11:12",  "Loss"),
    ("2026-05-07", "BOBS",   13.05,    13.10,   "11:28",  "Win"),
    ("2026-05-07", "DXYZ",   48.26,    52.52,   "11:28",  "Win"),
    ("2026-05-07", "FABC",    4.60,     4.39,   "10:00",  "Loss"),
    ("2026-05-07", "NSP",    31.84,    32.31,   "11:28",  "Win"),
    ("2026-05-07", "OSRH",    0.626,    0.611,  "09:56",  "Loss"),
    ("2026-05-07", "SOBR",    1.77,     1.685,  "09:58",  "Loss"),
    ("2026-05-07", "SEZL",   96.95,    93.50,   "10:37",  "Loss"),
    ("2026-05-07", "WGS",    38.69,    40.81,   "11:28",  "Win"),
    # ── May 11 ──
    ("2026-05-11", "BNAI",   22.54,    24.92,   "11:28",  "Win"),
    ("2026-05-11", "CEVA",   35.40,    35.77,   "11:28",  "Win"),
    ("2026-05-11", "DXYZ",   61.97,    65.30,   "11:28",  "Win"),
    ("2026-05-11", "GSIT",   10.83,    11.83,   "11:28",  "Win"),
    ("2026-05-11", "MRAM",   34.81,    34.60,   "10:00",  "Loss"),
    ("2026-05-11", "PRSO",    1.19,     1.47,   "11:22",  "Win"),
    ("2026-05-11", "TNET",   43.03,    42.22,   "11:28",  "Loss"),
    ("2026-05-11", "WYFI",   25.82,    28.12,   "11:28",  "Win"),
    # ── May 12 ──
    ("2026-05-12", "PACS",   40.95,    37.82,   "10:37",  "Loss"),
    ("2026-05-12", "SIBN",   13.12,    13.34,   "11:28",  "Win"),
    ("2026-05-12", "ZBRA",  249.53,   251.65,   "11:28",  "Win"),
    # ── May 13 ──
    ("2026-05-13", "DXYZ",   55.43,    52.88,   "10:09",  "Loss"),
    ("2026-05-13", "MNTS",    5.45,     5.59,   "11:28",  "Win"),
    ("2026-05-13", "MRAM",   45.76,    44.03,   "10:09",  "Loss"),
    ("2026-05-13", "PENG",   49.65,    48.34,   "11:20",  "Loss"),
    ("2026-05-13", "SNAL",    0.560,    0.522,  "10:04",  "Loss"),
    ("2026-05-13", "VELO",   17.83,    20.32,   "11:28",  "Win"),
    ("2026-05-13", "WOLF",   68.08,    63.61,   "09:59",  "Loss"),
    # ── May 14 ──
    ("2026-05-14", "AIIO",    4.138,    3.943,  "09:51",  "Loss"),
    ("2026-05-14", "ALP",     0.376,    0.383,  "10:03",  "Win"),
    ("2026-05-14", "BNKK",    2.74,     2.408,  "10:03",  "Loss"),
    ("2026-05-14", "EDBL",    0.430,    0.434,  "11:28",  "Win"),
    ("2026-05-14", "GTBP",    0.391,    0.374,  "09:56",  "Loss"),
    ("2026-05-14", "IPST",    6.75,     6.56,   "09:49",  "Loss"),
    ("2026-05-14", "LESL",    1.90,     2.36,   "10:50",  "Win"),
    ("2026-05-14", "MOBX",    3.62,     3.68,   "09:48",  "Win"),
    ("2026-05-14", "SNAL",    1.38,     1.23,   "09:50",  "Loss"),
    ("2026-05-14", "STAA",   32.19,    32.68,   "11:28",  "Win"),
    # ── May 15 ──
    ("2026-05-15", "BIYA",    1.24,     1.103,  "11:01",  "Loss"),
    ("2026-05-15", "BRUN",   27.63,    27.05,   "09:50",  "Loss"),
    ("2026-05-15", "GEMI",    6.66,     6.343,  "09:59",  "Loss"),
    ("2026-05-15", "HCWB",    1.290,    1.09,   "10:12",  "Loss"),
    ("2026-05-15", "HUBC",    0.187,    0.219,  "10:57",  "Win"),
    ("2026-05-15", "MRNO",    0.49,     0.449,  "10:13",  "Loss"),
    ("2026-05-15", "SNAL",    1.30,     1.27,   "11:28",  "Loss"),
    ("2026-05-15", "TDIC",    1.66,     1.20,   "11:02",  "Loss"),
    # ── May 18 ──
    ("2026-05-18", "AIIO",    4.41,     5.70,   "10:12",  "Win"),
    ("2026-05-18", "CREG",    0.770,    0.807,  "10:32",  "Win"),
    ("2026-05-18", "DXYZ",   53.40,    50.22,   "10:34",  "Loss"),
    ("2026-05-18", "MNTS",    5.85,     5.55,   "10:19",  "Loss"),
    ("2026-05-18", "PMI",     0.1363,   0.219,  "11:20",  "Win"),
    ("2026-05-18", "QUCY",    3.87,     3.82,   "09:48",  "Loss"),
    # ── May 19 ──
    ("2026-05-19", "AMST",    2.27,     2.21,   "09:47",  "Loss"),
    ("2026-05-19", "CODX",    2.329,    2.570,  "09:47",  "Win"),
    ("2026-05-19", "GIPR",    0.356,    0.386,  "09:54",  "Win"),
    ("2026-05-19", "MGN",     0.186,    0.173,  "10:12",  "Loss"),
    ("2026-05-19", "VRAX",    0.315,    0.303,  "09:52",  "Loss"),
    ("2026-05-19", "WNW",     4.87,     5.030,  "10:04",  "Win"),
    # ── May 20 ──
    ("2026-05-20", "BLNE",    1.16,     1.20,   "14:01",  "Win"),
    ("2026-05-20", "EDSA",   11.44,    10.88,   "10:26",  "Loss"),
    ("2026-05-20", "LGVN",    0.701,    0.715,  "14:02",  "Win"),
    ("2026-05-20", "RVI",    55.00,    58.00,   "14:01",  "Win"),
    ("2026-05-20", "SLXN",    0.589,    0.561,  "09:52",  "Loss"),
    ("2026-05-20", "TDIC",    0.640,    0.610,  "09:52",  "Loss"),
    ("2026-05-20", "TLN",   338.59,   342.81,   "14:01",  "Win"),
    ("2026-05-20", "WNW",     5.618,    5.088,  "12:39",  "Loss"),
    # ── May 21 ──
    ("2026-05-21", "AVEX",   25.28,    25.18,   "14:03",  "Loss"),
    ("2026-05-21", "CODX",    3.27,     3.18,   "09:49",  "Loss"),
    ("2026-05-21", "MRAM",   30.38,    32.25,   "14:01",  "Win"),
    ("2026-05-21", "QUCY",    4.07,     3.881,  "10:12",  "Loss"),
    ("2026-05-21", "RL",    365.00,   377.51,   "14:01",  "Win"),
    ("2026-05-21", "SBFM",    0.516,    0.488,  "09:48",  "Loss"),
    ("2026-05-21", "VIDA",    4.28,     4.09,   "09:55",  "Loss"),
    ("2026-05-21", "WNW",     4.27,     4.05,   "10:52",  "Loss"),
]

def fetch_bars(ticker, date_str):
    """Fetch full market day 1-min bars"""
    url = (
        f"{DATA_URL}/v2/stocks/{ticker}/bars"
        f"?timeframe=1Min&start={date_str}T13:30:00Z"
        f"&end={date_str}T20:00:00Z&limit=400&feed=iex"
    )
    req = urllib.request.Request(url, headers={
        "APCA-API-KEY-ID":     API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()).get("bars", [])
    except Exception as e:
        print(f"  Error: {e}")
        return []

def parse_bar_time(t_str):
    dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    return dt.astimezone(NY_TZ)

def analyze_trade(date_str, ticker, buy_price, sell_price, exit_time, result):
    print(f"\n{'─'*55}")
    print(f"{'✅' if result=='Win' else '❌'} {ticker} | {date_str} | Buy: ${buy_price:.4f} | {result}")

    bars_raw = fetch_bars(ticker, date_str)
    time.sleep(0.25)

    if not bars_raw:
        print("  No data")
        return None

    bars = []
    for b in bars_raw:
        dt = parse_bar_time(b["t"])
        bars.append({
            "time": dt.strftime("%H:%M"),
            "high": b["h"], "low": b["l"],
            "open": b["o"], "close": b["c"],
        })

    # 9:30 open price
    open_bar   = next((b for b in bars if b["time"] == "09:30"), None)
    open_price = open_bar["open"] if open_bar else (bars[0]["open"] if bars else None)

    if not open_price:
        print("  No open price found")
        return None

    # 30% target
    target_30  = open_price * 1.30
    actual_pnl = ((sell_price - buy_price) / buy_price) * 100

    # Did price ever hit 30% above open during market hours?
    hit_target      = False
    hit_target_time = None
    hit_target_price = None

    for b in bars:
        if b["high"] >= target_30:
            hit_target       = True
            hit_target_time  = b["time"]
            hit_target_price = target_30
            break  # first time it hit

    # If it hit — what would P&L have been?
    rule_pnl     = None
    rule_exit    = None
    rule_verdict = None

    if hit_target:
        rule_pnl     = ((target_30 - buy_price) / buy_price) * 100
        rule_exit    = hit_target_time

        # Did bot even hold until the target hit?
        # Compare exit time to hit time
        def time_to_mins(t):
            h, m = map(int, t.split(":"))
            return h * 60 + m

        exit_mins   = time_to_mins(exit_time)
        target_mins = time_to_mins(hit_target_time)

        if exit_mins <= target_mins:
            # Bot exited before target hit
            rule_verdict = "Bot exited before target — rule irrelevant"
        else:
            # Bot would have hit the 30% rule
            if rule_pnl > actual_pnl:
                rule_verdict = "HELPED — locked in more profit"
            elif rule_pnl < actual_pnl:
                rule_verdict = "HURT — cut winner short"
            else:
                rule_verdict = "NEUTRAL"
    else:
        rule_verdict = "Target never hit — rule had no effect"

    print(f"  9:30 open       : ${open_price:.4f}")
    print(f"  30% target      : ${target_30:.4f}")
    print(f"  Hit target?     : {'YES at ' + hit_target_time if hit_target else 'NO'}")
    print(f"  Actual exit     : {exit_time} @ ${sell_price:.4f} ({actual_pnl:+.2f}%)")
    if hit_target and rule_pnl is not None:
        print(f"  Rule exit would : {rule_exit} @ ${target_30:.4f} ({rule_pnl:+.2f}%)")
        diff = rule_pnl - actual_pnl
        print(f"  Difference      : {diff:+.2f}%")
    print(f"  Verdict         : {rule_verdict}")

    return {
        "Date":           date_str,
        "Ticker":         ticker,
        "Result":         result,
        "Buy Price":      round(buy_price, 4),
        "Sell Price":     round(sell_price, 4),
        "Actual Exit":    exit_time,
        "Actual PnL %":   round(actual_pnl, 2),
        "9:30 Open":      round(open_price, 4),
        "30% Target":     round(target_30, 4),
        "Hit Target":     "YES" if hit_target else "NO",
        "Target Hit Time": hit_target_time or "",
        "Rule PnL %":     round(rule_pnl, 2) if rule_pnl is not None else "",
        "PnL Difference %": round(rule_pnl - actual_pnl, 2) if rule_pnl is not None and exit_time > (hit_target_time or "") else "",
        "Verdict":        rule_verdict,
    }

def main():
    print("=" * 55)
    print("  30% RULE ANALYZER — Benjamin's Bot")
    print("  Sell if price hits 30% above 9:30 AM open")
    print("=" * 55)

    if not API_KEY or not API_SECRET:
        print("ERROR: Missing API keys")
        return

    results = []
    for trade in ALL_TRADES:
        row = analyze_trade(*trade)
        if row:
            results.append(row)
        time.sleep(0.2)

    if results:
        with open("thirty_pct_rule_analysis.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ CSV saved to thirty_pct_rule_analysis.csv")

    # ── Summary ────────────────────────────────────────────────────────────────
    hit       = [r for r in results if r["Hit Target"] == "YES"]
    no_hit    = [r for r in results if r["Hit Target"] == "NO"]
    helped    = [r for r in results if "HELPED" in r["Verdict"]]
    hurt      = [r for r in results if "HURT"   in r["Verdict"]]
    irrelevant= [r for r in results if "irrelevant" in r["Verdict"] or "no effect" in r["Verdict"]]

    avg_helped = sum(r["PnL Difference %"] for r in helped if r["PnL Difference %"] != "") / len(helped) if helped else 0
    avg_hurt   = sum(r["PnL Difference %"] for r in hurt   if r["PnL Difference %"] != "") / len(hurt)   if hurt   else 0

    print("\n" + "=" * 55)
    print("  30% RULE SUMMARY")
    print("=" * 55)
    print(f"  Total trades          : {len(results)}")
    print(f"  Target hit            : {len(hit)} trades")
    print(f"  Target never hit      : {len(no_hit)} trades")
    print(f"")
    print(f"  Of trades where rule applied:")
    print(f"  Rule HELPED           : {len(helped)} trades (avg +{avg_helped:.2f}% better)")
    print(f"  Rule HURT             : {len(hurt)} trades (avg {avg_hurt:.2f}% worse)")
    print(f"  Bot exited before hit : {len(irrelevant)} trades")
    print(f"")

    # Best saves and worst cuts
    if helped:
        best = max(helped, key=lambda r: r["PnL Difference %"] if r["PnL Difference %"] != "" else 0)
        print(f"  Best save  : {best['Ticker']} {best['Date']} (+{best['PnL Difference %']}% better)")
    if hurt:
        worst = min(hurt, key=lambda r: r["PnL Difference %"] if r["PnL Difference %"] != "" else 0)
        print(f"  Worst cut  : {worst['Ticker']} {worst['Date']} ({worst['PnL Difference %']}% worse)")

    print("=" * 55)

if __name__ == "__main__":
    main()
