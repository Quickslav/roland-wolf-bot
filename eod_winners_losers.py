"""
eod_winners_losers.py
──────────────────────────────────────────────────────────────────────────────
Fetches the top 20 biggest WINNERS and top 20 biggest LOSERS for every trading
day over a date range, ranked by daily % change (close vs previous close).

SOURCE: Alpaca daily bars (IEX feed)

OUTPUT: writes two CSV files
    eod_winners.csv   — top 20 gainers per day
    eod_losers.csv    — top 20 losers per day

Columns (Alpaca-derived only — fundamentals left as N/A):
    Date, Ticker, Volume, Price (close), Change %, Gap %,
    Prev Close, Open, High, Low, Trades

USAGE
──────
  pip install requests
  python eod_winners_losers.py
  python eod_winners_losers.py --start 2026-04-13 --end 2026-06-12

NOTES
──────
- Builds its ticker universe from Alpaca's most-active screener (top 100 by
  volume). For a wider net, raise UNIVERSE_SIZE — but note Alpaca's screener
  reflects RECENT actives, so very stale tickers from 2 months ago may not all
  appear. This is the main limitation of an Alpaca-only approach.
- % change uses each ticker's own previous trading-day close from the bars.
"""

import argparse
import csv
import time
from datetime import date, datetime, timedelta

import requests

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
KEY    = "PKIYGXQT3DGX7B6BDIZFZ6VQWU"
SECRET = "Ekaz5bQHUbbFUQvmidbBSU89wHMtgigik3TsyFD15NA3"

HEADERS = {
    "APCA-API-KEY-ID":     KEY,
    "APCA-API-SECRET-KEY": SECRET,
}

BATCH_SIZE      = 200          # tickers per bars request (keeps URLs sane)
TOP_N           = 20           # winners and losers count
MIN_PREV_CLOSE  = 0.50         # drop stocks under 50c
MIN_VOLUME      = 50000        # light liquidity floor so moves are real/tradable
WINNERS_CSV     = "eod_winners.csv"
LOSERS_CSV      = "eod_losers.csv"

COLUMNS = [
    "Date", "Ticker", "Volume", "Price", "Change %", "Gap %",
    "Prev Close", "Open", "High", "Low", "Trades",
]


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def trading_days(start: date, end: date) -> list[date]:
    """Weekdays between start and end inclusive (no holiday calendar)."""
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def get_universe() -> list[str]:
    """
    Pull the FULL list of active, tradable US equities from Alpaca.
    This is the free way to get a broad universe (thousands of names)
    instead of just the 100 most-actives.
    """
    print("Fetching full tradable US equity universe...")
    r = requests.get(
        "https://api.alpaca.markets/v2/assets",
        headers=HEADERS,
        params={"status": "active", "asset_class": "us_equity"},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  ⚠ Assets error {r.status_code}: {r.text[:200]}")
        return []

    assets = r.json()
    # Keep only tradable names on major exchanges, skip OTC junk
    tickers = [
        a["symbol"] for a in assets
        if a.get("tradable")
        and a.get("exchange") in ("NYSE", "NASDAQ", "AMEX", "ARCA", "BATS")
        and "/" not in a["symbol"]      # skip preferred/units oddities
    ]
    print(f"  Got {len(tickers)} tradable tickers.")
    return tickers


def fetch_all_bars(tickers: list[str], start: date, end: date) -> dict:
    """
    Fetch daily bars for all tickers across the range, batching the ticker
    list so each request stays a reasonable size. Handles pagination within
    each batch. Returns {ticker: {date_str: bar}}.
    """
    print(f"Fetching daily bars {start} → {end} for {len(tickers)} tickers...")
    out: dict[str, dict[str, dict]] = {}

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    print(f"  Split into {len(batches)} batches of up to {BATCH_SIZE}.")

    for bi, batch in enumerate(batches, 1):
        page_token = None
        while True:
            params = {
                "symbols":    ",".join(batch),
                "timeframe":  "1Day",
                "start":      str(start),
                "end":        str(end),
                "limit":      10000,
                "feed":       "iex",
                "adjustment": "raw",
            }
            if page_token:
                params["page_token"] = page_token

            r = requests.get(
                "https://data.alpaca.markets/v2/stocks/bars",
                headers=HEADERS,
                params=params,
                timeout=30,
            )
            if r.status_code != 200:
                print(f"  ⚠ Batch {bi} error {r.status_code}: {r.text[:150]}")
                break

            data = r.json()
            bars = data.get("bars", {})
            for ticker, bar_list in bars.items():
                out.setdefault(ticker, {})
                for bar in bar_list:
                    out[ticker][bar["t"][:10]] = bar

            page_token = data.get("next_page_token")
            if not page_token:
                break
            time.sleep(0.15)

        if bi % 5 == 0 or bi == len(batches):
            print(f"  ...batch {bi}/{len(batches)} done ({len(out)} tickers so far)")
        time.sleep(0.15)

    print(f"  Got bars for {len(out)} tickers.")
    return out


def prev_close_for(date_map: dict, day: date):
    """Find the close of the most recent trading day before `day`."""
    for back in range(1, 8):
        prev = str(day - timedelta(days=back))
        if prev in date_map:
            return date_map[prev]["c"]
    return None


def build_rows(day: date, ticker_map: dict) -> list[dict]:
    """Build candidate rows for one day with % change computed."""
    day_str = str(day)
    candidates = []

    for ticker, date_map in ticker_map.items():
        if day_str not in date_map:
            continue
        bar  = date_map[day_str]
        prev = prev_close_for(date_map, day)
        if prev is None or prev == 0:
            continue

        # Filters to strip junk: penny stocks and illiquid names produce
        # absurd % swings that aren't real trading opportunities
        if prev < MIN_PREV_CLOSE:
            continue
        if bar.get("v", 0) < MIN_VOLUME:
            continue

        pct  = (bar["c"] - prev) / prev * 100
        gap  = (bar["o"] - prev) / prev * 100

        candidates.append({
            "Date":       day_str,
            "Ticker":     ticker,
            "Volume":     bar.get("v", "N/A"),
            "Price":      round(bar["c"], 4),
            "Change %":   round(pct, 2),
            "Gap %":      round(gap, 2),
            "Prev Close": round(prev, 4),
            "Open":       round(bar["o"], 4),
            "High":       round(bar["h"], 4),
            "Low":        round(bar["l"], 4),
            "Trades":     bar.get("n", "N/A"),
            "_pct":       pct,     # sort key, stripped before write
        })

    return candidates


def write_csv(path: str, rows: list[dict]):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for r in rows:
            r.pop("_pct", None)
            writer.writerow(r)


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main(start: date, end: date):
    universe = get_universe()
    if not universe:
        print("No universe — aborting.")
        return

    ticker_map = fetch_all_bars(universe, start - timedelta(days=7), end)
    if not ticker_map:
        print("No bars — aborting.")
        return

    all_winners, all_losers = [], []

    for day in trading_days(start, end):
        candidates = build_rows(day, ticker_map)
        if not candidates:
            print(f"  {day} — no data")
            continue

        candidates.sort(key=lambda x: x["_pct"], reverse=True)
        winners = candidates[:TOP_N]
        losers  = candidates[-TOP_N:][::-1]   # most negative first

        all_winners.extend(winners)
        all_losers.extend(losers)

        print(f"  {day} — W: {winners[0]['Ticker']} +{winners[0]['_pct']:.1f}% | "
              f"L: {losers[0]['Ticker']} {losers[0]['_pct']:.1f}%")

    write_csv(WINNERS_CSV, all_winners)
    write_csv(LOSERS_CSV, all_losers)

    print(f"\n✓ Done.")
    print(f"  {WINNERS_CSV}: {len(all_winners)} rows")
    print(f"  {LOSERS_CSV}:  {len(all_losers)} rows")


if __name__ == "__main__":
    today = date.today()
    default_start = today - timedelta(days=60)

    parser = argparse.ArgumentParser(description="EOD top 20 winners/losers from Alpaca")
    parser.add_argument("--start", default=str(default_start),
                        help="Start date YYYY-MM-DD (default: 60 days ago)")
    parser.add_argument("--end", default=str(today),
                        help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end   = datetime.strptime(args.end,   "%Y-%m-%d").date()
    main(start, end)
