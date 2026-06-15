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

UNIVERSE_SIZE   = 100          # how many most-active tickers to pull
TOP_N           = 20           # winners and losers count
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
    """Pull most-active tickers from Alpaca's screener."""
    print(f"Fetching most-active universe (top {UNIVERSE_SIZE})...")
    r = requests.get(
        "https://data.alpaca.markets/v1beta1/screener/stocks/most-actives",
        headers=HEADERS,
        params={"by": "volume", "top": UNIVERSE_SIZE},
        timeout=15,
    )
    if r.status_code != 200:
        print(f"  ⚠ Screener error {r.status_code}: {r.text[:200]}")
        return []
    tickers = [x["symbol"] for x in r.json().get("most_actives", [])]
    print(f"  Got {len(tickers)} tickers.")
    return tickers


def fetch_all_bars(tickers: list[str], start: date, end: date) -> dict:
    """
    Fetch daily bars for all tickers across the range in one paginated call.
    Returns {ticker: {date_str: bar}}.
    """
    print(f"Fetching daily bars {start} → {end}...")
    out: dict[str, dict[str, dict]] = {}
    page_token = None

    while True:
        params = {
            "symbols":    ",".join(tickers),
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
            print(f"  ⚠ Bars error {r.status_code}: {r.text[:200]}")
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
        time.sleep(0.2)

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
