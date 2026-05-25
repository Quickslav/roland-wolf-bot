"""
Pre-Market Scanner v2 — Benjamin's Bot
=======================================
Two-mode scanning:
  1. LOOSE FILTER  — relaxed thresholds catching more genuine moves
  2. NEWS OVERRIDE — bypasses volume filters for big acquisition/
                     merger/financing catalyst news

Loosened thresholds (from historical data analysis):
  Gap          : 5% – 100%
  Change %     : under 60%
  Float        : under 50M shares
  Short Float  : over 5%
  Avg Vol      : 100K – 30M
  PM Vol       : under 50M

News override keywords (bypasses vol/gap filters):
  acquisition, merger, purchase facility, agreement, offering,
  placement, financing, blockchain, AI, partnership + $ amount

Runs at 9:00 AM ET Mon-Fri on Render Cron Job.
Cron schedule (UTC): 0 13 * * 1-5
"""

import urllib.request
import urllib.error
import json
import time
import os
import re
from datetime import datetime
import pytz

# ── Alpaca for PM volume ───────────────────────────────────────────────────────
API_KEY    = os.environ.get("ALPACA_API_KEY", "")
API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
DATA_URL   = "https://data.alpaca.markets"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ── News override keywords ─────────────────────────────────────────────────────
# If headline contains ANY of these → bypass volume filters
OVERRIDE_KEYWORDS = [
    "acqui", "merger", "purchase facilit", "equity purchase",
    "placement", "financing", "blockchain", "agreement to",
    "enters into", "strategic invest", "joint venture",
]

# ── Finviz screener — loose pre-filter ────────────────────────────────────────
# Gap 5%+, Change under 60%, Country USA, Float under 50M
FINVIZ_URL = (
    "https://finviz.com/screener.ashx?v=111&f="
    "geo_usa,"
    "sh_float_u50,"
    "sh_short_o5,"
    "ta_gap_o5"
    "&ft=4&o=-gap"
)

def fetch_url(url, headers=None):
    try:
        req = urllib.request.Request(url, headers=headers or HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  [ERROR] fetch {url}: {e}")
        return None

def parse_finviz_tickers(html):
    tickers = re.findall(r'quote\.ashx\?t=([A-Z]+)&', html)
    return list(dict.fromkeys(tickers))

def get_finviz_quote(ticker):
    html = fetch_url(f"https://finviz.com/quote.ashx?t={ticker}&ty=c&ta=1&p=d")
    if not html:
        return None

    def extract(label):
        pattern = re.compile(r'>' + re.escape(label) + r'<.*?<td.*?>(.*?)</td>', re.DOTALL)
        m = pattern.search(html)
        return re.sub(r'<[^>]+>', '', m.group(1)).strip() if m else None

    def parse_num(val):
        if not val or val == "-": return None
        val = val.replace("%","").replace(",","")
        for suffix, mult in [("B",1e9),("M",1e6),("K",1e3)]:
            if suffix in val:
                val = val.replace(suffix,"")
                try: return float(val) * mult
                except: return None
        try: return float(val)
        except: return None

    gap        = parse_num(extract("Gap"))
    change     = parse_num(extract("Change"))
    float_sh   = parse_num(extract("Shs Float"))
    short_fl   = parse_num(extract("Short Float"))
    avg_vol    = parse_num(extract("Avg Volume"))
    price      = parse_num(extract("Price"))
    prev_close = parse_num(extract("Prev Close"))
    country    = extract("Country")
    sector     = extract("Sector")
    market_cap = extract("Market Cap")

    # Convert float to millions if raw number
    if float_sh and float_sh > 1000000:
        float_sh = float_sh / 1e6

    # Convert avg vol from thousands if needed
    if avg_vol and avg_vol < 10000:
        avg_vol = avg_vol * 1000

    # Extract news headline
    news_match = re.search(r'class="news-link-cell"[^>]*>.*?<a[^>]*>(.*?)</a>', html, re.DOTALL)
    headline   = re.sub(r'<[^>]+>', '', news_match.group(1)).strip() if news_match else ""

    return {
        "ticker":     ticker,
        "price":      price,
        "prev_close": prev_close,
        "gap":        gap,
        "change":     change,
        "float_m":    float_sh,
        "short_float":short_fl,
        "avg_vol":    avg_vol,
        "country":    country,
        "sector":     sector,
        "market_cap": market_cap,
        "headline":   headline,
    }

def get_pm_volume(ticker):
    if not API_KEY or not API_SECRET:
        return None
    try:
        et_tz = pytz.timezone("America/New_York")
        today = datetime.now(et_tz).date().isoformat()
        url   = (
            f"{DATA_URL}/v2/stocks/{ticker}/bars"
            f"?timeframe=1Min&start={today}T08:00:00Z"
            f"&end={today}T13:30:00Z&limit=400&feed=iex"
        )
        req = urllib.request.Request(url, headers={
            "APCA-API-KEY-ID":     API_KEY,
            "APCA-API-SECRET-KEY": API_SECRET,
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return sum(b.get("v", 0) for b in data.get("bars", []))
    except:
        return None

def has_news_override(headline):
    """Check if headline contains big catalyst keywords."""
    hl = headline.lower()
    return any(kw in hl for kw in OVERRIDE_KEYWORDS)

def apply_loose_filter(q, pm_vol):
    """
    Loose filter — catches more genuine moves.
    Returns (passes, fails_list, mode)
    """
    gap       = q.get("gap")
    change    = q.get("change")
    float_m   = q.get("float_m")
    short_fl  = q.get("short_float")
    avg_vol   = q.get("avg_vol")
    country   = q.get("country", "")
    headline  = q.get("headline", "")
    override  = has_news_override(headline)
    fails     = []

    # ── Always apply these regardless of override ──────────────────────────────
    if gap is None:                    fails.append("Gap unknown")
    elif gap < 5:                      fails.append(f"Gap {gap:.1f}%<5%")
    elif gap > 100:                    fails.append(f"Gap {gap:.1f}%>100%")

    if change is None:                 pass  # allow if unknown
    elif change >= 60:                 fails.append(f"Change {change:.1f}%>=60%")

    if float_m is not None and float_m > 50:
                                       fails.append(f"Float {float_m:.1f}M>50M")

    if country and "USA" not in country and "United States" not in country:
                                       fails.append(f"Country:{country}")

    # ── Volume filters — skipped if news override ──────────────────────────────
    if not override:
        if short_fl is not None and short_fl < 5:
                                       fails.append(f"Short {short_fl:.1f}%<5%")
        if avg_vol is not None:
            if avg_vol < 100000:       fails.append(f"AvgVol {avg_vol:,.0f}<100K")
            if avg_vol > 30000000:     fails.append(f"AvgVol {avg_vol:,.0f}>30M")
        if pm_vol is not None and pm_vol > 50000000:
                                       fails.append(f"PMVol {pm_vol:,}>50M")

    # Intraday spread check (only when both values available)
    if gap is not None and change is not None:
        spread = change - gap
        if spread > 30 and not override:
                                       fails.append(f"Spread {spread:.1f}%>30%")

    passes = len(fails) == 0
    mode   = "NEWS OVERRIDE 🚨" if (override and passes) else ("LOOSE FILTER ✅" if passes else "")
    return passes, fails, mode

def main():
    et_tz = pytz.timezone("America/New_York")
    now   = datetime.now(et_tz)

    print("=" * 70)
    print(f"  PRE-MARKET SCANNER v2 — {now.strftime('%A %B %d, %Y %I:%M %p ET')}")
    print("=" * 70)
    print("  MODE 1 — LOOSE FILTER:")
    print("    Gap 5-100% | Change <60% | Float <50M | Short >5%")
    print("    AvgVol 100K-30M | PMVol <50M | Spread <30%")
    print("  MODE 2 — NEWS OVERRIDE:")
    print("    Acquisition/merger/financing keywords → bypasses vol filters")
    print("=" * 70)

    # Fetch Finviz
    html = fetch_url(FINVIZ_URL)
    if not html:
        print("[SCAN] Could not fetch Finviz")
        return

    tickers = parse_finviz_tickers(html)
    if not tickers:
        print("[SCAN] No tickers found")
        return

    print(f"\n[SCAN] {len(tickers)} tickers from Finviz — checking each...\n")

    candidates  = []
    overrides   = []
    rejected    = []

    for ticker in tickers:
        print(f"  Checking {ticker}...", end=" ")
        quote = get_finviz_quote(ticker)
        if not quote:
            print("no data")
            continue

        pm_vol = get_pm_volume(ticker)
        passes, fails, mode = apply_loose_filter(quote, pm_vol)

        g   = quote.get("gap")
        ch  = quote.get("change")
        fl  = quote.get("float_m")
        sf  = quote.get("short_float")
        av  = quote.get("avg_vol")
        hl  = quote.get("headline", "")[:60]
        spread = (ch - g) if (ch and g) else None

        if passes:
            entry = {**quote, "pm_vol": pm_vol, "spread": spread, "mode": mode}
            if "OVERRIDE" in mode:
                overrides.append(entry)
                print(f"🚨 OVERRIDE — {hl}")
            else:
                candidates.append(entry)
                print(f"✅ PASS")
        else:
            rejected.append(ticker)
            print(f"❌ {' | '.join(fails[:2])}")

        time.sleep(1.5)

    # ── Print results ──────────────────────────────────────────────────────────
    all_pass = candidates + overrides
    print("\n" + "=" * 70)
    print(f"  SCAN RESULTS — {now.strftime('%b %d %Y')}")
    print("=" * 70)

    if all_pass:
        print(f"\n  🎯 {len(all_pass)} CANDIDATE(S):\n")
        print(f"  {'Mode':<18} {'Ticker':<7} {'Gap%':>6} {'Chg%':>6} {'Sprd':>6} {'Float':>7} {'Short':>6} {'PMVol':>12}")
        print(f"  {'-'*72}")

        for c in sorted(all_pass, key=lambda x: x.get("gap") or 0, reverse=True):
            mode_short = "🚨 OVERRIDE" if "OVERRIDE" in c.get("mode","") else "✅ LOOSE"
            ticker = c['ticker']
            gap    = c.get('gap')
            chg    = c.get('change')
            sprd   = c.get('spread')
            fl     = c.get('float_m')
            sf     = c.get('short_float')
            pm     = c.get('pm_vol')

            print(f"  {mode_short:<18} {ticker:<7}"
                  f" {gap:>5.1f}%"
                  f" {chg:>5.1f}%"
                  f" {sprd:>5.1f}%"
                  f" {fl:>5.1f}M"
                  f" {sf:>5.1f}%"
                  f" {pm:>12,}" if pm else
                  f"  {mode_short:<18} {ticker:<7}"
                  f" {(gap or 0):>5.1f}%"
                  f" {(chg or 0):>5.1f}%"
                  f" {(sprd or 0):>5.1f}%"
                  f" {(fl or 0):>5.1f}M"
                  f" {(sf or 0):>5.1f}%"
                  f"          N/A")

            # Print headline for overrides
            if "OVERRIDE" in c.get("mode",""):
                hl = c.get("headline","")
                if hl:
                    print(f"  {'':18} 📰 {hl[:65]}")

        print(f"\n  Add candidates to TradingView watchlist before 9:30 AM ET")
        print(f"  🚨 Override stocks have big catalyst — expect high volatility")
    else:
        print(f"\n  ⚠️  No candidates today")

    print(f"\n  Total checked  : {len(tickers)}")
    print(f"  Loose filter   : {len(candidates)}")
    print(f"  News override  : {len(overrides)}")
    print(f"  Rejected       : {len(rejected)}")
    print("=" * 70)

if __name__ == "__main__":
    main()
