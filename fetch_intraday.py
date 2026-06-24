#!/usr/bin/env python3
"""
fetch_intraday.py
-----------------
Pull intraday bars (5-min default) for the mover tickers on their mover days, and
compute one row of PATH metrics per ticker-day. This is what daily OHLC can't give:
WHEN the high happens, how deep the pullback from a running peak is (trailing-stop
sizing), and how the first 15-30 min separates a trap from a runner.

Runs on Render. Alpaca keys from env: ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY.

IMPORTANT — feed: intraday microcap data on the free IEX feed is SPARSE (IEX is a
few % of volume, so 1-min bars for illiquid names have holes). For clean intraday
use --feed sip (needs the paid subscription). The script flags sparse days (data_ok).

Scope control (intraday is a big pull):
  --gap-min / --gap-max  default 5 / 50  (the tradable gap-up band; cuts noise)
  --timeframe            default 5Min     (1Min for finer, 5x the data)
  --label                all | W | L
  --premarket            also pull 08:00-09:30 (default off, RTH 09:30-16:00 only)
  --limit-dates N / --limit-tickers N    test runs
  --save-raw            also dump raw bars to parquet (large)

Output: a compact metrics CSV (one row per ticker-day) -> feeds analyze_intraday.py.

Usage:
  python fetch_intraday.py --in merged_trading_tracker__22-06-26_.xlsx --feed sip
  python fetch_intraday.py --in <file> --timeframe 1Min --gap-min 5 --gap-max 35
  python fetch_intraday.py --in <file> --dry-run     # validate plan, no Alpaca
"""
import os, sys, time, argparse, datetime as dt
from collections import defaultdict
from zoneinfo import ZoneInfo
import pandas as pd, numpy as np

ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
ET = ZoneInfo("America/New_York")
UTC = dt.timezone.utc
# (header row index 0-based, label) — Losers Q2 sits one row lower
EOD_SPECS = {'EOD Biggest Winners Q1': (2, 'W'), 'EOD Biggest Losers Q1': (2, 'L'),
             'EOD Biggest Winners Q2': (2, 'W'), 'EOD Biggest Losers Q2': (3, 'L')}
TARGETS = [('ret_0935', dt.time(9,35)), ('ret_0945', dt.time(9,45)), ('ret_1000', dt.time(10,0)),
           ('ret_1030', dt.time(10,30)), ('ret_1100', dt.time(11,0)), ('ret_1130', dt.time(11,30))]

# ---------- workbook read (pandas — fast) ----------

def collect(path, gmin, gmax, label):
    xl = pd.ExcelFile(path)
    today = dt.date.today()
    pairs = {}
    for tab, (hdr, lab) in EOD_SPECS.items():
        if label != 'all' and lab != label: continue
        df = pd.read_excel(xl, sheet_name=tab, header=hdr)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.rename(columns={'Gap %': 'Gap'})
        df = df[df['Ticker'].notna()]
        d = pd.to_datetime(df['Date'], errors='coerce').dt.date
        gap = pd.to_numeric(df['Gap'], errors='coerce')
        for tk, dd, gp in zip(df['Ticker'].astype(str).str.upper().str.strip(), d, gap):
            if dd is not None and dd == dd and dd < today and gp == gp and gmin <= gp <= gmax:
                pairs[(tk, dd)] = (lab, float(gp))   # dedupe ticker-day
    return pairs

# ---------- alpaca ----------

def _get(url, params, hdr, tries=5):
    import requests
    for t in range(tries):
        r = requests.get(url, params=params, headers=hdr, timeout=40)
        if r.status_code == 200: return r.json()
        if r.status_code == 429: time.sleep(2**t); continue
        if r.status_code in (500,502,503,504): time.sleep(2**t); continue
        raise RuntimeError(f"Alpaca {r.status_code}: {r.text[:200]}")
    raise RuntimeError("max retries")

def fetch_day(symbols, d, key, secret, tf, premarket, feed, pause=0.3):
    """All symbols' intraday bars for one date -> {sym: sorted [bars]} (bars carry ET time)."""
    hdr = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret}
    s_et = dt.datetime.combine(d, dt.time(8,0) if premarket else dt.time(9,30), ET)
    e_et = dt.datetime.combine(d, dt.time(16,0), ET)
    start, end = s_et.astimezone(UTC).isoformat(), e_et.astimezone(UTC).isoformat()
    out = defaultdict(list)
    syms = sorted(symbols)
    for i in range(0, len(syms), 100):
        batch = syms[i:i+100]; token = None
        while True:
            params = {'symbols': ','.join(batch), 'timeframe': tf, 'start': start, 'end': end,
                      'feed': feed, 'adjustment': 'raw', 'limit': 10000}
            if token: params['page_token'] = token
            js = _get(ALPACA_BARS_URL, params, hdr)
            for sym, bars in (js.get('bars') or {}).items():
                for b in bars:
                    t_et = dt.datetime.fromisoformat(b['t'].replace('Z','+00:00')).astimezone(ET)
                    out[sym].append({'t': t_et, 'o': b['o'], 'h': b['h'], 'l': b['l'], 'c': b['c'], 'v': b['v']})
            token = js.get('next_page_token')
            if not token: break
            time.sleep(pause)
        time.sleep(pause)
    for s in out: out[s].sort(key=lambda x: x['t'])
    return out

# ---------- path metrics ----------

def metrics(bars, d):
    rth = [b for b in bars if dt.time(9,30) <= b['t'].time() < dt.time(16,0)]
    if len(rth) < 2: return None
    o = rth[0]['o']
    if o <= 0: return None
    hi = max(b['h'] for b in rth); lo = min(b['l'] for b in rth); cl = rth[-1]['c']
    # time of high
    hbar = max(rth, key=lambda b: b['h'])
    mins_to_high = (hbar['t'] - rth[0]['t']).total_seconds()/60
    # worst dip before the high (path-aware initial stop)
    pre = [b for b in rth if b['t'] <= hbar['t']]
    mae_before = (o - min(b['l'] for b in pre))/o*100
    # deepest pullback from a running peak (trailing-stop sizing)
    run_max = -1e9; max_pb = 0.0
    for b in rth:
        run_max = max(run_max, b['h'])
        if run_max > 0:
            max_pb = max(max_pb, (run_max - b['l'])/run_max*100)
    # entry-timing returns (price at target = open of bar at/after target, else last prior close)
    def ret_at(tt):
        at = [b for b in rth if b['t'].time() >= tt]
        if at: return (at[0]['o'] - o)/o*100
        return np.nan
    rets = {name: ret_at(tt) for name, tt in TARGETS}
    # VWAP signals
    cum_pv = cum_v = 0.0; below = 0; above15 = np.nan
    for b in rth:
        tp = (b['h']+b['l']+b['c'])/3; cum_pv += tp*b['v']; cum_v += b['v']
        vwap = cum_pv/cum_v if cum_v else np.nan
        if vwap == vwap and b['c'] < vwap: below += 1
        if b['t'].time() >= dt.time(9,45) and above15 != above15:  # first bar at/after 9:45
            above15 = 1 if (vwap==vwap and b['c'] > vwap) else 0
    return dict(session_open=round(o,4), session_high=round(hi,4), session_low=round(lo,4),
                session_close=round(cl,4), mfe_pct=round((hi-o)/o*100,2),
                mae_before_high_pct=round(mae_before,2), max_pullback_from_peak_pct=round(max_pb,2),
                ret_close_pct=round((cl-o)/o*100,2), mins_to_high=round(mins_to_high,0),
                **{k: (round(v,2) if v==v else None) for k,v in rets.items()},
                frac_below_vwap=round(below/len(rth),2), above_vwap_0945=above15,
                n_bars=len(rth), data_ok='OK' if len(rth) >= 20 else 'SPARSE')

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='inp', required=True)
    ap.add_argument('--out', dest='out', default=None)
    ap.add_argument('--timeframe', default='5Min')
    ap.add_argument('--gap-min', type=float, default=5)
    ap.add_argument('--gap-max', type=float, default=50)
    ap.add_argument('--label', default='all', choices=['all','W','L'])
    ap.add_argument('--premarket', action='store_true')
    ap.add_argument('--feed', default='iex')
    ap.add_argument('--limit-dates', type=int, default=0)
    ap.add_argument('--limit-tickers', type=int, default=0)
    ap.add_argument('--save-raw', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    out = a.out or f"intraday_metrics_{a.timeframe}_{dt.date.today():%d-%m-%y}.csv"

    print("Reading EOD tabs…", flush=True)
    pairs = collect(a.inp, a.gap_min, a.gap_max, a.label)
    by_date = defaultdict(list)
    for (tk, d), (lab, gap) in pairs.items(): by_date[d].append((tk, lab, gap))
    dates = sorted(by_date)
    if a.limit_dates: dates = dates[:a.limit_dates]
    n_pairs = sum(len(by_date[d]) for d in dates)
    print(f"Tradable ticker-days (gap {a.gap_min}-{a.gap_max}, label {a.label}): {n_pairs} across {len(dates)} dates", flush=True)
    print(f"Timeframe {a.timeframe} | feed {a.feed} | est ~{n_pairs* (78 if a.timeframe=='5Min' else 390):,} bars (RTH)", flush=True)
    if a.dry_run:
        print("DRY RUN — plan validated, no Alpaca calls.")
        for d in dates[:3]:
            print(f"  {d}: {len(by_date[d])} symbols -> {[t for t,_,_ in by_date[d]][:8]}{'…' if len(by_date[d])>8 else ''}")
        return

    key, secret = os.environ.get('ALPACA_API_KEY_ID'), os.environ.get('ALPACA_API_SECRET_KEY')
    if not key or not secret: sys.exit("ERROR: set ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY")

    rows, raw_frames, sparse, nodata = [], [], 0, 0
    for i, d in enumerate(dates, 1):
        want = by_date[d]
        if a.limit_tickers: want = want[:a.limit_tickers]
        syms = [t for t,_,_ in want]
        bars = fetch_day(syms, d, key, secret, a.timeframe, a.premarket, a.feed)
        for tk, lab, gap in want:
            m = metrics(bars.get(tk, []), d)
            if m is None: nodata += 1; continue
            if m['data_ok'] == 'SPARSE': sparse += 1
            rows.append(dict(ticker=tk, date=d.isoformat(), label=lab, gap=round(gap,2), **m))
            if a.save_raw:
                for b in bars.get(tk, []):
                    raw_frames.append(dict(ticker=tk, date=d.isoformat(), t=b['t'].isoformat(),
                                           o=b['o'], h=b['h'], l=b['l'], c=b['c'], v=b['v']))
        if i % 10 == 0 or i == len(dates):
            print(f"  {i}/{len(dates)} dates | rows={len(rows)} sparse={sparse} nodata={nodata}", flush=True)

    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    print(f"\nDone. ticker-days={len(df)}  sparse={sparse}  nodata={nodata}\nSaved -> {out}", flush=True)
    if a.save_raw and raw_frames:
        rawp = out.replace('.csv', '_rawbars.parquet')
        try:
            pd.DataFrame(raw_frames).to_parquet(rawp, index=False); print(f"Raw bars -> {rawp}")
        except Exception as e:
            pd.DataFrame(raw_frames).to_csv(rawp.replace('.parquet','.csv'), index=False); print(f"(parquet unavailable: {e}) raw -> csv")

if __name__ == '__main__':
    main()
