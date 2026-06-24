#!/usr/bin/env python3
"""
simulate_exits.py
-----------------
Walk each mover-day's intraday path bar-by-bar from a 9:45 entry, apply a battery of
exit rules, and score them on REALIZED return across winners AND losers.

WHY the headline is not absolute P&L:
This universe is the biggest-mover list — ~94% winners. On a 94%-win universe, "hold to
close" looks unbeatable simply because almost everything wins. Live, at 9:45, you don't
know which gappers become movers, so the real win rate is lower. So the simulator reports,
per rule, the average return on WINNER-days and on LOSER-days separately, then expectancy
at a range of ASSUMED win rates:  E(p) = p*avgW + (1-p)*avgL.
Read the ranking at a realistic p (e.g. 0.55-0.70), not at the in-sample 0.94.

Inputs:
  --bars    rawbars file from fetch_intraday.py --save-raw  (.parquet or .csv)
  --metrics metrics CSV (for gap + label per ticker-day)

Entry: open of the 9:45 bar. Intra-bar order is PESSIMISTIC (a bar's low/stop is checked
before its high/target), so results are conservative, not rosy. Slippage/commissions = 0.
5-min bars miss intra-bar spikes, so real stops fill a touch worse than modeled.

Usage:
  python simulate_exits.py --bars intraday_metrics_5Min_24-06-26_rawbars.csv \
                           --metrics intraday_metrics_5Min_24-06-26.csv
"""
import argparse, datetime as dt
import numpy as np, pandas as pd

ENTRY = dt.time(9, 45)

def gap_bin(g):
    if g < 10: return '5-10'
    if g < 20: return '10-20'
    if g < 35: return '20-35'
    return '35-50'

# gap-scaled parameters (from the morning-low + pullback findings)
INIT_SCALED = {'5-10': 5, '10-20': 5, '20-35': 10, '35-50': None}     # None = don't trade
TRAIL_SCALED = {'5-10': 12, '10-20': 12, '20-35': 15, '35-50': None}

# rule = dict(init=, trail=, target=, so_frac=, so_target=, time_exit=, vwap=, scaled=)
RULES = {
    'hold_close':        dict(),
    'exit_1130':         dict(time_exit=dt.time(11,30)),
    'exit_1400':         dict(time_exit=dt.time(14,0)),
    'stop_8':            dict(init=8),
    'stop_gapscaled':    dict(init='scaled'),
    'trail_8':           dict(trail=8),
    'trail_12':          dict(trail=12),
    'trail_15':          dict(trail=15),
    'trail_gapscaled':   dict(trail='scaled'),
    'init+trail_scaled': dict(init='scaled', trail='scaled'),
    'target_12':         dict(target=12),
    'scaleout12_trail12':dict(so_frac=0.5, so_target=12, trail=12),
    'scaleout_scaled':   dict(init='scaled', so_frac=0.5, so_target=12, trail='scaled'),
    'vwap_exit':         dict(vwap=True),
}

def walk(bars, rule, gb):
    """bars: list of dicts (t,o,h,l,c,v) from the entry bar onward, sorted. Returns pct return or None (no-trade)."""
    init = rule.get('init'); trail = rule.get('trail')
    if init == 'scaled': init = INIT_SCALED[gb]
    if trail == 'scaled': trail = TRAIL_SCALED[gb]
    if (rule.get('init') == 'scaled' and init is None) or (rule.get('trail') == 'scaled' and trail is None):
        return None  # gap-scaled rule says skip this gap bin
    ep = bars[0]['o']
    if ep <= 0: return None
    peak = ep; pos = 1.0; realized = 0.0; scaled_done = False
    cum_pv = cum_v = 0.0
    last_c = bars[-1]['c']
    for b in bars:
        # time exit
        te = rule.get('time_exit')
        if te and b['t'].time() >= te:
            realized += pos*(b['o']-ep)/ep*100; pos = 0; break
        # ---- pessimistic: downside first ----
        if init and b['l'] <= ep*(1-init/100):
            realized += pos*(ep*(1-init/100)-ep)/ep*100; pos = 0; break
        if trail and b['l'] <= peak*(1-trail/100):
            realized += pos*(peak*(1-trail/100)-ep)/ep*100; pos = 0; break
        # vwap exit (close below running vwap)
        if rule.get('vwap'):
            tp = (b['h']+b['l']+b['c'])/3; cum_pv += tp*b['v']; cum_v += b['v']
            vwap = cum_pv/cum_v if cum_v else ep
            if b['c'] < vwap:
                realized += pos*(b['c']-ep)/ep*100; pos = 0; break
        # ---- upside ----
        peak = max(peak, b['h'])
        if rule.get('so_frac') and not scaled_done and b['h'] >= ep*(1+rule['so_target']/100):
            realized += rule['so_frac']*(ep*(1+rule['so_target']/100)-ep)/ep*100
            pos -= rule['so_frac']; scaled_done = True
        if rule.get('target') and b['h'] >= ep*(1+rule['target']/100):
            realized += pos*(ep*(1+rule['target']/100)-ep)/ep*100; pos = 0; break
    if pos > 0:
        realized += pos*(last_c-ep)/ep*100
    return realized

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bars', required=True); ap.add_argument('--metrics', required=True)
    a = ap.parse_args()
    raw = pd.read_parquet(a.bars) if a.bars.endswith('.parquet') else pd.read_csv(a.bars)
    raw['t'] = pd.to_datetime(raw['t'])
    meta = pd.read_csv(a.metrics)
    meta = meta[meta['data_ok']=='OK'][['ticker','date','label','gap']].copy()
    meta['gap'] = pd.to_numeric(meta['gap'], errors='coerce')

    # group bars by ticker-day
    raw['key'] = raw['ticker'].astype(str)+'|'+raw['date'].astype(str)
    groups = {k: g.sort_values('t') for k, g in raw.groupby('key')}

    results = {r: {'W': [], 'L': []} for r in RULES}
    for _, row in meta.iterrows():
        key = f"{row['ticker']}|{row['date']}"
        g = groups.get(key)
        if g is None or pd.isna(row['gap']): continue
        # entry bar onward (ET times already baked into t as tz-aware ISO)
        gt = g[g['t'].dt.time >= ENTRY]
        if len(gt) < 2: continue
        bars = [{'t': r.t, 'o': r.o, 'h': r.h, 'l': r.l, 'c': r.c, 'v': r.v} for r in gt.itertuples()]
        gb = gap_bin(row['gap'])
        for rname, rule in RULES.items():
            ret = walk(bars, rule, gb)
            if ret is not None:
                results[rname][row['label']].append(ret)

    rows = []
    for rname, d in results.items():
        W = np.array(d['W']); L = np.array(d['L'])
        allr = np.concatenate([W, L]) if len(W)+len(L) else np.array([])
        if not len(allr): continue
        avgW = W.mean() if len(W) else np.nan
        avgL = L.mean() if len(L) else np.nan
        def E(p): return round(p*avgW + (1-p)*(avgL if avgL==avgL else 0), 2)
        rows.append({'rule': rname, 'n': len(allr), 'win_%': round((allr>0).mean()*100,1),
                     'avg_all': round(allr.mean(),2), 'med': round(np.median(allr),2),
                     'avgW': round(avgW,2) if avgW==avgW else None,
                     'avgL': round(avgL,2) if avgL==avgL else None,
                     'E@94': E(0.94), 'E@70': E(0.70), 'E@60': E(0.60), 'E@50': E(0.50)})
    out = pd.DataFrame(rows)
    pd.set_option('display.width', 200, 'display.max_columns', 30)
    print("Exit-rule simulation — entry 9:45, conservative intra-bar fills")
    print(f"(in-sample win rate = {round((meta.label=='W').mean()*100,1)}% — read the E@ columns at a realistic lower p)\n")
    print("Ranked by expectancy at an assumed 60% win rate:\n")
    print(out.sort_values('E@60', ascending=False).to_string(index=False))
    print("\nE@p = p*avgW + (1-p)*avgL.  avgL is the loss side — the rules that hold up at E@60/E@50")
    print("are the ones that cut losers, not the ones that ride the 94%-winner in-sample tape.")

if __name__ == '__main__':
    main()
