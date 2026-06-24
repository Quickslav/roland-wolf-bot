#!/usr/bin/env python3
"""
analyze_intraday.py
-------------------
Run on the metrics CSV from fetch_intraday.py. Answers the three knobs daily data couldn't:

  1. ENTRY TIMING  — where is the move at 9:35/9:45/10:00…, and WHEN does the high land?
                     Is 9:45 well-placed, or are you buying into the top?
  2. TRAILING STOP — how deep is a winner's worst pullback FROM A RUNNING PEAK, by gap bin?
                     The trail distance that survives X% of winners.
  3. TRAP TELL     — does holding above VWAP / being green at 9:45 separate winners from
                     losers in real time?

Usage: python analyze_intraday.py --in intraday_metrics_5Min_<date>.csv
"""
import argparse
import numpy as np, pandas as pd

CHECK = ['ret_0935','ret_0945','ret_1000','ret_1030','ret_1100','ret_1130']

def p(x,q): return round(np.nanpercentile(pd.to_numeric(x,errors='coerce').dropna(),q),1) if len(pd.to_numeric(x,errors='coerce').dropna()) else float('nan')

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--in', dest='inp', required=True)
    d = pd.read_csv(ap.parse_args().inp)
    d = d[d['data_ok']=='OK'].copy()
    for c in CHECK+['mfe_pct','max_pullback_from_peak_pct','mae_before_high_pct','mins_to_high','ret_close_pct','frac_below_vwap','gap','am_low_from_open_pct','postentry_low_pct','am_low_min']:
        if c in d.columns: d[c] = pd.to_numeric(d[c], errors='coerce')
    W = d[d.label=='W'].copy(); L = d[d.label=='L'].copy()
    W['gb'] = pd.cut(W.gap,[5,10,20,35,50],labels=['5-10','10-20','20-35','35-50'])
    L['gb'] = pd.cut(L.gap,[5,10,20,35,50],labels=['5-10','10-20','20-35','35-50'])
    print(f"Clean ticker-days: {len(d)}  (winners={len(W)}, losers={len(L)})\n")

    print("="*74)
    print("1. ENTRY TIMING — winners' median return vs OPEN at each clock checkpoint")
    print("="*74)
    rows=[]
    for gb,g in W.groupby('gb',observed=True):
        r={'gap':gb,'n':len(g)}
        for c in CHECK: r[c.replace('ret_','')] = p(g[c],50)
        r['HIGH(med%)']=p(g.mfe_pct,50); r['min_to_high']=p(g.mins_to_high,50)
        rows.append(r)
    print(pd.DataFrame(rows).to_string(index=False))
    print(f"\n  When does the high land?  winners median {p(W.mins_to_high,50):.0f} min after open "
          f"(p25={p(W.mins_to_high,25):.0f}, p75={p(W.mins_to_high,75):.0f}).")
    frac_high_by_945 = (W.mins_to_high<=15).mean()*100
    print(f"  Winners whose high is in by ~9:45 (<=15 min): {frac_high_by_945:.0f}%  "
          f"-> that fraction, a 9:45 entry is buying the fade.")
    print(f"  Winners still near flat at 9:45 (ret_0945 < +3%): {(W.ret_0945<3).mean()*100:.0f}% "
          f"-> runway left for a 9:45 entry.")

    print("\n"+"="*74)
    print("2. MORNING PULLBACK (open -> midday) — the dip prices, winners vs losers")
    print("="*74)
    if 'am_low_from_open_pct' in d.columns:
        print("  Deepest dip below the OPEN before midday (% below open):")
        rows=[]
        for gb in ['5-10','10-20','20-35','35-50']:
            gw=W[W.gb==gb]; gl=L[L.gb==gb]
            rows.append({'gap':gb,'W_n':len(gw),'W_med':p(gw.am_low_from_open_pct,50),'W_p75':p(gw.am_low_from_open_pct,75),
                         'L_n':len(gl),'L_med':p(gl.am_low_from_open_pct,50)})
        print(pd.DataFrame(rows).to_string(index=False))
        print("\n  Heat AFTER a 9:45 entry to midday (% against you from the 9:45 price):")
        rows=[]
        for gb in ['5-10','10-20','20-35','35-50']:
            gw=W[W.gb==gb]
            rows.append({'gap':gb,'n':len(gw),'med':p(gw.postentry_low_pct,50),'p75':p(gw.postentry_low_pct,75),'p90':p(gw.postentry_low_pct,90)})
        print(pd.DataFrame(rows).to_string(index=False))
        print("\n  INITIAL-STOP SEPARATION (open->midday): a stop D% below open —")
        print("  W_keep = % of winners that DON'T hit it (survive)  |  L_cut = % of losers that DO (cut early)")
        sep=[]
        for D in [3,5,8,10,12,15,20]:
            wk=(W.am_low_from_open_pct<=D).mean()*100
            lc=(L.am_low_from_open_pct>D).mean()*100
            sep.append({'stop_%':D,'W_keep':round(wk,1),'L_cut':round(lc,1),'edge(W_keep+L_cut-100)':round(wk+lc-100,1)})
        print(pd.DataFrame(sep).to_string(index=False))
        print(f"  (losers n={len(L)} — thin; treat L_cut as directional. Edge peaks where winners survive but losers don't.)")

    print("\n"+"="*74)
    print("3. TRAILING STOP — deepest pullback from a running peak (winners), by gap bin")
    print("="*74)
    rows=[]
    for gb,g in W.groupby('gb',observed=True):
        rows.append({'gap':gb,'n':len(g),'pullback_med':p(g.max_pullback_from_peak_pct,50),
                     'pullback_p75':p(g.max_pullback_from_peak_pct,75),'pullback_p90':p(g.max_pullback_from_peak_pct,90)})
    print(pd.DataFrame(rows).to_string(index=False))
    print("\n  Trail-survival: % of winners whose worst peak-pullback stayed within the trail")
    surv=[]
    for tr in [3,5,8,10,12,15,20]:
        row={'trail_%':tr,'all_W':round((W.max_pullback_from_peak_pct<=tr).mean()*100,1)}
        for gb,g in W.groupby('gb',observed=True):
            row[str(gb)]=round((g.max_pullback_from_peak_pct<=tr).mean()*100,1)
        surv.append(row)
    print(pd.DataFrame(surv).to_string(index=False))

    print("\n"+"="*74)
    print("4. TRAP TELL — does 9:45 VWAP / green separate winners from losers?")
    print("="*74)
    both = d[d['above_vwap_0945'].notna()].copy()
    both['above_vwap_0945']=pd.to_numeric(both['above_vwap_0945'],errors='coerce')
    def wr(x): return pd.Series({'n':len(x),'winner_%':round((x.label=='W').mean()*100,1),
                                 'med_close':round(pd.to_numeric(x.ret_close_pct,errors='coerce').median(),1)})
    print("  Split by ABOVE VWAP at 9:45:")
    print(both.groupby('above_vwap_0945').apply(wr, include_groups=False).to_string())
    both['green_0945']=pd.to_numeric(both['ret_0945'],errors='coerce')>0
    print("\n  Split by GREEN vs open at 9:45:")
    print(both.groupby('green_0945').apply(wr, include_groups=False).to_string())
    print(f"\n  Losers spend a median {p(L.frac_below_vwap,50)*1:.0%} of the session below VWAP "
          f"vs winners {p(W.frac_below_vwap,50)*1:.0%}.")

    print("\nNote: pullback-from-peak sizes the TRAIL; mae_before_high (in the CSV) sizes the initial stop.")
    print("5-min bars miss intra-bar spikes, so true pullbacks run slightly deeper than shown.")

if __name__ == '__main__':
    main()
