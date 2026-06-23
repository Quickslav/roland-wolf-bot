#!/usr/bin/env python3
"""
analyze_prevday.py
------------------
Run AFTER fill_eod_prevday.py has populated the prev-day columns.
Tests, on real data:

  1. EXHAUSTION SIGNAL  — does "prev day's HOD > 25% above prev close" predict that
     today's mover fails (lower win rate, bigger fade)? This is the strongest single
     signal in the ruleset; here it gets measured directly.
  2. GRADED exhaustion  — win-rate / fade across prev-HOD-above-close buckets.
  3. TRAP TELL          — among today's gap-ups (>=5%), do the ones that ended LOSERS
     have a distinct prev-day footprint (already exhausted / closed weak the day before)?
  4. PRIOR-DAY CLOSE    — did closing strong vs weak yesterday matter today?

Usage:
    python analyze_prevday.py --in <filled_workbook>.xlsx
"""
import argparse
import numpy as np
import pandas as pd

EOD = {'EOD Biggest Winners Q1': (2, 'W'), 'EOD Biggest Losers Q1': (2, 'L'),
       'EOD Biggest Winners Q2': (2, 'W'), 'EOD Biggest Losers Q2': (3, 'L')}

def load(path):
    xl = pd.ExcelFile(path)
    frames = []
    for s, (h, lab) in EOD.items():
        df = pd.read_excel(xl, sheet_name=s, header=h)
        df.columns = [str(c).strip() for c in df.columns]
        df = df.rename(columns={'Change %': 'Change', 'Gap %': 'Gap'})
        df = df[df['Ticker'].notna()].copy()
        df['label'] = lab
        frames.append(df)
    d = pd.concat(frames, ignore_index=True, sort=False)
    num = ['Gap', 'Change', 'Open', 'High', 'Low', 'Price', 'Prev Close', 'Volume', 'Trades',
           'Prev High', 'Prev Low', 'Prev Volume', 'Prev Range %', 'Prev HOD>Close %', 'Prev Close Pos']
    for c in num:
        if c in d:
            d[c] = pd.to_numeric(d[c], errors='coerce')
    # current-day derived (outcomes — used to characterise, not predict)
    o, h, l, p = d['Open'], d['High'], d['Low'], d['Price']
    d['fade'] = (h - p) / h * 100
    d['close_pos'] = np.where((h - l) > 0, (p - l) / (h - l), np.nan)
    # only rows with valid prev-day data
    if 'Prev Data OK' in d:
        d = d[d['Prev Data OK'].astype(str).str.upper().isin(['OK', 'CHECK'])].copy()
    d = d.dropna(subset=['Prev HOD>Close %'])
    return d

def grp(df):
    return pd.Series({
        'n': len(df),
        'winner_%': round((df.label == 'W').mean() * 100, 1),
        'med_change': round(df['Change'].median(), 1),
        'med_fade': round(df['fade'].median(), 1),
        'med_close_pos': round(df['close_pos'].median(), 2),
    })

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='inp', required=True)
    a = ap.parse_args()
    d = load(a.inp)
    print(f"Rows with valid prev-day data: {len(d)}  (W={int((d.label=='W').sum())}, L={int((d.label=='L').sum())})\n")

    print("=" * 72)
    print("TEST 1 — EXHAUSTION SIGNAL: prev day's HOD > 25% above prev close")
    print("=" * 72)
    d['exhausted'] = d['Prev HOD>Close %'] > 25
    print(d.groupby('exhausted').apply(grp, include_groups=False).to_string())
    g5 = d[d['Gap'] >= 5]
    print("\n  Restricted to today's gap-ups >= 5% (the tradable universe):")
    print(g5.groupby('exhausted').apply(grp, include_groups=False).to_string())

    print("\n" + "=" * 72)
    print("TEST 2 — GRADED: outcome by how far prev HOD was above prev close")
    print("=" * 72)
    d['exh_bin'] = pd.cut(d['Prev HOD>Close %'], [-1e9, 0, 10, 25, 50, 1e9],
                          labels=['<=0', '0-10', '10-25', '25-50', '50+'])
    print(d.groupby('exh_bin', observed=True).apply(grp, include_groups=False).to_string())

    print("\n" + "=" * 72)
    print("TEST 3 — TRAP TELL: gap-up >=5% winners vs losers, prev-day footprint")
    print("=" * 72)
    trap = g5[g5.label == 'L']; win = g5[g5.label == 'W']
    print(f"  traps n={len(trap)}  |  winners n={len(win)}")
    for c, lab in [('Prev HOD>Close %', 'prev HOD above close %'),
                   ('Prev Range %', 'prev day range %'),
                   ('Prev Close Pos', 'prev close-in-range (1=strong)')]:
        if c in d:
            print(f"  {lab:34s} trap={trap[c].median():>8.2f}   winner={win[c].median():>8.2f}")

    print("\n" + "=" * 72)
    print("TEST 4 — PRIOR-DAY CLOSE STRENGTH vs today's outcome")
    print("=" * 72)
    d['prev_close_strong'] = d['Prev Close Pos'] > 0.5
    print(d.groupby('prev_close_strong').apply(grp, include_groups=False).to_string())

    print("\nNote: winner_%/change are direction+magnitude (stock). fade/close_pos describe")
    print("intraday character — high fade = hard to capture even when the stock closes green.")

if __name__ == '__main__':
    main()
