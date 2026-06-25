#!/usr/bin/env python3
"""
prevday_api.py — tiny proxy so the browser Gap Scorer can get the prev-day layer
without ever holding your Alpaca keys.

The browser POSTs a list of tickers; this reads your keys from the Render environment
(never from the browser), pulls each ticker's prior completed session from Alpaca, and
returns the three signals the scorer needs:
  exh  = (prev_high - prev_close)/prev_close * 100   (exhaustion: >50 kill, 25-50 caution)
  cpos = (prev_close - prev_low)/(prev_high - prev_low)   (1 = closed at high)
  rng  = (prev_high - prev_low)/prev_close * 100     (prior-day range, for trap-risk)

Set keys ONCE in Render's dashboard (Environment tab), so they survive deploys:
  ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY
Only market-DATA scope is used here — it never touches the account or places orders.

Run as a Render web service:  gunicorn prevday_api:app   (or: python prevday_api.py)
Then paste the service URL into the Gap Scorer once (e.g. https://roland-wolf-bot.onrender.com).
"""
import os, datetime as dt
from collections import defaultdict
from flask import Flask, request, jsonify, make_response
import requests

app = Flask(__name__)
BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"

def _cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
    return resp

@app.route('/prevday', methods=['POST', 'OPTIONS'])
def prevday():
    if request.method == 'OPTIONS':
        return _cors(make_response('', 204))
    key = os.environ.get('ALPACA_API_KEY_ID')
    sec = os.environ.get('ALPACA_API_SECRET_KEY')
    if not key or not sec:
        return _cors(jsonify({'error': 'ALPACA keys not set in environment'})), 500
    body = request.get_json(force=True, silent=True) or {}
    tickers = [str(t).upper().strip() for t in body.get('tickers', []) if t]
    feed = body.get('feed', 'iex')
    out = {}
    if tickers:
        hdr = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': sec}
        end = dt.date.today(); start = end - dt.timedelta(days=20)
        bars = defaultdict(dict)
        for i in range(0, len(tickers), 100):
            batch = tickers[i:i+100]; token = None
            while True:
                params = {'symbols': ','.join(batch), 'timeframe': '1Day',
                          'start': start.isoformat(), 'end': end.isoformat(),
                          'feed': feed, 'adjustment': 'raw', 'limit': 10000}
                if token: params['page_token'] = token
                try:
                    r = requests.get(BARS_URL, params=params, headers=hdr, timeout=30)
                except Exception as e:
                    return _cors(jsonify({'error': 'fetch failed: ' + str(e)[:120]})), 502
                if r.status_code != 200:
                    return _cors(jsonify({'error': 'alpaca ' + str(r.status_code), 'detail': r.text[:160]})), 502
                js = r.json()
                for sym, bl in (js.get('bars') or {}).items():
                    for b in bl:
                        d = dt.datetime.fromisoformat(b['t'].replace('Z', '+00:00')).date()
                        bars[sym][d] = b
                token = js.get('next_page_token')
                if not token: break
        for sym, bd in bars.items():
            if not bd: continue
            d = max(bd); b = bd[d]; ph, pl, pc = b['h'], b['l'], b['c']
            if ph >= pl > 0 and pc > 0 and not pc > 2*ph:        # glitch filter
                out[sym] = {'exh': round((ph-pc)/pc*100, 2),
                            'cpos': round((pc-pl)/(ph-pl), 3) if ph > pl else None,
                            'rng': round((ph-pl)/pc*100, 2), 'ok': True}
    return _cors(jsonify(out))

@app.route('/health')
def health():
    return _cors(jsonify({'ok': True}))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
