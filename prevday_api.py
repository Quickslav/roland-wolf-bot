#!/usr/bin/env python3
"""
prevday_api.py — gives the browser Gap Scorer the prev-day layer without ever
letting your Alpaca keys touch the browser.

The browser POSTs a list of tickers; this reads your keys from the environment
(never from the browser), pulls each ticker's prior completed session from Alpaca,
and returns three signals:
  exh  = (prev_high - prev_close)/prev_close * 100   (exhaustion: >50 kill, 25-50 caution)
  cpos = (prev_close - prev_low)/(prev_high - prev_low)   (1 = closed at the high)
  rng  = (prev_high - prev_low)/prev_close * 100     (prior-day range, for trap-risk)

Set keys ONCE in Render's dashboard (Environment tab) so they survive deploys:
  ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY
Only market-DATA scope is used — it never touches the account or places orders.

TWO WAYS TO RUN IT
------------------
A) Graft onto your existing Flask app (recommended — keeps your current app):
     in your main app file, right after `app = Flask(__name__)` add:
         from prevday_api import register
         register(app)
     then commit + redeploy. Your existing URL now also serves /prevday.

B) Run it standalone as its own service:
     Start Command:  gunicorn prevday_api:app
"""
import os, datetime as dt
from collections import defaultdict
from flask import Flask, request, jsonify, make_response
import requests

BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"

def _cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
    return resp

def _fetch_prevday(tickers, feed='iex'):
    key = os.environ.get('ALPACA_API_KEY_ID')
    sec = os.environ.get('ALPACA_API_SECRET_KEY')
    if not key or not sec:
        return None, 'ALPACA keys not set in environment'
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
                return None, 'fetch failed: ' + str(e)[:120]
            if r.status_code != 200:
                return None, 'alpaca ' + str(r.status_code) + ': ' + r.text[:140]
            js = r.json()
            for sym, bl in (js.get('bars') or {}).items():
                for b in bl:
                    d = dt.datetime.fromisoformat(b['t'].replace('Z', '+00:00')).date()
                    bars[sym][d] = b
            token = js.get('next_page_token')
            if not token: break
    out = {}
    for sym, bd in bars.items():
        if not bd: continue
        d = max(bd); b = bd[d]; ph, pl, pc = b['h'], b['l'], b['c']
        if ph >= pl > 0 and pc > 0 and not pc > 2*ph:          # glitch filter
            out[sym] = {'exh': round((ph-pc)/pc*100, 2),
                        'cpos': round((pc-pl)/(ph-pl), 3) if ph > pl else None,
                        'rng': round((ph-pl)/pc*100, 2), 'ok': True}
    return out, None

def register(app):
    """Attach the prev-day routes to an existing Flask app."""
    @app.route('/prevday', methods=['POST', 'OPTIONS'], endpoint='prevday_proxy')
    def _prevday():
        if request.method == 'OPTIONS':
            return _cors(make_response('', 204))
        body = request.get_json(force=True, silent=True) or {}
        tickers = [str(t).upper().strip() for t in body.get('tickers', []) if t]
        feed = body.get('feed', 'iex')
        if not tickers:
            return _cors(jsonify({}))
        out, err = _fetch_prevday(tickers, feed)
        if err:
            code = 500 if 'keys not set' in err else 502
            return _cors(jsonify({'error': err})), code
        return _cors(jsonify(out))

    @app.route('/pd_health', endpoint='prevday_health')
    def _pd_health():
        return _cors(jsonify({'ok': True}))
    return app

# standalone mode
app = Flask(__name__)
register(app)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
