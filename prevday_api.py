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

Keys: reuses your webhook server's existing env vars (API_KEY_1 / SECRET_KEY_1),
or ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY if you prefer to set those instead.
Already in your Render Environment, so nothing new to configure.
Only market-DATA reads are used — it never touches the account or places orders.

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
import os, datetime as dt, json
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
    # Reuse the webhook server's existing key env vars; fall back to ALPACA_* names.
    key = os.environ.get('API_KEY_1') or os.environ.get('ALPACA_API_KEY_ID')
    sec = os.environ.get('SECRET_KEY_1') or os.environ.get('ALPACA_API_SECRET_KEY')
    if not key or not sec:
        return None, 'no API keys in environment (API_KEY_1/SECRET_KEY_1 or ALPACA_API_KEY_ID/_SECRET_KEY)'
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

# ---- grade-tiered position sizing (fed by the Gap Scorer) -------------------
# Dollar amount deployed per ticker per account, based on its latest scan grade.
SIZE_BY_GRADE = {"A": 20000, "B": 10000, "C": 5000}
DEFAULT_SIZE  = 10000          # ungraded ticker; set to 0 to only trade graded names
GRADES_FILE   = "grades.json"

def _load_grades():
    try:
        with open(GRADES_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def _save_grades(g):
    try:
        with open(GRADES_FILE, "w") as f:
            json.dump(g, f)
    except Exception:
        pass

GRADES = _load_grades()        # {"ABSI": "A", "RUN": "B", ...}

def size_for(symbol):
    """Dollars to deploy for a ticker, from its latest scan grade. Import this in the webhook."""
    return SIZE_BY_GRADE.get(GRADES.get(str(symbol).upper()), DEFAULT_SIZE)


def register(app):
    """Attach the prev-day + grade routes to an existing Flask app."""
    @app.route('/grades', methods=['POST', 'GET', 'OPTIONS'], endpoint='grade_store')
    def _grades():
        global GRADES
        if request.method == 'OPTIONS':
            return _cors(make_response('', 204))
        if request.method == 'GET':
            return _cors(jsonify(GRADES))
        body = request.get_json(force=True, silent=True) or {}
        incoming = body.get('grades', body)
        GRADES = {str(k).upper(): str(v).upper() for k, v in incoming.items()
                  if str(v).upper() in ('A', 'B', 'C')}
        _save_grades(GRADES)
        return _cors(jsonify({'stored': len(GRADES), 'sizes': SIZE_BY_GRADE, 'grades': GRADES}))

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
            code = 500 if 'no API keys' in err else 502
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
