from flask import Flask, request, jsonify
import alpaca_trade_api as tradeapi
import os
import json
import threading
import time
import urllib.request

app = Flask(__name__)

# ─────────────────────────────────────────
# ALPACA CONFIGURATION
# ─────────────────────────────────────────
API_KEY    = "PKGABMMAXUYFY5NJCZIOT6XGLD"
SECRET_KEY = "9K8RUh1QA5jQ64jCzf6TL1SPFofh5LQMF1TQubWdyBAs"
BASE_URL   = "https://paper-api.alpaca.markets"

api = tradeapi.REST(API_KEY, SECRET_KEY, BASE_URL, api_version='v2')

# ─────────────────────────────────────────
# KEEP ALIVE — pings server every 10 mins
# Prevents Render free tier from sleeping
# ─────────────────────────────────────────
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:5000")

def keep_alive():
    while True:
        try:
            urllib.request.urlopen(f"{RENDER_URL}/")
            print("Keep-alive ping sent")
        except Exception as e:
            print(f"Keep-alive failed: {e}")
        time.sleep(600)  # ping every 10 minutes

threading.Thread(target=keep_alive, daemon=True).start()

# ─────────────────────────────────────────
# WEBHOOK ENDPOINT
# Accepts both application/json and text/plain
# ─────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    # FIX: Parse JSON regardless of Content-Type header
    try:
        raw = request.get_data(as_text=True)
        data = json.loads(raw)
    except Exception:
        return jsonify({"error": "Could not parse JSON body"}), 400

    if not data:
        return jsonify({"error": "No data received"}), 400

    action = data.get('action', '').upper()
    symbol = data.get('ticker') or data.get('symbol')
    qty    = data.get('qty', 100)
    stop   = data.get('stop')
    tp     = data.get('tp')

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action"}), 400

    print(f"Received: action={action}, symbol={symbol}, qty={qty}")

    try:
        # ── ENTRY ──
        if action == "ENTRY":
            api.submit_order(
                symbol        = symbol,
                qty           = int(qty),
                side          = 'buy',
                type          = 'market',
                time_in_force = 'day'
            )
            print(f"BUY order placed: {qty} shares of {symbol}")
            return jsonify({"message": f"BUY {qty} shares of {symbol}", "stop": stop, "tp": tp}), 200

        # ── EXIT ──
        elif action == "EXIT":
            reason = data.get('reason', 'UNKNOWN')
            try:
                position = api.get_position(symbol)
                qty_held = abs(int(float(position.qty)))
                api.submit_order(
                    symbol        = symbol,
                    qty           = qty_held,
                    side          = 'sell',
                    type          = 'market',
                    time_in_force = 'day'
                )
                print(f"SELL order placed: {qty_held} shares of {symbol} — reason: {reason}")
                return jsonify({"message": f"SELL {qty_held} shares of {symbol}", "reason": reason}), 200
            except Exception as e:
                print(f"No position found for {symbol}: {e}")
                return jsonify({"message": f"No open position for {symbol}"}), 200

        # ── LEGACY SUPPORT ──
        elif action == "BUY":
            api.submit_order(
                symbol        = symbol,
                qty           = int(qty),
                side          = 'buy',
                type          = 'market',
                time_in_force = 'day'
            )
            return jsonify({"message": f"BUY {qty} shares of {symbol}"}), 200

        elif action == "SELL":
            api.submit_order(
                symbol        = symbol,
                qty           = int(qty),
                side          = 'sell',
                type          = 'market',
                time_in_force = 'day'
            )
            return jsonify({"message": f"SELL {qty} shares of {symbol}"}), 200

        else:
            return jsonify({"error": f"Unknown action: {action}"}), 400

    except Exception as e:
        print(f"Error placing order: {e}")
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.route('/', methods=['GET'])
def health():
    return jsonify({"status": "Webhook server is running!"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
