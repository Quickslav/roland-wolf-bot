from flask import Flask, request, jsonify
import os
import json
import threading
import time
import urllib.request
from datetime import datetime
import pytz
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

app = Flask(__name__)

# ─────────────────────────────────────────
# ALPACA CONFIGURATION
# ─────────────────────────────────────────
API_KEY    = "PKGABMMAXUYFY5NJCZIOT6XGLD"
SECRET_KEY = "9K8RUh1QA5jQ64jCzf6TL1SPFofh5LQMF1TQubWdyBAs"

trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

# ─────────────────────────────────────────
# HELPER — safe int conversion
# ─────────────────────────────────────────
def safe_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────
# SAFETY NET — closes all positions at 2PM ET
# Runs every 30 seconds, triggers between 1:58-2:02 PM ET
# Catches any positions TradingView failed to close
# ─────────────────────────────────────────
def safety_liquidation():
    et_tz = pytz.timezone("America/New_York")
    liquidated_today = False
    last_date = None

    while True:
        try:
            now_et = datetime.now(et_tz)
            today  = now_et.date()

            # Reset daily flag on new day
            if last_date != today:
                liquidated_today = False
                last_date = today

            # Trigger between 1:58 PM and 2:02 PM ET
            is_liquidation_window = (
                now_et.hour == 13 and now_et.minute >= 58
            ) or (
                now_et.hour == 14 and now_et.minute <= 2
            )

            if is_liquidation_window and not liquidated_today:
                positions = trading_client.get_all_positions()
                if positions:
                    print(f"[SAFETY NET] {len(positions)} open position(s) found at 2PM — closing all")
                    for position in positions:
                        try:
                            qty_held = abs(safe_int(position.qty))
                            order = MarketOrderRequest(
                                symbol        = position.symbol,
                                qty           = qty_held,
                                side          = OrderSide.SELL,
                                time_in_force = TimeInForce.DAY
                            )
                            trading_client.submit_order(order)
                            print(f"[SAFETY NET] Closed {qty_held} shares of {position.symbol}")
                        except Exception as e:
                            print(f"[SAFETY NET] Error closing {position.symbol}: {e}")
                    liquidated_today = True
                else:
                    print("[SAFETY NET] 2PM check — no open positions found")
                    liquidated_today = True

        except Exception as e:
            print(f"[SAFETY NET] Error: {e}")

        time.sleep(30)  # check every 30 seconds

threading.Thread(target=safety_liquidation, daemon=True).start()

# ─────────────────────────────────────────
# KEEP ALIVE — pings server every 10 mins
# ─────────────────────────────────────────
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:5000")

def keep_alive():
    time.sleep(15)
    while True:
        try:
            urllib.request.urlopen(f"{RENDER_URL}/")
            print("Keep-alive ping sent")
        except Exception as e:
            print(f"Keep-alive failed: {e}")
        time.sleep(600)

threading.Thread(target=keep_alive, daemon=True).start()

# ─────────────────────────────────────────
# WEBHOOK ENDPOINT
# ─────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        raw = request.get_data(as_text=True)
        data = json.loads(raw)
    except Exception:
        return jsonify({"error": "Could not parse JSON body"}), 400

    if not data:
        return jsonify({"error": "No data received"}), 400

    action = data.get('action', '').upper()
    symbol = data.get('ticker') or data.get('symbol')
    qty    = safe_int(data.get('qty', 100))
    stop   = data.get('stop')
    tp     = data.get('tp')

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action"}), 400

    if qty is None or qty <= 0:
        return jsonify({"error": f"Invalid qty value: {data.get('qty')}"}), 400

    print(f"Received: action={action}, symbol={symbol}, qty={qty}")

    try:
        # ── ENTRY ──
        if action in ("ENTRY", "BUY"):
            order = MarketOrderRequest(
                symbol        = symbol,
                qty           = qty,
                side          = OrderSide.BUY,
                time_in_force = TimeInForce.DAY
            )
            trading_client.submit_order(order)
            print(f"BUY order placed: {qty} shares of {symbol}")
            return jsonify({"message": f"BUY {qty} shares of {symbol}", "stop": stop, "tp": tp}), 200

        # ── EXIT ──
        elif action in ("EXIT", "SELL"):
            reason = data.get('reason', 'UNKNOWN')
            try:
                position = trading_client.get_open_position(symbol)
                qty_held = abs(safe_int(position.qty))
                order = MarketOrderRequest(
                    symbol        = symbol,
                    qty           = qty_held,
                    side          = OrderSide.SELL,
                    time_in_force = TimeInForce.DAY
                )
                trading_client.submit_order(order)
                print(f"SELL order placed: {qty_held} shares of {symbol} — reason: {reason}")
                return jsonify({"message": f"SELL {qty_held} shares of {symbol}", "reason": reason}), 200
            except Exception as e:
                print(f"No position found for {symbol}: {e}")
                return jsonify({"message": f"No open position for {symbol}"}), 200

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
