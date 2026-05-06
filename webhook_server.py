from flask import Flask, request, jsonify
import os
import json
import threading
import time
import urllib.request
from datetime import datetime
import pytz
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetLatestTradeRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

app = Flask(__name__)

# ─────────────────────────────────────────
# ALPACA ACCOUNTS
# Account 1 — liquidates at 2:00 PM ET
# Account 2 — liquidates at 11:30 AM ET
# ─────────────────────────────────────────
API_KEY_1    = "PKGABMMAXUYFY5NJCZIOT6XGLD"
SECRET_KEY_1 = "9K8RUh1QA5jQ64jCzf6TL1SPFofh5LQMF1TQubWdyBAs"

API_KEY_2    = "PK6Q5L6JMLIJYYQGPUBUNQLNU5"
SECRET_KEY_2 = "HdrTT2wNELMFKm6xZHymKCLbiaLCgC5dspUv6HDuGEWx"

ACCOUNT_1 = TradingClient(API_KEY_1, SECRET_KEY_1, paper=True)
ACCOUNT_2 = TradingClient(API_KEY_2, SECRET_KEY_2, paper=True)

# Data client for fetching live prices (uses account 1 keys)
DATA_CLIENT = StockHistoricalDataClient(API_KEY_1, SECRET_KEY_1)

ACCOUNTS = {
    "account1": {"client": ACCOUNT_1, "name": "Account 1 (2PM Exit)"},
    "account2": {"client": ACCOUNT_2, "name": "Account 2 (11:30AM Exit)"},
}

TRADE_AMOUNT = 10000  # $10,000 per ticker per account

# ─────────────────────────────────────────
# HELPER — safe int conversion
# ─────────────────────────────────────────
def safe_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────
# HELPER — get live price using correct alpaca-py method
# ─────────────────────────────────────────
def get_live_price(symbol):
    try:
        request_params = StockLatestTradeRequest(symbol_or_symbols=symbol)
        response       = DATA_CLIENT.get_stock_latest_trade(request_params)
        price          = float(response[symbol].price)
        return price
    except Exception as e:
        print(f"Error fetching price for {symbol}: {e}")
        return None

# ─────────────────────────────────────────
# HELPER — place order
# ─────────────────────────────────────────
def place_order(client, symbol, qty, side):
    order = MarketOrderRequest(
        symbol        = symbol,
        qty           = qty,
        side          = side,
        time_in_force = TimeInForce.DAY
    )
    return client.submit_order(order)

# ─────────────────────────────────────────
# SAFETY NET LIQUIDATION
# Account 1 — 2:00 PM ET
# Account 2 — 11:30 AM ET
# ─────────────────────────────────────────
def safety_liquidation():
    et_tz = pytz.timezone("America/New_York")
    liquidated_today = {"account1": False, "account2": False}
    last_date = None

    while True:
        try:
            now_et = datetime.now(et_tz)
            today  = now_et.date()

            if last_date != today:
                liquidated_today = {"account1": False, "account2": False}
                last_date = today

            acc2_window = (now_et.hour == 11 and now_et.minute >= 28 and now_et.minute <= 32)
            acc1_window = (now_et.hour == 13 and now_et.minute >= 58) or (now_et.hour == 14 and now_et.minute <= 2)

            for acc_key, window in [("account2", acc2_window), ("account1", acc1_window)]:
                if window and not liquidated_today[acc_key]:
                    client = ACCOUNTS[acc_key]["client"]
                    name   = ACCOUNTS[acc_key]["name"]
                    try:
                        positions = client.get_all_positions()
                        if positions:
                            print(f"[SAFETY NET] {name} — closing {len(positions)} position(s)")
                            for position in positions:
                                try:
                                    qty_held = abs(safe_int(position.qty))
                                    place_order(client, position.symbol, qty_held, OrderSide.SELL)
                                    print(f"[SAFETY NET] {name} — closed {qty_held} shares of {position.symbol}")
                                except Exception as e:
                                    print(f"[SAFETY NET] {name} — error closing {position.symbol}: {e}")
                        else:
                            print(f"[SAFETY NET] {name} — no open positions")
                    except Exception as e:
                        print(f"[SAFETY NET] {name} — error: {e}")
                    liquidated_today[acc_key] = True

        except Exception as e:
            print(f"[SAFETY NET] Error: {e}")

        time.sleep(30)

threading.Thread(target=safety_liquidation, daemon=True).start()

# ─────────────────────────────────────────
# KEEP ALIVE
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
        raw  = request.get_data(as_text=True)
        data = json.loads(raw)
    except Exception:
        return jsonify({"error": "Could not parse JSON body"}), 400

    if not data:
        return jsonify({"error": "No data received"}), 400

    action = data.get('action', '').upper()
    symbol = data.get('ticker') or data.get('symbol')

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action"}), 400

    print(f"Received: action={action}, symbol={symbol}")

    results = {}

    # ── ENTRY ──
    if action in ("ENTRY", "BUY"):
        # Fetch live price once — use for both accounts
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        shares = int(TRADE_AMOUNT / price)
        if shares <= 0:
            return jsonify({"error": f"Price too high to buy ${TRADE_AMOUNT} worth"}), 400

        print(f"Live price for {symbol}: ${price:.4f} — buying {shares} shares")

        for acc_key, acc in ACCOUNTS.items():
            client = acc["client"]
            name   = acc["name"]
            try:
                place_order(client, symbol, shares, OrderSide.BUY)
                print(f"[{name}] BUY {shares} shares of {symbol} @ ~${price:.4f}")
                results[acc_key] = f"BUY {shares} shares of {symbol} @ ~${price:.4f}"
            except Exception as e:
                print(f"[{name}] Error on BUY: {e}")
                results[acc_key] = f"Error: {str(e)}"

        return jsonify({"message": "Entry processed", "results": results}), 200

    # ── EXIT ──
    elif action in ("EXIT", "SELL"):
        reason = data.get('reason', 'UNKNOWN')
        for acc_key, acc in ACCOUNTS.items():
            client = acc["client"]
            name   = acc["name"]
            try:
                position = client.get_open_position(symbol)
                qty_held = abs(safe_int(position.qty))
                place_order(client, symbol, qty_held, OrderSide.SELL)
                print(f"[{name}] SELL {qty_held} shares of {symbol} — reason: {reason}")
                results[acc_key] = f"SELL {qty_held} shares of {symbol}"
            except Exception as e:
                print(f"[{name}] No position or error for {symbol}: {e}")
                results[acc_key] = f"No open position for {symbol}"

        return jsonify({"message": "Exit processed", "results": results}), 200

    else:
        return jsonify({"error": f"Unknown action: {action}"}), 400

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.route('/', methods=['GET'])
def health():
    return jsonify({"status": "Dual account webhook server running!"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
