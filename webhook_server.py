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
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

app = Flask(__name__)

# ─────────────────────────────────────────
# ALPACA ACCOUNTS — all three run v17 simple (9:40 entry, 3% stop, 15:55 flat)
# ─────────────────────────────────────────
API_KEY_1    = os.environ.get("API_KEY_1", "PKGABMMAXUYFY5NJCZIOT6XGLD")
SECRET_KEY_1 = os.environ.get("SECRET_KEY_1", "9K8RUh1QA5jQ64jCzf6TL1SPFofh5LQMF1TQubWdyBAs")

API_KEY_2    = os.environ.get("API_KEY_2", "PK6Q5L6JMLIJYYQGPUBUNQLNU5")
SECRET_KEY_2 = os.environ.get("SECRET_KEY_2", "HdrTT2wNELMFKm6xZHymKCLbiaLCgC5dspUv6HDuGEWx")

API_KEY_3    = os.environ.get("API_KEY_3", "PKIYGXQT3DGX7B6BDIZFZ6VQWU")
SECRET_KEY_3 = os.environ.get("SECRET_KEY_3", "Ekaz5bQHUbbFUQvmidbBSU89wHMtgigik3TsyFD15NA3")

ACCOUNT_1 = TradingClient(API_KEY_1, SECRET_KEY_1, paper=True)
ACCOUNT_2 = TradingClient(API_KEY_2, SECRET_KEY_2, paper=True)
ACCOUNT_3 = TradingClient(API_KEY_3, SECRET_KEY_3, paper=True)

DATA_CLIENT = StockHistoricalDataClient(API_KEY_1, SECRET_KEY_1)

# Main accounts — v17 simple via /webhook
ACCOUNTS = {
    "account1": {"client": ACCOUNT_1, "name": "Account 1 (v17 simple)"},
    "account2": {"client": ACCOUNT_2, "name": "Account 2 (v17 simple)"},
}

# Test account — v17 simple via /webhook-test
ACCOUNT_TEST = {"client": ACCOUNT_3, "name": "Account 3 (v17 simple test)"}

# ─────────────────────────────────────────
# HELPER — safe int conversion
# ─────────────────────────────────────────
def safe_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────
# HELPER — parse alert and extract shares if present
# Format: ENTRY,TICKER,SIMPLE-940,shares=123,fill=1.50,t=...
# Returns (action, symbol, reason, shares_from_alert)
# ─────────────────────────────────────────
def parse_alert(raw):
    raw = (raw or "").strip()
    parts = [p.strip() for p in raw.split(',')]
    if len(parts) >= 2:
        word   = parts[0].upper()
        symbol = parts[1]
        
        # Extract shares if present
        shares_from_alert = None
        for part in parts:
            if part.startswith('shares='):
                try:
                    shares_from_alert = int(part.replace('shares=', '').strip())
                except:
                    pass
        
        if word.startswith('ENTRY') or word == 'BUY':
            return 'ENTRY', symbol, 'UNKNOWN', shares_from_alert
        if word.startswith('EXIT') or word == 'SELL':
            return 'EXIT', symbol, word, None
    return '', None, 'UNKNOWN', None

# ─────────────────────────────────────────
# HELPER — get live price
# ─────────────────────────────────────────
def get_live_price(symbol):
    try:
        request_params = StockLatestTradeRequest(symbol_or_symbols=symbol)
        response       = DATA_CLIENT.get_stock_latest_trade(request_params)
        price          = float(response[symbol].price)
        return price
    except Exception as e:
        print(f"[PRICE] Error fetching {symbol}: {e}")
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
# SAFETY NET LIQUIDATION — 15:57 ET, once per day
# ─────────────────────────────────────────
def safety_liquidation():
    et_tz = pytz.timezone("America/New_York")
    liquidated_today = {"account1": False, "account2": False, "account3": False}
    last_date = None

    ALL_ACCOUNTS = [
        ("account1", ACCOUNTS["account1"]["client"], ACCOUNTS["account1"]["name"]),
        ("account2", ACCOUNTS["account2"]["client"], ACCOUNTS["account2"]["name"]),
        ("account3", ACCOUNT_TEST["client"],          ACCOUNT_TEST["name"]),
    ]

    while True:
        try:
            now_et = datetime.now(et_tz)
            today  = now_et.date()

            if last_date != today:
                liquidated_today = {"account1": False, "account2": False, "account3": False}
                last_date = today

            # weekdays only, 15:57-16:05 ET
            in_window = (now_et.weekday() < 5 and
                         ((now_et.hour == 15 and now_et.minute >= 57) or
                          (now_et.hour == 16 and now_et.minute <= 5)))

            if in_window:
                for acc_key, client, name in ALL_ACCOUNTS:
                    if liquidated_today[acc_key]:
                        continue
                    try:
                        positions = client.get_all_positions()
                        if positions:
                            print(f"[SAFETY NET] {name} — flattening {len(positions)} position(s)")
                            client.close_all_positions(cancel_orders=True)
                        else:
                            print(f"[SAFETY NET] {name} — already flat")
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
            print("[KEEP-ALIVE] ping sent")
        except Exception as e:
            print(f"[KEEP-ALIVE] failed: {e}")
        time.sleep(600)

threading.Thread(target=keep_alive, daemon=True).start()

# ─────────────────────────────────────────
# MAIN WEBHOOK — Accounts 1 and 2 (v17 simple)
# ─────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    raw = request.get_data(as_text=True)
    print(f"[IN] raw={raw[:200]}", flush=True)
    action, symbol, reason, shares_from_alert = parse_alert(raw)

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action", "raw": raw[:200]}), 400

    print(f"[MAIN] action={action}, symbol={symbol}")
    results = {}

    if action in ("ENTRY", "BUY"):
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        # Use shares from alert if present, else calculate
        if shares_from_alert is not None:
            shares = shares_from_alert
            print(f"[MAIN] {symbol} — using Pine-calculated {shares} shares @ ${price:.4f}")
        else:
            shares = int(5000 / price)
            print(f"[MAIN] {symbol} — calculated {shares} shares @ ${price:.4f}")
        
        if shares <= 0:
            return jsonify({"error": "Price too high for $5k position"}), 400

        for acc_key, acc in ACCOUNTS.items():
            try:
                place_order(acc["client"], symbol, shares, OrderSide.BUY)
                print(f"[{acc['name']}] BUY {shares} shares of {symbol} @ ${price:.4f}")
                results[acc_key] = f"BUY {shares} shares @ ${price:.4f}"
            except Exception as e:
                print(f"[{acc['name']}] Error: {e}")
                results[acc_key] = f"Error: {str(e)}"

        return jsonify({"message": "Entry processed", "results": results}), 200

    elif action in ("EXIT", "SELL"):
        for acc_key, acc in ACCOUNTS.items():
            try:
                position = acc["client"].get_open_position(symbol)
                qty_held = abs(safe_int(position.qty))
                place_order(acc["client"], symbol, qty_held, OrderSide.SELL)
                print(f"[{acc['name']}] SELL {qty_held} shares of {symbol} — {reason}")
                results[acc_key] = f"SELL {qty_held} shares"
            except Exception as e:
                print(f"[{acc['name']}] No position or error: {e}")
                results[acc_key] = "No open position"

        return jsonify({"message": "Exit processed", "results": results}), 200

    return jsonify({"error": f"Unknown action: {action}"}), 400

# ─────────────────────────────────────────
# TEST WEBHOOK — Account 3 only (v17 simple)
# ─────────────────────────────────────────
@app.route('/webhook-test', methods=['POST'])
def webhook_test():
    raw = request.get_data(as_text=True)
    print(f"[IN] raw={raw[:200]}", flush=True)
    action, symbol, reason, shares_from_alert = parse_alert(raw)

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action", "raw": raw[:200]}), 400

    print(f"[TEST] action={action}, symbol={symbol}")
    client = ACCOUNT_TEST["client"]
    name   = ACCOUNT_TEST["name"]

    if action in ("ENTRY", "BUY"):
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        # Use shares from alert if present, else calculate
        if shares_from_alert is not None:
            shares = shares_from_alert
            print(f"[TEST] {symbol} — using Pine-calculated {shares} shares @ ${price:.4f}")
        else:
            shares = int(5000 / price)
            print(f"[TEST] {symbol} — calculated {shares} shares @ ${price:.4f}")
        
        if shares <= 0:
            return jsonify({"error": "Price too high for $5k position"}), 400

        try:
            place_order(client, symbol, shares, OrderSide.BUY)
            print(f"[{name}] BUY {shares} shares of {symbol} @ ${price:.4f}")
            return jsonify({"message": f"BUY {shares} shares of {symbol}", "price": price, "shares": shares}), 200
        except Exception as e:
            print(f"[{name}] Error: {e}")
            return jsonify({"error": str(e)}), 500

    elif action in ("EXIT", "SELL"):
        try:
            position = client.get_open_position(symbol)
            qty_held = abs(safe_int(position.qty))
            place_order(client, symbol, qty_held, OrderSide.SELL)
            print(f"[{name}] SELL {qty_held} shares of {symbol} — {reason}")
            return jsonify({"message": f"SELL {qty_held} shares of {symbol}", "reason": reason}), 200
        except Exception as e:
            print(f"[{name}] No position or error: {e}")
            return jsonify({"message": f"No open position for {symbol}"}), 200

    return jsonify({"error": f"Unknown action: {action}"}), 400

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.route('/', methods=['GET'])
def health():
    return jsonify({"status": "v18 — no gate, no grades, $5k fixed sizing, safety net 15:57 ET"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
