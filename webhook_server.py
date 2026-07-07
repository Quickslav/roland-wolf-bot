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
from prevday_api import size_for

app = Flask(__name__)
from prevday_api import register
register(app)

# ─────────────────────────────────────────
# ALPACA ACCOUNTS — all three run v16 (open entry, hold to 15:55 flat)
# The old per-account exit times (11:30 / 2PM, v9 experiment) are GONE:
# that liquidation thread is what sold PEW at 13:58 ET on 6 Jul.
# KEYS: rotate at Alpaca -> set in Render env -> delete these fallbacks.
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

# Main accounts — v16 via /webhook
ACCOUNTS = {
    "account1": {"client": ACCOUNT_1, "name": "Account 1 (v16 hold-to-15:55)"},
    "account2": {"client": ACCOUNT_2, "name": "Account 2 (v16 hold-to-15:55)"},
}

# Test account — v16 via /webhook-test
ACCOUNT_TEST = {"client": ACCOUNT_3, "name": "Account 3 (v16 test, hold-to-15:55)"}

# ─────────────────────────────────────────
# HELPER — safe int conversion
# ─────────────────────────────────────────
def safe_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None

# ─────────────────────────────────────────
# HELPER — parse an alert body in EITHER format
#   a) JSON         : {"action":"buy","ticker":"QCOM","reason":"stop"}
#   b) TradingView  : ENTRY,QCOM,gap11.15,OPEN-HOLD,...   (v16 alert_message)
#                     EXIT-STOP,QCOM,...  /  EXIT-EOD,QCOM,...
# Only the first two comma fields matter (action word, ticker), so the
# v16 OPEN-HOLD token passes through with no parser change.
# Returns (action, symbol, reason)
# ─────────────────────────────────────────
def parse_alert(raw):
    raw = (raw or "").strip()
    # try JSON first (old hand-written alerts)
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            action = (data.get('action') or '').upper()
            symbol = data.get('ticker') or data.get('symbol')
            reason = data.get('reason', 'UNKNOWN')
            return action, symbol, reason
    except Exception:
        pass
    # fall back to comma-separated alert_message: ACTION,TICKER,...
    parts = [p.strip() for p in raw.split(',')]
    if len(parts) >= 2:
        word   = parts[0].upper()
        symbol = parts[1]
        if word.startswith('ENTRY') or word == 'BUY':
            return 'ENTRY', symbol, 'UNKNOWN'
        if word.startswith('EXIT') or word == 'SELL':
            return 'EXIT', symbol, word          # e.g. EXIT-STOP / EXIT-EOD as the reason
    return '', None, 'UNKNOWN'

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
# NOTE: check_entry_filter() is deliberately REMOVED (was here in the v9-era
# server). Its three rules were never backtested, were designed around 9:45
# signals, and the PM-volume>15,000 skip contradicts the measured winner
# profile (winners' PM volume runs ~3.3x losers). Any entry-filter idea goes
# through backtest_bars_v2.py before it gets to veto live orders.
# ─────────────────────────────────────────

# ─────────────────────────────────────────
# SAFETY NET LIQUIDATION — ALL accounts, 15:57 ET, once per day.
# Fires two minutes AFTER the TradingView EXIT-EOD alert (15:55), so it only
# catches stragglers: expired/unarmed alerts, missed exits, manual positions.
# It is the failsafe, not the exit — the TV alert stays primary so fills keep
# their EXIT-EOD tags.
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

            # weekdays only, window 15:57-16:05 ET
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
                            print(f"[SAFETY NET] {name} — flattening {len(positions)} position(s) + cancelling orders")
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
            print("Keep-alive ping sent")
        except Exception as e:
            print(f"Keep-alive failed: {e}")
        time.sleep(600)

threading.Thread(target=keep_alive, daemon=True).start()

# ─────────────────────────────────────────
# MAIN WEBHOOK — Accounts 1 and 2 (v16)
# ─────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    raw = request.get_data(as_text=True)
    print(f"[IN] raw={raw[:200]}", flush=True)
    action, symbol, reason = parse_alert(raw)

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action", "raw": raw[:200]}), 400

    print(f"[MAIN] action={action}, symbol={symbol}")
    results = {}

    if action in ("ENTRY", "BUY"):
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        amount = size_for(symbol)
        shares = int(amount / price)
        if shares <= 0:
            return jsonify({"error": "Price too high"}), 400
        print(f"[MAIN] {symbol} — grade size ${amount:,} → {shares} shares")

        for acc_key, acc in ACCOUNTS.items():
            try:
                place_order(acc["client"], symbol, shares, OrderSide.BUY)
                print(f"[{acc['name']}] BUY {shares} shares of {symbol} @ ~${price:.4f}")
                results[acc_key] = f"BUY {shares} shares @ ~${price:.4f}"
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
                print(f"[{acc['name']}] No position: {e}")
                results[acc_key] = "No open position"

        return jsonify({"message": "Exit processed", "results": results}), 200

    return jsonify({"error": f"Unknown action: {action}"}), 400

# ─────────────────────────────────────────
# TEST WEBHOOK — Account 3 only (v16)
# ─────────────────────────────────────────
@app.route('/webhook-test', methods=['POST'])
def webhook_test():
    raw = request.get_data(as_text=True)
    print(f"[IN] raw={raw[:200]}", flush=True)
    action, symbol, reason = parse_alert(raw)

    if not symbol or not action:
        return jsonify({"error": "Missing ticker or action", "raw": raw[:200]}), 400

    print(f"[TEST] action={action}, symbol={symbol}")
    client = ACCOUNT_TEST["client"]
    name   = ACCOUNT_TEST["name"]

    if action in ("ENTRY", "BUY"):
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        amount = size_for(symbol)
        shares = int(amount / price)
        if shares <= 0:
            return jsonify({"error": "Price too high"}), 400
        print(f"[TEST] {symbol} — grade size ${amount:,} → {shares} shares")

        try:
            place_order(client, symbol, shares, OrderSide.BUY)
            print(f"[{name}] BUY {shares} shares of {symbol} @ ~${price:.4f}")
            return jsonify({"message": f"TEST BUY {shares} shares of {symbol}"}), 200
        except Exception as e:
            print(f"[{name}] Error: {e}")
            return jsonify({"error": str(e)}), 500

    elif action in ("EXIT", "SELL"):
        try:
            position = client.get_open_position(symbol)
            qty_held = abs(safe_int(position.qty))
            place_order(client, symbol, qty_held, OrderSide.SELL)
            print(f"[{name}] SELL {qty_held} shares of {symbol} — {reason}")
            return jsonify({"message": f"TEST SELL {qty_held} shares of {symbol}", "reason": reason}), 200
        except Exception as e:
            print(f"[{name}] No position: {e}")
            return jsonify({"message": f"No open position for {symbol}"}), 200

    return jsonify({"error": f"Unknown action: {action}"}), 400

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.route('/', methods=['GET'])
def health():
    return jsonify({"status": "Triple account webhook server running — v16 on all endpoints, safety net 15:57 ET"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
