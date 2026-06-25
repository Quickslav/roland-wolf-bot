from flask import Flask, request, jsonify
import os
import json
import threading
import time
import urllib.request
from datetime import datetime, date
import pytz
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from prevday_api import size_for

app = Flask(__name__)
from prevday_api import register
register(app)

# ─────────────────────────────────────────
# ALPACA ACCOUNTS
# Account 1 — 2:00 PM ET exit (strategy v9)
# Account 2 — 11:30 AM ET exit (strategy v9)
# Account 3 — VWAP exit test (strategy v10)
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

# Main accounts — v9 strategy
ACCOUNTS = {
    "account1": {"client": ACCOUNT_1, "name": "Account 1 (2PM Exit)"},
    "account2": {"client": ACCOUNT_2, "name": "Account 2 (11:30AM Exit)"},
}

# Test account — v10 VWAP strategy only
ACCOUNT_TEST = {"client": ACCOUNT_3, "name": "Account 3 (VWAP Test)"}

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
# ENTRY FILTER — checks 3 rules at 9:44 AM
# Returns (should_skip, reason_string)
# ─────────────────────────────────────────
def check_entry_filter(symbol, entry_price):
    """
    Pulls real-time Alpaca data and checks 3 filter rules.
    All data is available at the moment the signal fires (~9:44-9:45 AM ET).

    Rules — SKIP trade if ANY are true:
      1. Pre-market volume > 15,000
      2. Stock fading from open AND entry is >6% below OR high
      3. Entry is >10% below OR high (momentum gone)

    Returns: (skip: bool, reason: str)
    """
    et_tz    = pytz.timezone("America/New_York")
    now_et   = datetime.now(et_tz)
    today    = now_et.date()

    try:
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        from datetime import timedelta

        tomorrow = today + timedelta(days=1)

        # ── Pull pre-market bars (4:00 AM – 9:30 AM ET) ──────────────────────
        pm_request = StockBarsRequest(
            symbol_or_symbols = symbol,
            timeframe         = TimeFrame.Minute,
            start             = datetime(today.year, today.month, today.day, 8, 0,  tzinfo=pytz.utc),
            end               = datetime(today.year, today.month, today.day, 13, 30, tzinfo=pytz.utc),
            feed              = "iex"
        )
        pm_bars_resp = DATA_CLIENT.get_stock_bars(pm_request)
        pm_bars      = pm_bars_resp.get(symbol, [])

        pm_volume = sum(getattr(b, 'volume', 0) or 0 for b in pm_bars)

        # ── Pull opening range bars (9:30 AM – 9:44 AM ET) ───────────────────
        or_request = StockBarsRequest(
            symbol_or_symbols = symbol,
            timeframe         = TimeFrame.Minute,
            start             = datetime(today.year, today.month, today.day, 13, 30, tzinfo=pytz.utc),
            end               = datetime(today.year, today.month, today.day, 13, 45, tzinfo=pytz.utc),
            feed              = "iex"
        )
        or_bars_resp = DATA_CLIENT.get_stock_bars(or_request)
        or_bars      = or_bars_resp.get(symbol, [])

        if not or_bars:
            print(f"[FILTER] {symbol} — no OR bars available, allowing trade")
            return False, "No OR data — filter skipped"

        or_open  = or_bars[0].open
        or_close = or_bars[-1].close
        or_high  = max(b.high for b in or_bars)

        # ── Calculate signals ─────────────────────────────────────────────────
        fading           = or_close < or_open
        entry_vs_or_high = ((entry_price - or_high) / or_high) * 100  # negative = below OR high

        # ── Log what we see ───────────────────────────────────────────────────
        print(f"[FILTER] {symbol} — PM vol: {pm_volume:,} | OR open: ${or_open:.4f} | OR high: ${or_high:.4f} | Price at OR close: ${or_close:.4f}")
        print(f"[FILTER] {symbol} — Fading: {'YES' if fading else 'NO'} | Entry vs OR high: {entry_vs_or_high:+.2f}%")

        # ── Rule 1: High pre-market volume ────────────────────────────────────
        if pm_volume > 15000:
            reason = f"FILTER SKIP — PM volume {pm_volume:,} > 15,000 (smart money already moved)"
            print(f"[FILTER] {symbol} — {reason}")
            return True, reason

        # ── Rule 2: Fading from open AND deep pullback from OR high ───────────
        if fading and entry_vs_or_high < -6:
            reason = f"FILTER SKIP — Fading from open AND entry {entry_vs_or_high:.1f}% below OR high"
            print(f"[FILTER] {symbol} — {reason}")
            return True, reason

        # ── Rule 3: Entry too far below OR high (momentum gone) ───────────────
        if entry_vs_or_high < -10:
            reason = f"FILTER SKIP — Entry {entry_vs_or_high:.1f}% below OR high (momentum gone)"
            print(f"[FILTER] {symbol} — {reason}")
            return True, reason

        print(f"[FILTER] {symbol} — All rules passed ✅ — placing order")
        return False, "All filter rules passed"

    except Exception as e:
        # If filter errors for any reason, allow the trade (fail open)
        print(f"[FILTER] {symbol} — Error running filter: {e} — allowing trade")
        return False, f"Filter error: {e}"


# ─────────────────────────────────────────
# SAFETY NET LIQUIDATION
# Account 1 — 2:00 PM ET
# Account 2 — 11:30 AM ET
# Account 3 — 2:00 PM ET
# ─────────────────────────────────────────
def safety_liquidation():
    et_tz = pytz.timezone("America/New_York")
    liquidated_today = {"account1": False, "account2": False, "account3": False}
    last_date = None

    while True:
        try:
            now_et = datetime.now(et_tz)
            today  = now_et.date()

            if last_date != today:
                liquidated_today = {"account1": False, "account2": False, "account3": False}
                last_date = today

            acc2_window = (now_et.hour == 11 and now_et.minute >= 28 and now_et.minute <= 32)
            acc1_window = (now_et.hour == 13 and now_et.minute >= 58) or (now_et.hour == 14 and now_et.minute <= 2)

            for acc_key, window in [("account2", acc2_window), ("account1", acc1_window), ("account3", acc1_window)]:
                if window and not liquidated_today[acc_key]:

                    # ── FIXED: use correct client references ──────────────────
                    if acc_key == "account2":
                        client = ACCOUNTS["account2"]["client"]
                        name   = ACCOUNTS["account2"]["name"]
                    elif acc_key == "account1":
                        client = ACCOUNTS["account1"]["client"]
                        name   = ACCOUNTS["account1"]["name"]
                    else:
                        client = ACCOUNT_TEST["client"]
                        name   = ACCOUNT_TEST["name"]
                    # ─────────────────────────────────────────────────────────

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
# MAIN WEBHOOK — Accounts 1 and 2 (v9 strategy)
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

    print(f"[MAIN] action={action}, symbol={symbol}")
    results = {}

    if action in ("ENTRY", "BUY"):
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        shares = int(TRADE_AMOUNT / price)
        if shares <= 0:
            return jsonify({"error": "Price too high"}), 400

        # ── Run entry filter ──────────────────────────────────────────────────
        skip, filter_reason = check_entry_filter(symbol, price)
        if skip:
            print(f"[MAIN] {symbol} — Trade SKIPPED: {filter_reason}")
            return jsonify({
                "message": f"Trade skipped by entry filter",
                "symbol":  symbol,
                "reason":  filter_reason
            }), 200

        # ── Place orders on all accounts ──────────────────────────────────────
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
        reason = data.get('reason', 'UNKNOWN')
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
# TEST WEBHOOK — Account 3 only (v10 VWAP strategy)
# ─────────────────────────────────────────
@app.route('/webhook-test', methods=['POST'])
def webhook_test():
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

    print(f"[TEST] action={action}, symbol={symbol}")
    client = ACCOUNT_TEST["client"]
    name   = ACCOUNT_TEST["name"]

    if action in ("ENTRY", "BUY"):
        price = get_live_price(symbol)
        if not price:
            return jsonify({"error": f"Could not fetch price for {symbol}"}), 500

        shares = int(TRADE_AMOUNT / price)
        if shares <= 0:
            return jsonify({"error": "Price too high"}), 400

        # ── Run entry filter ──────────────────────────────────────────────────
        skip, filter_reason = check_entry_filter(symbol, price)
        if skip:
            print(f"[TEST] {symbol} — Trade SKIPPED: {filter_reason}")
            return jsonify({
                "message": f"Trade skipped by entry filter",
                "symbol":  symbol,
                "reason":  filter_reason
            }), 200

        # ── Place order on test account ───────────────────────────────────────
        try:
            place_order(client, symbol, shares, OrderSide.BUY)
            print(f"[{name}] BUY {shares} shares of {symbol} @ ~${price:.4f}")
            return jsonify({"message": f"TEST BUY {shares} shares of {symbol}"}), 200
        except Exception as e:
            print(f"[{name}] Error: {e}")
            return jsonify({"error": str(e)}), 500

    elif action in ("EXIT", "SELL"):
        reason = data.get('reason', 'UNKNOWN')
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
    return jsonify({"status": "Triple account webhook server running! v9=/webhook v10=/webhook-test"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
