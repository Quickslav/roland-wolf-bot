from flask import Flask, request, jsonify
import alpaca_trade_api as tradeapi
import os

app = Flask(__name__)

# ─────────────────────────────────────────
# ALPACA CONFIGURATION
# ─────────────────────────────────────────
API_KEY    = "PKGABMMAXUYFY5NJCZIOT6XGLD"
SECRET_KEY = "9K8RUh1QA5jQ64jCzf6TL1SPFofh5LQMF1TQubWdyBAs"
BASE_URL   = "https://paper-api.alpaca.markets"

api = tradeapi.REST(API_KEY, SECRET_KEY, BASE_URL, api_version='v2')

# ─────────────────────────────────────────
# WEBHOOK ENDPOINT
# TradingView will send alerts here
# ─────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json

    if not data:
        return jsonify({"error": "No data received"}), 400

    symbol = data.get('symbol')
    action = data.get('action')  # "buy" or "sell"
    qty    = data.get('qty', 100)

    if not symbol or not action:
        return jsonify({"error": "Missing symbol or action"}), 400

    try:
        if action == "buy":
            api.submit_order(
                symbol     = symbol,
                qty        = qty,
                side       = 'buy',
                type       = 'market',
                time_in_force = 'day'
            )
            print(f"BUY order placed for {qty} shares of {symbol}")
            return jsonify({"message": f"BUY {qty} shares of {symbol}"}), 200

        elif action == "sell":
            api.submit_order(
                symbol     = symbol,
                qty        = qty,
                side       = 'sell',
                type       = 'market',
                time_in_force = 'day'
            )
            print(f"SELL order placed for {qty} shares of {symbol}")
            return jsonify({"message": f"SELL {qty} shares of {symbol}"}), 200

        else:
            return jsonify({"error": "Invalid action"}), 400

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
