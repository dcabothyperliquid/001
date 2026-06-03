import os
import json
import time
import threading
import logging
from flask import Flask, jsonify, request, render_template
from hyperliquid_client import HyperliquidClient
from bot_engine import BotEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global state
client = HyperliquidClient()
bot_engine = BotEngine(client)

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/coins', methods=['GET'])
def get_coins():
    return jsonify(bot_engine.get_coins())

@app.route('/api/coins', methods=['POST'])
def add_coin():
    data = request.json
    symbol = data.get('symbol', '').upper().strip()
    capital = float(data.get('capital', 10))
    timeframe = data.get('timeframe', '1h')
    stop_loss = float(data.get('stop_loss', 1.5))
    trailing_stop = float(data.get('trailing_stop', 1.0))
    result = bot_engine.add_coin(symbol, capital, timeframe, stop_loss, trailing_stop)
    return jsonify(result)

@app.route('/api/coins/<symbol>', methods=['DELETE'])
def remove_coin(symbol):
    result = bot_engine.remove_coin(symbol.upper())
    return jsonify(result)

@app.route('/api/coins/<symbol>', methods=['PUT'])
def update_coin(symbol):
    data = request.json
    result = bot_engine.update_coin(symbol.upper(), data)
    return jsonify(result)

@app.route('/api/market/<symbol>', methods=['GET'])
def get_market_data(symbol):
    data = bot_engine.get_market_data(symbol.upper())
    return jsonify(data)

@app.route('/api/balance', methods=['GET'])
def get_balance():
    wallet = request.args.get('wallet', '')
    if not wallet:
        return jsonify({'error': 'Wallet address required'})
    balance = client.get_spot_balance(wallet)
    return jsonify(balance)

@app.route('/api/holdings', methods=['GET'])
def get_holdings():
    return jsonify(bot_engine.get_holdings())

@app.route('/api/trades', methods=['GET'])
def get_trades():
    return jsonify(bot_engine.get_trade_history())

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify(bot_engine.get_stats())

@app.route('/api/bot/start', methods=['POST'])
def start_bot():
    data = request.json or {}
    wallet = data.get('wallet', '')
    private_key = data.get('private_key', '')
    result = bot_engine.start(wallet, private_key)
    return jsonify(result)

@app.route('/api/bot/stop', methods=['POST'])
def stop_bot():
    result = bot_engine.stop()
    return jsonify(result)

@app.route('/api/bot/status', methods=['GET'])
def bot_status():
    return jsonify(bot_engine.get_status())

@app.route('/api/sell/<symbol>', methods=['POST'])
def manual_sell(symbol):
    data = request.json or {}
    amount = data.get('amount', None)
    result = bot_engine.manual_sell(symbol.upper(), amount)
    return jsonify(result)

@app.route('/api/spot/pairs', methods=['GET'])
def get_spot_pairs():
    pairs = client.get_spot_pairs()
    return jsonify(pairs)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
