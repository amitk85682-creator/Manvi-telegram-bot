from flask import Flask, jsonify
import threading

# Flask app for health checks
app = Flask(__name__)

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy", "service": "telegram-bot"}), 200

@app.route('/')
def home():
    return "Telegram Bot is Running!", 200

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# Main function में add करें
def main():
    # Start Flask server in separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Start Telegram bot
    application.run_polling(drop_pending_updates=True)
