from flask import Flask
import threading
import os

# Flask app for web server
app = Flask(__name__)

@app.route('/')
def home():
    return "Telegram Bot is Running!"

def run_flask():
    app.run(host='0.0.0.0', port=5000)

def start_bot():
    # Your existing bot code here
    from telegram.ext import Application, CommandHandler, MessageHandler, filters
    
    # Initialize bot application
    application = Application.builder().token(os.environ.get('TELEGRAM_BOT_TOKEN')).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Start polling
    application.run_polling()

if __name__ == "__main__":
    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Start Telegram bot
    start_bot()
