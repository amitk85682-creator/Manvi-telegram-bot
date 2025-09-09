import os
import logging
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from config import Config
from database import init_db
from bot_handlers import start, handle_message, handle_callback
from fuzzy_search import search_movies

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def main():
    # Initialize database
    init_db()
    
    # Create application
    application = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_callback))

    # Start the bot
    if os.environ.get('DYNO'):  # Heroku deployment
        port = int(os.environ.get('PORT', 8443))
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=Config.TELEGRAM_BOT_TOKEN,
            webhook_url=f"https://your-app-name.herokuapp.com/{Config.TELEGRAM_BOT_TOKEN}"
        )
    else:  # Local development
        application.run_polling()

if __name__ == "__main__":
    main()
