import logging
import asyncio
from telegram.ext import Application, ConversationHandler, MessageHandler, CommandHandler, filters

from config import Config
from database import init_db
from bot_handlers import (
    start, handle_main_menu, handle_search, handle_request,
    cancel, error_handler, MAIN_MENU, SEARCHING, REQUESTING
)

# Configure logging
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
    
    # Set up conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)
            ],
            SEARCHING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search)
            ],
            REQUESTING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_request)
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )
    
    # Add handlers
    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)
    
    # Start the bot
    logger.info("Bot starting...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
