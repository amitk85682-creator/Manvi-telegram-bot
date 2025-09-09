# File: main.py
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from config import Config
import database
import ai_utils
import bot_handlers

def main():
    # जाँच करें कि ज़रूरी वेरिएबल्स मौजूद हैं
    if not Config.TELEGRAM_BOT_TOKEN or not Config.DATABASE_URL or not Config.ADMIN_ID:
        print("FATAL: Missing essential environment variables.")
        return

    # कंपोनेंट्स को शुरू करें
    database.init_db()
    ai_utils.init_gemini()

    # एप्लीकेशन बनाएँ
    application = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()

    # हैंडलर्स जोड़ें
    application.add_handler(CommandHandler("start", bot_handlers.start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.handle_message))
    
    # एरर हैंडलर जोड़ें
    application.add_error_handler(bot_handlers.error_handler)

    # बॉट शुरू करें
    print("Bot is starting...")
    application.run_polling()

if __name__ == "__main__":
    main()
