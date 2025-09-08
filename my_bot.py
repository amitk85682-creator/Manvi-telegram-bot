import os
import logging
import psycopg2
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from thefuzz import process

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    return psycopg2.connect(os.environ.get('DATABASE_URL'))

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to FilmFyBot!\n\n"
        "Send me any movie name and I'll find it for you!"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_message = update.message.text.strip()
        
        # Simple search without AI
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Try exact match
        cur.execute("SELECT title, url FROM movies WHERE LOWER(title) = LOWER(%s)", (user_message,))
        result = cur.fetchone()
        
        if result:
            title, url = result
            keyboard = [[InlineKeyboardButton("📥 Download", url=url)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(f"🎉 Found '{title}'!", reply_markup=reply_markup)
        else:
            # Try partial match
            cur.execute("SELECT title, url FROM movies")
            all_movies = cur.fetchall()
            movie_titles = [movie[0] for movie in all_movies]
            
            # Fuzzy matching
            matches = process.extract(user_message, movie_titles, limit=5)
            filtered_matches = [match for match, score in matches if score >= 60]
            
            if filtered_matches:
                keyboard = []
                for match in filtered_matches[:3]:
                    cur.execute("SELECT url FROM movies WHERE title = %s", (match,))
                    url = cur.fetchone()[0]
                    keyboard.append([InlineKeyboardButton(match, url=url)])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text("🔍 Similar movies found:", reply_markup=reply_markup)
            else:
                await update.message.reply_text("😔 Movie not found. We'll add it soon!")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await update.message.reply_text("❌ Sorry, something went wrong. Please try again.")

def main():
    # Create application
    application = Application.builder().token(os.environ.get('TELEGRAM_BOT_TOKEN')).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Start the bot
    logger.info("Bot starting...")
    application.run_polling()

if __name__ == "__main__":
    main()
