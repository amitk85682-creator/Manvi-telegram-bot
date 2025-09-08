import os
import logging
import psycopg2
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
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

# Check if message is a movie request
def is_movie_request(message):
    message_lower = message.lower().strip()
    
    # Ignore short messages and greetings
    non_movie_words = ['hi', 'hello', 'hey', 'thanks', 'thank you', 'ok', 'please', 'help', '?', '!']
    
    if len(message_lower) < 3:
        return False
        
    if message_lower in non_movie_words:
        return False
        
    if any(word in message_lower for word in ['how are you', 'whats up', 'good morning', 'good night']):
        return False
        
    return True

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to FilmFyBot!\n\n"
        "Send me any movie name and I'll find it for you!\n\n"
        "Examples: 'KGF 2', 'Vikram', 'Pushpa', 'RRR'"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_message = update.message.text.strip()
        
        # Check if it's a movie request
        if not is_movie_request(user_message):
            return  # Ignore non-movie messages
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Try exact match first
        cur.execute("SELECT title, url FROM movies WHERE LOWER(title) = LOWER(%s)", (user_message,))
        exact_match = cur.fetchone()
        
        if exact_match:
            title, url = exact_match
            keyboard = [[InlineKeyboardButton("📥 Download", url=url)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(f"🎉 Found '{title}'!", reply_markup=reply_markup)
            cur.close()
            conn.close()
            return
        
        # Try partial match
        cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 5", (f'%{user_message}%',))
        partial_matches = cur.fetchall()
        
        if partial_matches:
            if len(partial_matches) == 1:
                # Single match found
                title, url = partial_matches[0]
                keyboard = [[InlineKeyboardButton("📥 Download", url=url)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(f"🎉 Found '{title}'!", reply_markup=reply_markup)
            else:
                # Multiple matches found
                keyboard = []
                for title, url in partial_matches[:3]:  # Show max 3 options
                    keyboard.append([InlineKeyboardButton(title, url=url)])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text("🎬 Movies found:", reply_markup=reply_markup)
            
            cur.close()
            conn.close()
            return
        
        # If no matches found
        await update.message.reply_text("😔 This movie is not available yet. We'll add it soon!")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await update.message.reply_text("❌ Sorry, something went wrong. Please try again later.")

def main():
    try:
        # Create application
        application = Application.builder().token(os.environ.get('TELEGRAM_BOT_TOKEN')).build()
        
        # Add handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        # Start the bot
        logger.info("Bot starting...")
        application.run_polling()
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == "__main__":
    main()
