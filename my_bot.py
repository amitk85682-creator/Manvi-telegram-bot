import os
import logging
import psycopg2
import asyncio
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

# Admin user ID - अपना Telegram User ID यहाँ डालें
ADMIN_USER_ID = 6946322342  # यहाँ अपना User ID replace करें

# Add movie command
async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Check if user is admin
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Only admin can add movies!")
        return
    
    # Check if correct format is used
    if len(context.args) < 2:
        await update.message.reply_text("❌ Format: /addmovie Movie_Name URL")
        return
    
    # Extract movie name and URL
    movie_name = ' '.join(context.args[:-1])
    movie_url = context.args[-1]
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Insert into database
        cur.execute(
            "INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
            (movie_name, movie_url)
        )
        conn.commit()
        
        await update.message.reply_text(f"✅ Added '{movie_name}' successfully!")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
    finally:
        cur.close()
        conn.close()

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
        application.add_handler(CommandHandler("addmovie", add_movie))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        # Start the bot with drop_pending_updates
        logger.info("Bot starting...")
        application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == "__main__":
    main()
