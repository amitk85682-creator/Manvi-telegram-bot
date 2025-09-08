# main.py

import os
import logging
import asyncio
import re
import json
from contextlib import contextmanager
from typing import List, Optional, Tuple
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ==============================================================================
# 1. CONFIGURATION (कॉन्फ़िगरेशन)
# ==============================================================================

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class Config:
    """Loads all configuration from environment variables."""
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    DATABASE_URL = os.environ.get('DATABASE_URL')
    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
    BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
    BLOG_ID = os.environ.get('BLOG_ID')
    ADMIN_ID = int(os.environ.get('ADMIN_USER_ID', 0))
    FUZZY_MATCH_THRESHOLD = 80  # Similarity percentage for search
    MAX_SEARCH_RESULTS = 5      # Max buttons to show for a search

# ==============================================================================
# 2. GLOBALS & INITIALIZATION (ग्लोबल वेरिएबल्स और शुरुआत)
# ==============================================================================

db_pool = None
gemini_model = None

def init_db():
    """Initializes the database connection pool and creates tables if they don't exist."""
    global db_pool
    if not Config.DATABASE_URL:
        logger.error("DATABASE_URL is not set. Database features will be disabled.")
        return
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, dsn=Config.DATABASE_URL)
        logger.info("Database connection pool initialized.")
        
        with db_pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS movies (
                        id SERIAL PRIMARY KEY, title TEXT NOT NULL UNIQUE, url TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_requests (
                        id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, movie_title TEXT NOT NULL,
                        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, notified BOOLEAN DEFAULT FALSE,
                        UNIQUE(user_id, movie_title)
                    );
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS sync_history (
                        id SERIAL PRIMARY KEY, last_sync_time TIMESTAMP NOT NULL,
                        items_processed INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
                conn.commit()
        logger.info("Database tables verified.")
    except psycopg2.Error as e:
        logger.error(f"Database initialization failed: {e}")
        db_pool = None

def init_gemini():
    """Initializes the Gemini AI model."""
    global gemini_model
    if Config.GEMINI_API_KEY:
        try:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            gemini_model = genai.GenerativeModel('gemini-1.5-flash')
            logger.info("Gemini AI model initialized successfully.")
        except Exception as e:
            logger.error(f"Gemini initialization failed: {e}")
    else:
        logger.warning("GEMINI_API_KEY not set. AI features will be disabled.")

@contextmanager
def get_db_cursor():
    """Provides a database cursor from the pool."""
    if not db_pool:
        raise ConnectionError("Database pool is not initialized.")
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        db_pool.putconn(conn)

# ==============================================================================
# 3. BLOCKING HELPER FUNCTIONS (धीमे काम करने वाले हेल्पर फंक्शन्स)
# These functions perform slow, blocking I/O (like database or API calls).
# We will run them in a separate thread using asyncio.to_thread to keep the bot responsive.
# ==============================================================================

def _search_movies_blocking(query: str) -> List[Tuple[str, str]]:
    """Performs the actual database search."""
    with get_db_cursor() as cur:
        try:
            # First, try similarity search if the extension is enabled
            cur.execute("""
                SELECT title, url FROM movies 
                WHERE SIMILARITY(title, %s) > %s
                ORDER BY SIMILARITY(title, %s) DESC LIMIT %s
            """, (query, Config.FUZZY_MATCH_THRESHOLD / 100.0, query, Config.MAX_SEARCH_RESULTS))
            results = cur.fetchall()
            if results:
                return results
        except psycopg2.Error:
            # Fallback if pg_trgm is not available or fails
            logger.warning("pg_trgm similarity search failed. Falling back to ILIKE.")
        
        # Fallback to simple ILIKE search
        cur.execute(
            "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
            (f'%{query}%', Config.MAX_SEARCH_RESULTS)
        )
        return cur.fetchall()

def _store_user_request_blocking(user_id: int, movie_title: str):
    """Stores a user's movie request."""
    with get_db_cursor() as cur:
        cur.execute(
            "INSERT INTO user_requests (user_id, movie_title) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (user_id, movie_title)
        )

def _incremental_sync_blocking() -> str:
    """Performs the actual sync with the Blogger API."""
    if not Config.BLOGGER_API_KEY or not Config.BLOG_ID:
        return "Blogger API Key or Blog ID not configured."

    with get_db_cursor() as cur:
        cur.execute("SELECT last_sync_time FROM sync_history ORDER BY id DESC LIMIT 1")
        last_sync = cur.fetchone()
        last_sync_time = last_sync[0].isoformat() + "Z" if last_sync else "1970-01-01T00:00:00Z"

        service = build('blogger', 'v3', developerKey=Config.BLOGGER_API_KEY)
        posts_request = service.posts().list(
            blogId=Config.BLOG_ID, maxResults=500, status='live',
            updatedMin=last_sync_time, fetchBodies=False
        )
        new_items = []
        while posts_request:
            posts_response = posts_request.execute()
            for item in posts_response.get('items', []):
                new_items.append((item['title'].strip(), item['url']))
            posts_request = service.posts().list_next(posts_request, posts_response)

        if new_items:
            execute_values(
                cur,
                "INSERT INTO movies (title, url) VALUES %s ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
                new_items
            )
        
        cur.execute(
            "INSERT INTO sync_history (last_sync_time, items_processed) VALUES (%s, %s)",
            (datetime.now(), len(new_items))
        )
        return f"Sync complete. Added/updated {len(new_items)} items."

# ==============================================================================
# 4. ASYNC WRAPPER FUNCTIONS (असिंक्रोनस रैपर फंक्शन्स)
# These are the async functions our bot will call. They use asyncio.to_thread
# to run the slow, blocking functions without freezing the bot.
# ==============================================================================

async def search_movies(query: str) -> List[Tuple[str, str]]:
    """Asynchronously searches for movies."""
    return await asyncio.to_thread(_search_movies_blocking, query)

async def store_user_request(user_id: int, movie_title: str):
    """Asynchronously stores a user request."""
    await asyncio.to_thread(_store_user_request_blocking, user_id, movie_title)

async def incremental_sync() -> str:
    """Asynchronously performs an incremental sync."""
    return await asyncio.to_thread(_incremental_sync_blocking)

async def analyze_message_intent(message_text: str) -> Tuple[bool, Optional[str]]:
    """Uses Gemini to analyze the user's intent."""
    if not gemini_model:
        return False, None  # AI is disabled
    try:
        prompt = f"""
        Analyze the user's message to determine if they are requesting a movie or web series. 
        - If it is a request, extract the most likely title.
        - If it is a greeting, question, or general chat, it is not a request.
        Respond ONLY with a valid JSON object in the format: 
        {{"is_request": boolean, "content_title": "The Extracted Title or null"}}

        User message: "{message_text}"
        """
        response = await asyncio.to_thread(gemini_model.generate_content, prompt)
        
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return result.get('is_request', False), result.get('content_title')
        
        logger.warning(f"AI response was not valid JSON: {response.text}")
        return False, None
    except Exception as e:
        logger.error(f"AI analysis error: {e}")
        return False, None

# ==============================================================================
# 5. TELEGRAM HANDLERS (टेलीग्राम हैंडलर्स)
# ==============================================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    await update.message.reply_text("👋 Hi! I'm Manvi. Just send me a movie or series name!")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The main message handler."""
    user_message = update.message.text.strip()
    if len(user_message) < 2:
        return

    is_request, content_title = await analyze_message_intent(user_message)

    if not is_request or not content_title:
        logger.info(f"Ignoring non-request message: {user_message}")
        return

    movies = await search_movies(content_title)

    if not movies:
        await store_user_request(update.effective_user.id, content_title)
        await update.message.reply_text(f"😔 Sorry, I couldn't find '{content_title}'. I'll notify you if it's added!")
    elif len(movies) == 1:
        title, url = movies[0]
        await send_movie_content(update.message, context, title, url)
    else:
        keyboard = [[InlineKeyboardButton(title, callback_data=f"movie_{url}")] for title, url in movies]
        await update.message.reply_text("I found a few matches, which one did you mean?", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_movie_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles when a user clicks an inline button for a movie."""
    query = update.callback_query
    await query.answer()
    url = query.data.split('movie_', 1)[1]
    
    # We need to find the title associated with this URL to display it.
    # This is a small query, so direct blocking is acceptable for simplicity.
    title = "your selected movie"
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT title FROM movies WHERE url = %s LIMIT 1", (url,))
            result = cur.fetchone()
            if result:
                title = result[0]
    except Exception as e:
        logger.error(f"Could not fetch title for URL callback: {e}")

    await send_movie_content(query.message, context, title, url)

async def send_movie_content(message: telegram.Message, context: ContextTypes.DEFAULT_TYPE, title: str, url: str):
    """Sends the movie content to the user."""
    loading_msg = await message.reply_text(f"🎬 Preparing '{title}'...")
    try:
        if url.startswith("https://t.me/"):
            parts = url.split('/')
            chat_id = parts[-2]
            if not chat_id.startswith('-100'):
                chat_id = '-100' + chat_id
            await context.bot.copy_message(chat_id=message.chat_id, from_chat_id=chat_id, message_id=int(parts[-1]))
        else:
            await message.reply_text(f"Here is the link for **{title}**:\n\n{url}", parse_mode='Markdown')
        await loading_msg.delete()
    except TelegramError as e:
        logger.error(f"Failed to send content for '{title}': {e}")
        await loading_msg.edit_text(f"❌ Sorry, there was an error sending the file for '{title}'.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Logs errors and sends a generic message."""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

# ==============================================================================
# 6. ADMIN & AUTOMATION (एडमिन और ऑटोमेशन)
# ==============================================================================

async def sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual sync command for the admin."""
    if update.effective_user.id != Config.ADMIN_ID:
        return
    message = await update.message.reply_text("🔄 Starting sync...")
    result = await incremental_sync()
    await message.edit_text(f"✅ Sync Result: {result}")

async def periodic_sync(context: ContextTypes.DEFAULT_TYPE):
    """The function that runs automatically every few hours."""
    logger.info("Running periodic background sync...")
    result = await incremental_sync()
    logger.info(f"Periodic sync finished. Result: {result}")

async def setup_database_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """A temporary, one-time command for the admin to enable the pg_trgm extension."""
    if update.effective_user.id != Config.ADMIN_ID:
        return
    message = await update.message.reply_text("⚙️ Attempting to enable 'pg_trgm' extension...")
    try:
        with get_db_cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        await message.edit_text("✅ Success! 'pg_trgm' is enabled. You can now remove this command from the code.")
    except Exception as e:
        await message.edit_text(f"❌ Error: {e}")

# ==============================================================================
# 7. MAIN (मुख्य फंक्शन)
# ==============================================================================

def main():
    """Starts the bot."""
    if not all([Config.TELEGRAM_TOKEN, Config.DATABASE_URL, Config.ADMIN_ID]):
        logger.critical("FATAL: Missing TELEGRAM_TOKEN, DATABASE_URL, or ADMIN_USER_ID. Exiting.")
        return

    init_db()
    init_gemini()

    application = Application.builder().token(Config.TELEGRAM_TOKEN).build()
    
    # Add all handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_movie_selection, pattern="^movie_"))
    
    # Admin commands
    application.add_handler(CommandHandler("sync", sync_command))
    application.add_handler(CommandHandler("setupdb", setup_database_command)) # Remember to remove this after first run

    application.add_error_handler(error_handler)

    # Schedule the automatic sync job
    if application.job_queue:
        application.job_queue.run_repeating(periodic_sync, interval=timedelta(hours=4), first=10)

    logger.info("Bot is starting...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
