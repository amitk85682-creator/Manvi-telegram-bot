import os
import logging
import asyncio
import re
import json
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError, NetworkError
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from thefuzz import fuzz, process

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
class Config:
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    DATABASE_URL = os.environ.get('DATABASE_URL')
    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
    BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
    BLOG_ID = os.environ.get('BLOG_ID')
    ADMIN_ID = int(os.environ.get('ADMIN_USER_ID', 0))
    GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
    UPDATE_SECRET = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
    REQUEST_TIMEOUT = 30
    FUZZY_MATCH_THRESHOLD = 80  # Fuzzy matching threshold percentage
    MAX_MOVIE_RESULTS = 5

# Database connection pool
db_pool = None

# Initialize Gemini AI
gemini_chat = None

def init_db():
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, Config.DATABASE_URL)
        logger.info("Database connection pool initialized")
        
        # Create necessary tables and indexes
        with db_pool.getconn() as conn:
            with conn.cursor() as cur:
                # Movies table
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS movies (
                        id SERIAL PRIMARY KEY,
                        title TEXT NOT NULL UNIQUE,
                        url TEXT NOT NULL,
                        quality TEXT,
                        size TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # User requests table
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_requests (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        username TEXT,
                        first_name TEXT,
                        movie_title TEXT NOT NULL,
                        requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notified BOOLEAN DEFAULT FALSE,
                        group_id BIGINT,
                        message_id BIGINT,
                        UNIQUE(user_id, movie_title)
                    )
                ''')
                
                # Sync history table for incremental updates
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS sync_history (
                        id SERIAL PRIMARY KEY,
                        last_sync_time TIMESTAMP NOT NULL,
                        items_processed INTEGER DEFAULT 0,
                        sync_type TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes for better performance
                cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies USING gin(title gin_trgm_ops)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_user_movie ON user_requests(user_id, movie_title)')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_notified ON user_requests(notified)')
                
                conn.commit()
                
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

def init_gemini():
    global gemini_chat
    if Config.GEMINI_API_KEY:
        try:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            model = genai.GenerativeModel('gemini-pro')
            
            # System prompt for intent analysis
            system_prompt = """
            You are a 'Request Analyzer' for a Telegram bot. Your only purpose is to analyze user messages 
            and determine if they are requesting a movie or web series.
            
            Respond ONLY with a JSON object in this format:
            {
                "is_request": boolean,
                "content_title": string or null
            }
            
            Rules:
            1. If the user is asking for a movie/series, set "is_request": true and extract the title in "content_title"
            2. If it's general conversation, greetings, or other topics, set "is_request": false and "content_title": null
            3. Never explain your reasoning. Only return the JSON.
            """
            
            gemini_chat = model.start_chat(history=[{'role': 'user', 'parts': [system_prompt]}])
            logger.info("Gemini AI initialized successfully")
        except Exception as e:
            logger.error(f"Gemini initialization failed: {e}")
    else:
        logger.warning("GEMINI_API_KEY not set, AI features disabled")

@asynccontextmanager
async def db_cursor():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        db_pool.putconn(conn)

async def analyze_message_intent(message_text: str) -> Tuple[bool, Optional[str]]:
    """
    Use AI to analyze if the message is a movie request and extract the title
    """
    if not gemini_chat:
        # Fallback to simple keyword matching if AI is not available
        movie_keywords = ['movie', 'film', 'series', 'download', 'watch', 'see', 'चाहिए', 'दो', 'भेजो']
        if any(keyword in message_text.lower() for keyword in movie_keywords):
            # Extract potential title by removing common words
            common_words = ['movie', 'film', 'send', 'me', 'please', 'want', 'need', 'download', 'watch', 'see', 'hi', 'hello']
            words = message_text.lower().split()
            title_words = [word for word in words if word not in common_words]
            return True, ' '.join(title_words) if title_words else None
        return False, None
    
    try:
        prompt = f"Analyze this message: '{message_text}'"
        response = gemini_chat.send_message(prompt)
        
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return result.get('is_request', False), result.get('content_title')
        
        return False, None
    except Exception as e:
        logger.error(f"AI analysis error: {e}")
        return False, None

async def search_movies(query: str, limit: int = Config.MAX_MOVIE_RESULTS) -> List[Tuple[str, str]]:
    """
    Search movies using fuzzy matching and full-text search
    """
    try:
        async with db_cursor() as cur:
            # First try exact match
            cur.execute(
                "SELECT title, url FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1",
                (query,)
            )
            exact_match = cur.fetchone()
            if exact_match:
                return [exact_match]
            
            # Then try fuzzy matching with pg_trgm
            cur.execute("""
                SELECT title, url, SIMILARITY(title, %s) as similarity
                FROM movies 
                WHERE title % %s
                ORDER BY similarity DESC
                LIMIT %s
            """, (query, query, limit))
            
            results = cur.fetchall()
            if results:
                return [(title, url) for title, url, similarity in results 
                       if similarity * 100 >= Config.FUZZY_MATCH_THRESHOLD]
            
            # Fallback to ILIKE if no good fuzzy matches
            cur.execute(
                "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
                (f'%{query}%', limit)
            )
            return cur.fetchall()
            
    except Exception as e:
        logger.error(f"Search error: {e}")
        return []

async def send_movie_options(update: Update, movies: List[Tuple[str, str]]):
    """
    Send interactive buttons for multiple movie results
    """
    keyboard = []
    for title, url in movies:
        keyboard.append([InlineKeyboardButton(title, callback_data=f"movie_{url}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Multiple matches found. Select one:",
        reply_markup=reply_markup
    )

async def handle_movie_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle movie selection from inline keyboard
    """
    query = update.callback_query
    await query.answer()
    
    url = query.data.replace('movie_', '')
    await send_movie_content(update, context, "Selected Movie", url)

async def send_movie_content(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                           title: str, url: str):
    """
    Send movie content with proper formatting and error handling
    """
    try:
        # Show loading message
        if hasattr(update, 'message'):
            message = update.message
        else:
            message = update.callback_query.message
            
        loading_msg = await message.reply_text(
            f"🎬 Preparing '{title}'...\n⏳ Please wait"
        )
        
        # Determine content type and send appropriately
        if url.startswith("https://t.me/"):
            # Telegram channel post
            parts = url.split('/')
            message_id = int(parts[-1])
            from_chat_id = parts[-2]
            
            await context.bot.copy_message(
                chat_id=message.chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id
            )
        elif url.startswith(("http://", "https://")):
            # Direct URL
            keyboard = [[
                InlineKeyboardButton("📥 Download", url=url),
                InlineKeyboardButton("🎥 Trailer", callback_data=f"trailer_{title}")
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await message.reply_text(
                f"🎉 Here's your movie!\n**{title}**",
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            # Assume it's a file ID
            await context.bot.send_document(
                chat_id=message.chat_id,
                document=url,
                caption=f"🎬 {title}"
            )
        
        # Delete loading message
        await context.bot.delete_message(
            chat_id=message.chat_id,
            message_id=loading_msg.message_id
        )
        
    except TelegramError as e:
        logger.error(f"Failed to send content: {e}")
        await message.reply_text("❌ Failed to send the file. Please try again later.")

async def incremental_sync():
    """
    Perform incremental sync with Blogger API
    """
    try:
        async with db_cursor() as cur:
            # Get last sync time
            cur.execute(
                "SELECT last_sync_time FROM sync_history WHERE sync_type = 'blogger' ORDER BY id DESC LIMIT 1"
            )
            last_sync = cur.fetchone()
            last_sync_time = last_sync[0] if last_sync else datetime(2000, 1, 1)
            
            service = build('blogger', 'v3', developerKey=Config.BLOGGER_API_KEY)
            new_items = []
            
            # Fetch posts updated since last sync
            posts_request = service.posts().list(
                blogId=Config.BLOG_ID, 
                maxResults=500,
                fetchBodies=False,
                fetchImages=False,
                status='live'
            )
            
            while posts_request:
                posts_response = posts_request.execute()
                for item in posts_response.get('items', []):
                    updated = datetime.strptime(item['updated'], '%Y-%m-%dT%H:%M:%S%z')
                    
                    if updated > last_sync_time:
                        new_items.append((item['title'], item['url']))
                
                posts_request = service.posts().list_next(posts_request, posts_response)
            
            # Insert new items
            if new_items:
                execute_values(
                    cur,
                    "INSERT INTO movies (title, url) VALUES %s ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
                    new_items
                )
            
            # Update sync history
            cur.execute(
                "INSERT INTO sync_history (last_sync_time, items_processed, sync_type) VALUES (%s, %s, %s)",
                (datetime.now(), len(new_items), 'blogger')
            )
            
            return f"Incremental sync completed. Added/updated {len(new_items)} items."
            
    except (HttpError, Exception) as e:
        logger.error(f"Sync error: {e}")
        return f"Sync failed: {str(e)}"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Main message handler with AI intent analysis
    """
    try:
        user_message = update.message.text.strip()
        
        # Step 1: Basic filter (The Bouncer)
        if len(user_message) < 3 or is_spam(user_message):
            return  # Ignore short messages and spam
            
        # Step 2: AI Intent Analysis (The Brain)
        is_request, content_title = await analyze_message_intent(user_message)
        
        if not is_request:
            return  # Ignore non-movie requests
            
        # Use extracted title or fallback to original message
        search_query = content_title or user_message
        
        # Step 3: Search and respond
        movies = await search_movies(search_query)
        
        if not movies:
            # Store request for future notification
            user = update.effective_user
            await store_user_request(
                user.id, user.username, user.first_name,
                search_query, update.effective_chat.id, update.message.message_id
            )
            
            await update.message.reply_text(
                f"😔 '{search_query}' is not available yet.\n"
                "I'll notify you when it's added! 🔔"
            )
        elif len(movies) == 1:
            title, url = movies[0]
            await send_movie_content(update, context, title, url)
        else:
            await send_movie_options(update, movies)
            
    except Exception as e:
        logger.error(f"Message handling error: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.")

def is_spam(message: str) -> bool:
    """
    Basic spam detection
    """
    spam_indicators = [
        r'http[s]?://',  # URLs
        r'@\w+',         # Mentions
        r'#\w+',         # Hashtags
        r'[<>]',         # HTML tags
    ]
    
    return any(re.search(pattern, message) for pattern in spam_indicators)

async def store_user_request(user_id: int, username: str, first_name: str, 
                           movie_title: str, group_id: int = None, message_id: int = None):
    """
    Store user request in database
    """
    try:
        async with db_cursor() as cur:
            cur.execute("""
                INSERT INTO user_requests 
                (user_id, username, first_name, movie_title, group_id, message_id) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, movie_title) DO NOTHING
            """, (user_id, username, first_name, movie_title, group_id, message_id))
    except Exception as e:
        logger.error(f"Failed to store user request: {e}")

# Admin commands
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Show bot statistics to admin
    """
    if update.effective_user.id != Config.ADMIN_ID:
        return
        
    try:
        async with db_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM movies")
            movie_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM user_requests WHERE NOT notified")
            pending_requests = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
            unique_users = cur.fetchone()[0]
            
            stats_text = f"""
📊 *Bot Statistics*
• Total Movies: `{movie_count}`
• Pending Requests: `{pending_requests}`
• Unique Users: `{unique_users}`
• Last Sync: `{get_last_sync_time()}`
            """
            
            await update.message.reply_text(stats_text, parse_mode='Markdown')
            
    except Exception as e:
        logger.error(f"Stats error: {e}")
        await update.message.reply_text("❌ Failed to fetch statistics")

def get_last_sync_time() -> str:
    """
    Get last sync time from database
    """
    try:
        with db_pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_sync_time FROM sync_history ORDER BY id DESC LIMIT 1"
                )
                result = cur.fetchone()
                return result[0].strftime('%Y-%m-%d %H:%M') if result else "Never"
    except Exception as e:
        logger.error(f"Failed to get sync time: {e}")
        return "Unknown"
    finally:
        db_pool.putconn(conn)

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle errors gracefully
    """
    logger.error(f"Exception while handling update: {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ An error occurred. Please try again later."
            )
        except Exception:
            pass  # Avoid infinite error loop

def main():
    """
    Main application entry point
    """
    # Validate configuration
    if not all([Config.TELEGRAM_TOKEN, Config.DATABASE_URL]):
        logger.error("Missing required environment variables")
        return
        
    # Initialize components
    init_db()
    init_gemini()
    
    # Create application
    application = Application.builder().token(Config.TELEGRAM_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("stats", admin_stats))
    application.add_handler(CommandHandler("sync", sync_command))
    application.add_handler(CallbackQueryHandler(handle_movie_selection, pattern="^movie_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Start periodic tasks
    application.job_queue.run_repeating(
        periodic_sync, interval=timedelta(hours=6), first=10
    )
    
    # Start the bot
    logger.info("Bot is starting...")
    application.run_polling()

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle /start command
    """
    welcome_text = """
🎬 *Welcome to FilmFyBox!*

I'm Manvi! Send me any movie/series name and I'll provide you with the link.

✨ *Features:*
• 10,000+ Movies/Series
• Smart search with fuzzy matching
• Quality content
• Regular updates

Just type a movie name and enjoy! 🍿
    """
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Manual sync command for admin
    """
    if update.effective_user.id != Config.ADMIN_ID:
        return
        
    message = await update.message.reply_text("🔄 Starting sync...")
    result = await incremental_sync()
    await message.edit_text(f"🔄 Sync Result: {result}")

async def periodic_sync(context: ContextTypes.DEFAULT_TYPE):
    """
    Periodic sync task
    """
    logger.info("Running periodic sync")
    try:
        result = await incremental_sync()
        logger.info(f"Periodic sync completed: {result}")
    except Exception as e:
        logger.error(f"Periodic sync failed: {e}")

if __name__ == "__main__":
    main()
