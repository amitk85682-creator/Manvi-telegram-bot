import os
import asyncio
import logging
import json
import re
import aiohttp
import psycopg2
from psycopg2 import pool
import threading
import telegram
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler, CallbackQueryHandler
)
from datetime import datetime
import time
import socket
from urllib.parse import urlparse
from flask import Flask, request, jsonify
from waitress import serve
import google.generativeai as genai
import redis

# --- Enhanced Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Centralized configuration class to fetch environment variables
class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
    DATABASE_URL = os.environ.get('DATABASE_URL')
    REDIS_URL = os.environ.get('REDIS_URL') # Optional: e.g., 'redis://localhost:6379'
    ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
    ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')
    PORT = int(os.environ.get('PORT', 8080))
    REQUEST_LIMIT = int(os.environ.get('REQUEST_LIMIT', 20))
    REQUEST_WINDOW = int(os.environ.get('REQUEST_WINDOW', 3600)) # 1 hour
    UPDATE_SECRET = os.environ.get('UPDATE_SECRET', 'default_secret_123')


# --- Conversation States ---
class States:
    MAIN_MENU, SEARCHING, REQUESTING, FEEDBACK = range(4)


# --- Flask App Initialization ---
app = Flask(__name__)


# --- Redis Connection ---
redis_conn = None
if Config.REDIS_URL:
    try:
        redis_conn = redis.from_url(Config.REDIS_URL)
        redis_conn.ping()
        logger.info("Redis connected successfully.")
    except redis.ConnectionError as e:
        logger.warning(f"Redis connection failed: {e}. Proceeding without cache.")
        redis_conn = None


# --- Database Module ---
class Database:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls):
        """Initializes the connection pool, forcing IPv4 resolution."""
        if not Config.DATABASE_URL:
            logger.error("DATABASE_URL is not set. Cannot initialize database.")
            return False
        try:
            if cls._connection_pool is None:
                # FIX: Parse DB URL and resolve hostname to IPv4 to prevent network errors on Render
                result = urlparse(Config.DATABASE_URL)
                hostname = result.hostname
                ipv4_address = socket.gethostbyname(hostname)
                logger.info(f"Resolved database host '{hostname}' to IPv4: {ipv4_address}")

                cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    user=result.username,
                    password=result.password,
                    host=ipv4_address,
                    port=result.port,
                    database=result.path[1:]
                )
                logger.info("Database connection pool initialized successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            return False

    @classmethod
    def execute_query(cls, query, params=None, fetch=False):
        """Executes a synchronous database query."""
        conn = None
        try:
            if cls._connection_pool is None:
                raise Exception("Database pool is not initialized.")
            conn = cls._connection_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(query, params)
                result = cur.fetchall() if fetch else None
                conn.commit()
                return result
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database query error: {e}")
            raise
        finally:
            if conn:
                cls._connection_pool.putconn(conn)
    
    @classmethod
    async def async_execute_query(cls, query, params=None, fetch=False):
        """
        Executes a blocking database query asynchronously in a separate thread
        to avoid blocking the main asyncio event loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,  # Uses the default thread pool executor
            cls.execute_query,
            query,
            params,
            fetch
        )


async def setup_database(retries=3, delay=2):
    """Sets up database tables asynchronously."""
    if not Database.initialize_pool():
        logger.critical("Database pool initialization failed on first attempt.")
        return False

    for attempt in range(retries):
        try:
            # FIX: Switched to async_execute_query for all setup queries
            await Database.async_execute_query('''
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY, title TEXT NOT NULL, url TEXT NOT NULL, file_id TEXT,
                    quality TEXT, size TEXT, language TEXT, year INTEGER, imdb_rating FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(title, quality, language)
                )
            ''')
            await Database.async_execute_query('''
                CREATE TABLE IF NOT EXISTS movie_aliases (
                    id SERIAL PRIMARY KEY, movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                    alias TEXT NOT NULL, UNIQUE(movie_id, alias)
                )
            ''')
            await Database.async_execute_query('''
                CREATE TABLE IF NOT EXISTS user_requests (
                    id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, username TEXT, first_name TEXT,
                    movie_title TEXT NOT NULL, requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified BOOLEAN DEFAULT FALSE, group_id BIGINT, message_id BIGINT,
                    priority INTEGER DEFAULT 1, UNIQUE(user_id, movie_title)
                )
            ''')
            await Database.async_execute_query('''
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT,
                    search_count INTEGER DEFAULT 0, request_count INTEGER DEFAULT 0,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await Database.async_execute_query('''
                CREATE TABLE IF NOT EXISTS feedback (
                    id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, message TEXT NOT NULL,
                    rating INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info("Database setup completed successfully.")
            return True
        except Exception as e:
            logger.error(f"Database setup attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                logger.critical("All database setup attempts failed.")
                return False

# --- Core Logic Modules (Refactored for Async) ---

async def async_store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    """Stores user movie request in the database asynchronously."""
    try:
        await Database.async_execute_query('''
            INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, movie_title) DO UPDATE SET
            requested_at = CURRENT_TIMESTAMP, notified = FALSE
        ''', (user_id, username, first_name, movie_title, group_id, message_id))
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        return False

class MovieSearch:
    @staticmethod
    async def search_movie(title, max_results=5):
        try:
            db_results = await Database.async_execute_query(
                "SELECT title, url, file_id, quality, size FROM movies WHERE title ILIKE %s LIMIT %s",
                (f'%{title}%', max_results),
                fetch=True
            )
            return [{
                'title': r[0], 'url': r[1], 'file_id': r[2],
                'quality': r[3], 'size': r[4], 'source': 'database'
            } for r in db_results]
        except Exception as e:
            logger.error(f"Database search error: {e}")
            return []

class AIAssistant:
    def __init__(self):
        self.model = None
        if Config.GEMINI_API_KEY:
            try:
                genai.configure(api_key=Config.GEMINI_API_KEY)
                self.model = genai.GenerativeModel(model_name='gemini-pro')
            except Exception as e:
                logger.error(f"Failed to initialize AI model: {e}")

    async def analyze_intent(self, message_text):
        if not self.model:
            return self._fallback_intent_analysis(message_text)
        try:
            prompt = f"""
            Analyze if this message is requesting a movie or series: "{message_text}"
            Respond with JSON: {{"is_request": boolean, "content_title": string|null}}
            """
            # FIX: Use the async version of the Gemini API
            response = await self.model.generate_content_async(prompt)
            json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
        return self._fallback_intent_analysis(message_text)

    def _fallback_intent_analysis(self, message_text):
        keywords = ["movie", "film", "series", "watch", "download", "चलचित्र", "फिल्म"]
        if any(keyword in message_text.lower() for keyword in keywords):
            return {"is_request": True, "content_title": message_text}
        return {"is_request": False, "content_title": None}

class UserManager:
    @staticmethod
    async def async_track_activity(user_id, username, first_name, action_type):
        try:
            await Database.async_execute_query('''
                INSERT INTO user_stats (user_id, username, first_name, last_active)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET
                last_active = CURRENT_TIMESTAMP, username = EXCLUDED.username, first_name = EXCLUDED.first_name
            ''', (user_id, username, first_name))

            if action_type in ['search', 'request']:
                await Database.async_execute_query(
                    f'UPDATE user_stats SET {action_type}_count = {action_type}_count + 1 WHERE user_id = %s',
                    (user_id,)
                )
        except Exception as e:
            logger.error(f"Error tracking user activity: {e}")

class NotificationSystem:
    @staticmethod
    async def notify_admin(context, user, movie_title):
        if not Config.ADMIN_CHANNEL_ID: return
        try:
            user_info = f"User: {user.first_name or 'N/A'} (@{user.username or 'N/A'}, ID: {user.id})"
            message = f"🎬 New Movie Request!\n\nMovie: {movie_title}\n{user_info}\nTime: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}"
            await context.bot.send_message(chat_id=Config.ADMIN_CHANNEL_ID, text=message)
        except Exception as e:
            logger.error(f"Error sending admin notification: {e}")

# --- UI Components ---
class Keyboards:
    @staticmethod
    def main_menu():
        return ReplyKeyboardMarkup([
            ['🔍 Search Movies', '🙋 Request Movie'],
            ['📊 My Stats', '⭐ Rate Us', '❓ Help']
        ], resize_keyboard=True)

    @staticmethod
    def request_confirmation(movie_title):
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, Request It", callback_data=f"request_{movie_title}")
        ]])

# --- Telegram Bot Handlers ---
class BotHandlers:
    def __init__(self):
        self.ai_assistant = AIAssistant()

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        await UserManager.async_track_activity(user.id, user.username, user.first_name, 'start')
        welcome_text = "🎬 Welcome to MovieFinder Bot! 🎬\n\nI can help you find and request movies. Use the buttons below to get started!"
        await update.message.reply_text(welcome_text, reply_markup=Keyboards.main_menu())
        return States.MAIN_MENU

    async def search_movies_entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🔍 Enter the name of the movie or series you want to search for:")
        return States.SEARCHING

    async def perform_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        query = update.message.text.strip()

        if len(query) < 3:
            await update.message.reply_text("Please enter at least 3 characters to search.")
            return States.SEARCHING

        await UserManager.async_track_activity(user.id, user.username, user.first_name, 'search')
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
        results = await MovieSearch.search_movie(query)

        if results:
            await update.message.reply_text(f"🎉 Found {len(results)} result(s) for '{query}':")
            for res in results:
                caption = f"🎬 **{res['title']}**\n"
                if res.get('quality'): caption += f"🎞️ Quality: {res['quality']}\n"
                if res.get('size'): caption += f"💾 Size: {res['size']}\n"
                
                watch_button = InlineKeyboardButton("▶️ Watch Now", url=res['url'])
                if res.get('file_id'):
                    await update.message.reply_document(
                        document=res['file_id'], caption=caption, parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup([[watch_button]])
                    )
                else:
                    await update.message.reply_text(
                        caption, parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup([[watch_button]])
                    )
        else:
            response = f"😔 Sorry, '{query}' is not in our collection yet."
            await update.message.reply_text(response, reply_markup=Keyboards.request_confirmation(query))
        
        await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
        return States.MAIN_MENU

    async def request_movie_entry(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("📝 Enter the full name of the movie or series you want to request:")
        return States.REQUESTING

    async def perform_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        movie_title = update.message.text.strip()
        
        if len(movie_title) < 3:
            await update.message.reply_text("Please enter a valid name (at least 3 characters).")
            return States.REQUESTING

        await UserManager.async_track_activity(user.id, user.username, user.first_name, 'request')
        if await async_store_user_request(user.id, user.username, user.first_name, movie_title):
            await NotificationSystem.notify_admin(context, user, movie_title)
            await update.message.reply_text(f"✅ Your request for '{movie_title}' has been recorded! We'll notify you when it's available.")
        else:
            await update.message.reply_text("❌ There was an error with your request. You might have requested this already.")
        
        await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
        return States.MAIN_MENU

    async def my_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        try:
            stats = await Database.async_execute_query(
                "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = %s",
                (user.id,), fetch=True
            )
            if stats:
                s_count, r_count, l_active = stats[0]
                response = f"📊 Your Stats:\n\n🔍 Searches: {s_count}\n🙋 Requests: {r_count}\n🕐 Last Active: {l_active.strftime('%Y-%m-%d')}"
            else:
                response = "📊 No activity recorded yet."
            await update.message.reply_text(response)
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            await update.message.reply_text("❌ Error retrieving your stats.")
        return States.MAIN_MENU

    async def rate_us(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Please share your feedback or suggestions:")
        return States.FEEDBACK
    
    async def process_feedback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        feedback_text = update.message.text.strip()
        try:
            await Database.async_execute_query(
                "INSERT INTO feedback (user_id, message) VALUES (%s, %s)",
                (user.id, feedback_text)
            )
            await update.message.reply_text("✅ Thank you for your feedback!")
        except Exception as e:
            logger.error(f"Error storing feedback: {e}")
        
        await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
        return States.MAIN_MENU

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = "🤖 How to use me:\n\n🔍 **Search Movies** - Find movies in our database.\n🙋 **Request Movie** - Ask for movies we don't have.\n\nType /cancel to exit any operation."
        await update.message.reply_text(help_text, parse_mode='Markdown')
        return States.MAIN_MENU

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data

        if data.startswith('request_'):
            movie_title = data.replace('request_', '', 1)
            user = update.effective_user
            await UserManager.async_track_activity(user.id, user.username, user.first_name, 'request')
            if await async_store_user_request(user.id, user.username, user.first_name, movie_title):
                await NotificationSystem.notify_admin(context, user, movie_title)
                await query.edit_message_text(f"✅ Your request for '{movie_title}' has been recorded!")
            else:
                await query.edit_message_text("❌ Error processing your request.")

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Operation cancelled.", reply_markup=Keyboards.main_menu())
        return States.MAIN_MENU

# --- Flask Web Routes ---
@app.route('/')
def home():
    return jsonify({"status": "online", "service": "MovieFinder Bot API"})

@app.route('/stats')
def stats():
    try:
        # Flask runs in a separate thread, so we can use the synchronous DB method here
        users = Database.execute_query("SELECT COUNT(*) FROM user_stats", fetch=True)[0][0]
        movies = Database.execute_query("SELECT COUNT(*) FROM movies", fetch=True)[0][0]
        requests = Database.execute_query("SELECT COUNT(*) FROM user_requests WHERE notified = FALSE", fetch=True)[0][0]
        return jsonify({"total_users": users, "movies_in_db": movies, "pending_requests": requests})
    except Exception as e:
        logger.error(f"Flask stats error: {e}")
        return jsonify({"error": "Could not retrieve stats"}), 500

# --- Main Application Runner ---
async def run_bot():
    """Initializes and runs the Telegram bot."""
    if not Config.TELEGRAM_BOT_TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN is not set. Bot cannot start.")
        return

    application = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()
    handlers = BotHandlers()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', handlers.start)],
        states={
            States.MAIN_MENU: [
                MessageHandler(filters.Regex('^🔍 Search Movies$'), handlers.search_movies_entry),
                MessageHandler(filters.Regex('^🙋 Request Movie$'), handlers.request_movie_entry),
                MessageHandler(filters.Regex('^📊 My Stats$'), handlers.my_stats),
                MessageHandler(filters.Regex('^⭐ Rate Us$'), handlers.rate_us),
                MessageHandler(filters.Regex('^❓ Help$'), handlers.help_command),
            ],
            States.SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.perform_search)],
            States.REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.perform_request)],
            States.FEEDBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.process_feedback)],
        },
        fallbacks=[CommandHandler('cancel', handlers.cancel), CommandHandler('start', handlers.start)],
    )
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(handlers.button_callback))
    application.add_handler(CommandHandler("help", handlers.help_command))
    
    logger.info("Bot is starting polling...")
    await application.run_polling(drop_pending_updates=True)


def run_flask():
    """Runs the Flask app using a production-ready server."""
    logger.info(f"Starting Flask server on host 0.0.0.0 port {Config.PORT}...")
    serve(app, host='0.0.0.0', port=Config.PORT)


async def main():
    """Initializes the application and starts both the Bot and Flask server."""
    if not await setup_database():
        logger.critical("Shutting down due to database setup failure.")
        return
    
    # Run Flask in a separate, daemonized thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Run the bot in the main asyncio event loop
    await run_bot()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Application failed to run: {e}", exc_info=True)
