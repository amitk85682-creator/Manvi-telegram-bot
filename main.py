import os
import asyncio
import logging
import json
import re
import aiohttp
import asyncpg  # Replaced psycopg2 with asyncpg for better async support
import redis
import telegram
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler, CallbackQueryHandler
)
from datetime import datetime, timedelta
from functools import wraps
import google.generativeai as genai
import hashlib
import time
import ssl

# Enhanced logging configuration
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration with enhanced connection settings
class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
    DATABASE_URL = os.environ.get('DATABASE_URL')
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
    BLOG_ID = os.environ.get('BLOG_ID')
    UPDATE_SECRET = os.environ.get('UPDATE_SECRET', 'default_secret_123')
    ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
    GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
    ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')
    PORT = int(os.environ.get('PORT', 5000))
    REQUEST_LIMIT = int(os.environ.get('REQUEST_LIMIT', 20))
    REQUEST_WINDOW = int(os.environ.get('REQUEST_WINDOW', 3600))
    
    # Database connection settings
    DB_MAX_CONNECTIONS = int(os.environ.get('DB_MAX_CONNECTIONS', 10))
    DB_MIN_CONNECTIONS = int(os.environ.get('DB_MIN_CONNECTIONS', 1))
    DB_COMMAND_TIMEOUT = int(os.environ.get('DB_COMMAND_TIMEOUT', 30))

# Conversation states
class States:
    MAIN_MENU, SEARCHING, REQUESTING, FEEDBACK = range(4)

# Enhanced Redis connection with retry logic
class RedisManager:
    _instance = None
    _redis_conn = None
    
    @classmethod
    async def get_connection(cls):
        if cls._redis_conn is None:
            await cls.initialize_redis()
        return cls._redis_conn
    
    @classmethod
    async def initialize_redis(cls, retries=3, delay=2):
        for attempt in range(retries):
            try:
                if Config.REDIS_URL:
                    # Parse Redis URL for proper configuration
                    redis_url = Config.REDIS_URL
                    if redis_url.startswith('rediss://'):  # SSL connection
                        cls._redis_conn = redis.from_url(
                            redis_url,
                            ssl_cert_reqs=ssl.CERT_NONE,
                            decode_responses=True
                        )
                    else:
                        cls._redis_conn = redis.from_url(redis_url, decode_responses=True)
                    
                    # Test connection
                    cls._redis_conn.ping()
                    logger.info("✅ Redis connected successfully")
                    return True
                else:
                    logger.warning("⚠️ No REDIS_URL provided, proceeding without Redis")
                    return False
            except Exception as e:
                logger.warning(f"Redis connection attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    logger.error("❌ All Redis connection attempts failed")
                    cls._redis_conn = None
                    return False

# Async Database connection pool
class Database:
    _pool = None
    
    @classmethod
    async def get_pool(cls):
        if cls._pool is None:
            await cls.initialize_pool()
        return cls._pool
    
    @classmethod
    async def initialize_pool(cls, retries=5, delay=3):
        for attempt in range(retries):
            try:
                # Parse DATABASE_URL and ensure SSL for Supabase
                db_url = Config.DATABASE_URL
                
                # For Supabase, we need to use SSL
                if 'supabase' in db_url:
                    # Ensure SSL parameters are included
                    if 'sslmode' not in db_url:
                        db_url += '?sslmode=require'
                
                logger.info(f"🔄 Attempting database connection (attempt {attempt + 1})...")
                
                cls._pool = await asyncpg.create_pool(
                    dsn=db_url,
                    min_size=Config.DB_MIN_CONNECTIONS,
                    max_size=Config.DB_MAX_CONNECTIONS,
                    command_timeout=Config.DB_COMMAND_TIMEOUT,
                    # SSL configuration for Supabase
                    ssl='require' if 'supabase' in db_url else False
                )
                
                # Test connection
                async with cls._pool.acquire() as conn:
                    await conn.execute('SELECT 1')
                
                logger.info("✅ Database connection pool initialized successfully")
                return True
                
            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logger.error("❌ All database connection attempts failed")
                    raise e
    
    @classmethod
    async def execute_query(cls, query, params=None, fetch=False, fetchval=False, fetchrow=False):
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            try:
                if fetch:
                    return await conn.fetch(query, *params) if params else await conn.fetch(query)
                elif fetchval:
                    return await conn.fetchval(query, *params) if params else await conn.fetchval(query)
                elif fetchrow:
                    return await conn.fetchrow(query, *params) if params else await conn.fetchrow(query)
                else:
                    return await conn.execute(query, *params) if params else await conn.execute(query)
            except Exception as e:
                logger.error(f"Database query error: {e} - Query: {query}")
                raise e

# Enhanced database setup with proper error handling
async def setup_database(retries=5, delay=3):
    """Setup database tables with proper error handling"""
    for attempt in range(retries):
        try:
            logger.info(f"🔄 Setting up database (attempt {attempt + 1})...")
            
            # Initialize pool first
            await Database.initialize_pool()
            
            # Create tables
            tables_queries = [
                '''
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    file_id TEXT,
                    quality TEXT,
                    size TEXT,
                    language TEXT,
                    year INTEGER,
                    imdb_rating FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(title, quality, language)
                )
                ''',
                '''
                CREATE TABLE IF NOT EXISTS movie_aliases (
                    id SERIAL PRIMARY KEY,
                    movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                    alias TEXT NOT NULL,
                    UNIQUE(movie_id, alias)
                )
                ''',
                '''
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
                    priority INTEGER DEFAULT 1
                )
                ''',
                '''
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    search_count INTEGER DEFAULT 0,
                    request_count INTEGER DEFAULT 0,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''',
                '''
                CREATE TABLE IF NOT EXISTS feedback (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    message TEXT NOT NULL,
                    rating INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                '''
            ]
            
            for query in tables_queries:
                await Database.execute_query(query)
            
            # Create indexes for better performance
            index_queries = [
                'CREATE INDEX IF NOT EXISTS idx_movies_title ON movies(title)',
                'CREATE INDEX IF NOT EXISTS idx_movies_title_lower ON movies(LOWER(title))',
                'CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests(user_id)',
                'CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests(movie_title)',
                'CREATE INDEX IF NOT EXISTS idx_user_requests_notified ON user_requests(notified)'
            ]
            
            for query in index_queries:
                await Database.execute_query(query)
            
            logger.info("✅ Database setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Database setup attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying database setup in {delay} seconds...")
                await asyncio.sleep(delay)
            else:
                logger.error("❌ All database setup attempts failed")
                return False

# Store user request function
async def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    """Store user movie request in database"""
    try:
        await Database.execute_query('''
            INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (user_id, movie_title) DO UPDATE SET
            requested_at = CURRENT_TIMESTAMP
        ''', (user_id, username, first_name, movie_title, group_id, message_id))
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        return False

# Enhanced movie search with multiple sources
class MovieSearch:
    @staticmethod
    async def search_movie(title, max_results=5):
        results = []
        
        # Search in database first
        try:
            db_results = await Database.execute_query(
                "SELECT title, url, file_id, quality, size FROM movies WHERE title ILIKE $1 OR id IN (SELECT movie_id FROM movie_aliases WHERE alias ILIKE $2) LIMIT $3",
                f'%{title}%', f'%{title}%', max_results,
                fetch=True
            )
            
            for result in db_results:
                results.append({
                    'title': result['title'],
                    'url': result['url'],
                    'file_id': result['file_id'],
                    'quality': result['quality'],
                    'size': result['size'],
                    'source': 'database'
                })
        except Exception as e:
            logger.error(f"Database search error: {e}")
        
        return results

# AI-powered intent recognition with fallback
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
            Respond with JSON: {{"is_request": boolean, "content_title": string|null, "confidence": float}}
            """
            
            response = self.model.generate_content(prompt)
            json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            return self._fallback_intent_analysis(message_text)
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._fallback_intent_analysis(message_text)
    
    def _fallback_intent_analysis(self, message_text):
        movie_keywords = ["movie", "film", "series", "watch", "download", "see", "चलचित्र", "फिल्म", "सीरीज"]
        if any(keyword in message_text.lower() for keyword in movie_keywords):
            return {"is_request": True, "content_title": message_text, "confidence": 0.7}
        return {"is_request": False, "content_title": None, "confidence": 0.6}

# User management and analytics
class UserManager:
    @staticmethod
    async def track_activity(user_id, username, first_name, action_type):
        try:
            # Update user stats
            await Database.execute_query('''
                INSERT INTO user_stats (user_id, username, first_name, last_active)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET
                last_active = CURRENT_TIMESTAMP,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
            ''', (user_id, username, first_name))
            
            # Increment specific counter
            if action_type == 'search':
                await Database.execute_query('''
                    UPDATE user_stats SET search_count = search_count + 1 
                    WHERE user_id = $1
                ''', (user_id,))
            elif action_type == 'request':
                await Database.execute_query('''
                    UPDATE user_stats SET request_count = request_count + 1 
                    WHERE user_id = $1
                ''', (user_id,))
                
        except Exception as e:
            logger.error(f"Error tracking user activity: {e}")

# Rate limiting with Redis fallback
async def check_rate_limit(user_id):
    """Check if user has exceeded rate limit"""
    try:
        redis_conn = await RedisManager.get_connection()
        if redis_conn:
            key = f"rate_limit:{user_id}"
            current = redis_conn.get(key)
            if current and int(current) >= Config.REQUEST_LIMIT:
                return False
            
            if current:
                redis_conn.incr(key)
            else:
                redis_conn.setex(key, Config.REQUEST_WINDOW, 1)
            return True
        else:
            # If Redis is not available, allow all requests
            return True
    except Exception as e:
        logger.error(f"Rate limit check error: {e}")
        return True  # Allow request if rate limiting fails

# Notification system with multiple channels
class NotificationSystem:
    @staticmethod
    async def notify_admin(context, user, movie_title, group_info=None):
        if not Config.ADMIN_CHANNEL_ID:
            return
        
        try:
            user_info = f"User: {user.first_name or 'Unknown'}"
            if user.username:
                user_info += f" (@{user.username})"
            user_info += f" (ID: {user.id})"
            
            group_info_text = f"From Group: {group_info}" if group_info else "Via Private Message"
            
            message = f"""
🎬 New Movie Request! 🎬

Movie: {movie_title}
{user_info}
{group_info_text}
Time: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}
            """
            
            await context.bot.send_message(chat_id=Config.ADMIN_CHANNEL_ID, text=message)
        except Exception as e:
            logger.error(f"Error sending admin notification: {e}")
    
    @staticmethod
    async def notify_users(context, movie_title, movie_url, file_id=None):
        logger.info(f"Notifying users about: {movie_title}")
        try:
            users = await Database.execute_query(
                "SELECT user_id, group_id FROM user_requests WHERE movie_title ILIKE $1 AND notified = FALSE",
                f'%{movie_title}%',
                fetch=True
            )
            
            notified_count = 0
            for user in users:
                user_id = user['user_id']
                try:
                    if file_id:
                        await context.bot.send_document(chat_id=user_id, document=file_id)
                    else:
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=f"🎉 Your requested movie '{movie_title}' is now available!",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("Watch Now", url=movie_url)]
                            ])
                        )
                    
                    # Mark as notified
                    await Database.execute_query(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = $1 AND movie_title ILIKE $2",
                        (user_id, f'%{movie_title}%')
                    )
                    notified_count += 1
                    
                except telegram.error.Forbidden:
                    logger.warning(f"User {user_id} blocked the bot")
                except Exception as e:
                    logger.error(f"Error notifying user {user_id}: {e}")
            
            logger.info(f"Notified {notified_count} users about {movie_title}")
            return notified_count
            
        except Exception as e:
            logger.error(f"Error in user notification: {e}")
            return 0

# Keyboard layouts
class Keyboards:
    @staticmethod
    def main_menu():
        keyboard = [
            ['🔍 Search Movies', '🙋 Request Movie'],
            ['📊 My Stats', '⭐ Rate Us', '❓ Help']
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    
    @staticmethod
    def movie_options(movie_title, url, quality=None, size=None):
        buttons = [[InlineKeyboardButton("🎬 Watch Now", url=url)]]
        
        if quality:
            buttons.append([InlineKeyboardButton(f"Quality: {quality}", callback_data="info_quality")])
        if size:
            buttons.append([InlineKeyboardButton(f"Size: {size}", callback_data="info_size")])
            
        buttons.append([InlineKeyboardButton("📥 Download", callback_data=f"download_{movie_title}")])
        
        return InlineKeyboardMarkup(buttons)
    
    @staticmethod
    def rating_options():
        keyboard = [
            [InlineKeyboardButton("⭐", callback_data="rate_1"),
             InlineKeyboardButton("⭐⭐", callback_data="rate_2"),
             InlineKeyboardButton("⭐⭐⭐", callback_data="rate_3"),
             InlineKeyboardButton("⭐⭐⭐⭐", callback_data="rate_4"),
             InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="rate_5")]
        ]
        return InlineKeyboardMarkup(keyboard)

# Telegram bot handlers
class BotHandlers:
    def __init__(self, application):
        self.app = application
        self.ai_assistant = AIAssistant()
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user = update.effective_user
            await UserManager.track_activity(user.id, user.username, user.first_name, 'start')
            
            welcome_text = """
            🎬 Welcome to MovieFinder Bot! 🎬

            I can help you find and request movies and series.

            Features:
            • Search our extensive movie database
            • Request movies we don't have yet
            • Get notified when requested movies become available
            • Multi-language support

            Use the buttons below to get started!
            """
            
            await update.message.reply_text(welcome_text, reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
        except Exception as e:
            logger.error(f"Error in start command: {e}")
            return States.MAIN_MENU
    
    async def search_movies(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user_message = update.message.text.strip()
            user = update.effective_user
            
            # Apply rate limiting
            if not await check_rate_limit(user.id):
                await update.message.reply_text("🚫 You've reached your hourly request limit. Please try again later.")
                return States.MAIN_MENU
            
            await UserManager.track_activity(user.id, user.username, user.first_name, 'search')
            
            if len(user_message) < 3:
                await update.message.reply_text("Please enter at least 3 characters to search.")
                return States.SEARCHING
            
            # Show typing indicator
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
            
            results = await MovieSearch.search_movie(user_message)
            
            if results:
                # Send first result immediately
                first_result = results[0]
                response = f"🎉 Found {len(results)} results!\n\nTop result: {first_result['title']}"
                
                if first_result.get('quality'):
                    response += f"\nQuality: {first_result['quality']}"
                if first_result.get('size'):
                    response += f" | Size: {first_result['size']}"
                
                if first_result.get('file_id'):
                    await update.message.reply_document(
                        document=first_result['file_id'],
                        caption=response
                    )
                else:
                    await update.message.reply_text(
                        response,
                        reply_markup=Keyboards.movie_options(
                            first_result['title'],
                            first_result['url'],
                            first_result.get('quality'),
                            first_result.get('size')
                        )
                    )
                
                # Send remaining results if any
                if len(results) > 1:
                    for result in results[1:]:
                        if result.get('file_id'):
                            await context.bot.send_document(
                                chat_id=update.effective_chat.id,
                                document=result['file_id'],
                                caption=result['title']
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=update.effective_chat.id,
                                text=result['title'],
                                reply_markup=Keyboards.movie_options(
                                    result['title'],
                                    result['url'],
                                    result.get('quality'),
                                    result.get('size')
                                )
                            )
            else:
                # No results found, offer to request the movie
                await store_user_request(
                    user.id, user.username, user.first_name, user_message,
                    update.effective_chat.id if update.effective_chat.type != "private" else None,
                    update.message.message_id
                )
                
                response = f"😔 Sorry, '{user_message}' is not in our collection yet. Would you like to request it?"
                keyboard = [[InlineKeyboardButton("✅ Yes, Request It", callback_data=f"request_{user_message}")]]
                await update.message.reply_text(
                    response, 
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
            await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
            
        except Exception as e:
            logger.error(f"Error in search movies: {e}")
            await update.message.reply_text("Sorry, something went wrong. Please try again.")
            return States.MAIN_MENU

# Additional handler functions
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu selections"""
    text = update.message.text
    user = update.effective_user
    
    if text == '🔍 Search Movies':
        await update.message.reply_text("🔍 Enter movie name to search:")
        return States.SEARCHING
    elif text == '🙋 Request Movie':
        await update.message.reply_text("📝 Enter the movie name you want to request:")
        return States.REQUESTING
    elif text == '📊 My Stats':
        try:
            stats = await Database.execute_query(
                "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = $1",
                (user.id,),
                fetchrow=True
            )
            if stats:
                response = f"📊 Your Stats:\n\n🔍 Searches: {stats['search_count']}\n🙋 Requests: {stats['request_count']}\n🕐 Last Active: {stats['last_active'].strftime('%Y-%m-%d %H:%M')}"
            else:
                response = "📊 Your Stats:\n\nNo activity recorded yet."
            await update.message.reply_text(response)
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            await update.message.reply_text("❌ Error retrieving your stats.")
    elif text == '⭐ Rate Us':
        await update.message.reply_text("Please rate our service:", reply_markup=Keyboards.rating_options())
    elif text == '❓ Help':
        help_text = """
🤖 How to use MovieFinder Bot:

🔍 Search Movies - Find movies in our database
🙋 Request Movie - Request movies we don't have
📊 My Stats - View your usage statistics
⭐ Rate Us - Rate our service

Commands:
/start - Start the bot
/stats - View your statistics  
/help - Show this help message
        """
        await update.message.reply_text(help_text)
    
    return States.MAIN_MENU

async def request_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie requests"""
    movie_title = update.message.text.strip()
    user = update.effective_user
    
    if len(movie_title) < 2:
        await update.message.reply_text("Please enter a valid movie name (at least 2 characters).")
        return States.REQUESTING
    
    # Store the request
    success = await store_user_request(user.id, user.username, user.first_name, movie_title)
    
    if success:
        # Notify admin
        try:
            await NotificationSystem.notify_admin(context, user, movie_title)
        except Exception as e:
            logger.error(f"Error notifying admin: {e}")
        
        await update.message.reply_text(f"✅ Your request for '{movie_title}' has been recorded! We'll notify you when it's available.")
    else:
        await update.message.reply_text("❌ Sorry, there was an error processing your request. Please try again.")
    
    await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU

async def process_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user feedback"""
    feedback_text = update.message.text.strip()
    user = update.effective_user
    
    try:
        await Database.execute_query(
            "INSERT INTO feedback (user_id, message) VALUES ($1, $2)",
            (user.id, feedback_text)
        )
        await update.message.reply_text("✅ Thank you for your feedback!")
    except Exception as e:
        logger.error(f"Error storing feedback: {e}")
        await update.message.reply_text("❌ Error saving your feedback.")
    
    await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith('rate_'):
        rating = int(data.split('_')[1])
        user = update.effective_user
        
        try:
            await Database.execute_query(
                "INSERT INTO feedback (user_id, message, rating) VALUES ($1, $2, $3)",
                (user.id, f"User rating: {rating} stars", rating)
            )
            await query.edit_message_text(f"✅ Thank you for your {rating} star rating!")
        except Exception as e:
            logger.error(f"Error storing rating: {e}")
            await query.edit_message_text("❌ Error saving your rating.")
    
    elif data.startswith('request_'):
        movie_title = data.replace('request_', '')
        user = update.effective_user
        
        success = await store_user_request(user.id, user.username, user.first_name, movie_title)
        if success:
            await query.edit_message_text(f"✅ Your request for '{movie_title}' has been recorded!")
        else:
            await query.edit_message_text("❌ Error processing your request.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel conversation"""
    await update.message.reply_text("Operation cancelled.", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU

async def user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user statistics"""
    user = update.effective_user
    try:
        stats = await Database.execute_query(
            "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = $1",
            (user.id,),
            fetchrow=True
        )
        if stats:
            response = f"📊 Your Stats:\n\n🔍 Searches: {stats['search_count']}\n🙋 Requests: {stats['request_count']}\n🕐 Last Active: {stats['last_active'].strftime('%Y-%m-%d %H:%M')}"
        else:
            response = "📊 Your Stats:\n\nNo activity recorded yet."
        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
        await update.message.reply_text("❌ Error retrieving your stats.")

async def feedback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start feedback conversation"""
    await update.message.reply_text("💬 Please share your feedback or suggestions:")
    return States.FEEDBACK

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message"""
    help_text = """
🤖 How to use MovieFinder Bot:

🔍 Search Movies - Find movies in our database
🙋 Request Movie - Request movies we don't have
📊 My Stats - View your usage statistics
⭐ Rate Us - Rate our service

Commands:
/start - Start the bot
/stats - View your statistics
/feedback - Share feedback
/help - Show this help message

Need help? Contact support.
    """
    await update.message.reply_text(help_text)

async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add movies (placeholder)"""
    user = update.effective_user
    if user.id != Config.ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is for administrators only.")
        return
    
    await update.message.reply_text("📝 Admin movie addition feature will be implemented here.")

# Main bot runner
async def run_bot():
    """Run the Telegram bot asynchronously"""
    try:
        logger.info("🔄 Starting bot initialization...")
        
        # Initialize connections first
        await RedisManager.initialize_redis()
        await setup_database()
        
        # Create application
        application = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()
        
        # Initialize handlers
        bot_handlers = BotHandlers(application)
        
        # Add conversation handler
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', bot_handlers.start)],
            states={
                States.MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)],
                States.SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot_handlers.search_movies)],
                States.REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_movie)],
                States.FEEDBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_feedback)],
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )
        
        application.add_handler(conv_handler)
        application.add_handler(CallbackQueryHandler(button_callback))
        
        # Add command handlers
        application.add_handler(CommandHandler("addmovie", add_movie))
        application.add_handler(CommandHandler("stats", user_stats))
        application.add_handler(CommandHandler("feedback", feedback_handler))
        application.add_handler(CommandHandler("help", help_command))
        
        # Run the bot
        logger.info("✅ Bot starting polling...")
        await application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")
        raise e

# Main function
async def main():
    """Main async function"""
    logger.info("🚀 Starting MovieFinder Bot...")
    await run_bot()

if __name__ == "__main__":
    # Run the bot
    asyncio.run(main())
