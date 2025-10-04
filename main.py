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
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify
import google.generativeai as genai
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import redis
import hashlib
import time
from urllib.parse import urlparse

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

# Configuration with defaults for Render
class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
    DATABASE_URL = os.environ.get('DATABASE_URL')

# Support both direct and pooler URLs
DATABASE_POOLER_URL = os.environ.get('DATABASE_POOLER_URL', DATABASE_URL)
USE_POOLER = os.environ.get('USE_POOLER', 'true').lower() == 'true'

REDIS_URL = os.environ.get('REDIS_URL', '')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET = os.environ.get('UPDATE_SECRET', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')
PORT = int(os.environ.get('PORT', 5000))
REQUEST_LIMIT = int(os.environ.get('REQUEST_LIMIT', 20))
REQUEST_WINDOW = int(os.environ.get('REQUEST_WINDOW', 3600))

def get_database_config():
    """
Get the appropriate database configuration
Automatically detects and uses pooler URL if on Render
"""
db_url = Config.DATABASE_POOLER_URL if Config.USE_POOLER else Config.DATABASE_URL

if not db_url:
logger.error("No database URL configured!")
return None

# Parse the database URL to check if it's a pooler URL
parsed = urlparse(db_url)

# Detect if this is a Supabase pooler URL
if 'pooler.supabase.com' in parsed.hostname:
logger.info(f"Using Supabase pooler connection on port {parsed.port}")

# For transaction mode (port 6543), disable prepared statements
if parsed.port == 6543:
    # Add options for transaction mode if not already present
    if '?' not in db_url:
        db_url += '?sslmode=require'
    elif 'sslmode' not in db_url:
        db_url += '&sslmode=require'
        
    # Set connection parameters for better pooler compatibility
    return {
        'dsn': db_url,
        'options': '-c statement_timeout=30s -c idle_in_transaction_session_timeout=30s'
    }

# For direct connections or session mode
return {'dsn': db_url}

# Conversation states
class States:
MAIN_MENU, SEARCHING, REQUESTING, FEEDBACK = range(4)

# Initialize Flask app
app = Flask(__name__)

# Redis connection for caching and rate limiting
redis_conn = None
if Config.REDIS_URL:
try:
# Only connect to Redis if URL is provided and not empty
if Config.REDIS_URL and Config.REDIS_URL != 'redis://localhost:6379':
    redis_conn = redis.from_url(Config.REDIS_URL, decode_responses=True)
    redis_conn.ping()
    logger.info("Redis connected successfully")
else:
    logger.info("Redis URL not configured, proceeding without cache")
except Exception as e:
logger.warning(f"Redis connection failed: {e}, proceeding without cache")
redis_conn = None

# Database connection pool with improved error handling
class Database:
_connection_pool = None
_pool_lock = threading.Lock()

@classmethod
def initialize_pool(cls, retry_count=3):
"""Initialize the connection pool with retry logic"""
with cls._pool_lock:
    if cls._connection_pool is not None:
        return True
        
    db_config = get_database_config()
    if not db_config:
        logger.error("Database configuration not available")
        return False
    
    for attempt in range(retry_count):
        try:
            logger.info(f"Attempting to connect to database (attempt {attempt + 1}/{retry_count})")
            
            # Create connection pool with appropriate settings
            if 'options' in db_config:
                cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 10,
                    db_config['dsn'],
                    options=db_config.get('options', '')
                )
            else:
                cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 10,
                    db_config['dsn']
                )
            
            # Test the connection
            test_conn = cls._connection_pool.getconn()
            test_conn.close()
            cls._connection_pool.putconn(test_conn)
            
            logger.info("Database connection pool initialized successfully")
            return True
            
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            if 'Network is unreachable' in error_msg or 'IPv6' in error_msg:
                logger.error(f"IPv6 connection failed. Please use the Supabase pooler URL (IPv4) from your dashboard.")
                logger.error(f"Set DATABASE_POOLER_URL environment variable with the pooler connection string")
            else:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
            
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                
        except Exception as e:
            logger.error(f"Unexpected error during pool initialization: {e}")
            if attempt < retry_count - 1:
                time.sleep(2 ** attempt)
    
    return False

@classmethod
def get_connection(cls):
"""Get connection from pool"""
if cls._connection_pool is None:
    if not cls.initialize_pool():
        raise Exception("Database connection pool not available")
try:
    return cls._connection_pool.getconn()
except Exception as e:
    logger.error(f"Failed to get connection from pool: {e}")
    # Try to reinitialize the pool
    cls._connection_pool = None
    if cls.initialize_pool():
        return cls._connection_pool.getconn()
    raise

@classmethod
def return_connection(cls, conn):
"""Return connection to pool"""
if cls._connection_pool and conn:
    try:
        # Reset the connection before returning to pool
        conn.rollback()
        cls._connection_pool.putconn(conn)
    except Exception as e:
        logger.error(f"Error returning connection to pool: {e}")
        try:
            conn.close()
        except:
            pass

@classmethod
def execute_query(cls, query, params=None, fetch=False):
"""Execute database query with proper error handling"""
conn = None
retry_count = 2

for attempt in range(retry_count):
    try:
        conn = cls.get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                result = cur.fetchall()
                conn.commit()
                return result
            conn.commit()
            return True
            
    except psycopg2.OperationalError as e:
        if conn:
            conn.rollback()
        logger.error(f"Operational error on attempt {attempt + 1}: {e}")
        if attempt < retry_count - 1:
            time.sleep(1)
            # Reset pool on operational errors
            cls._connection_pool = None
        else:
            raise
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database query error: {e}")
        raise
        
    finally:
        if conn:
            cls.return_connection(conn)

# Database setup with retry logic
def setup_database(retries=3, delay=2):
"""Setup database tables with proper error handling"""

for attempt in range(retries):
try:
    if not Database.initialize_pool():
        if attempt < retries - 1:
            logger.warning(f"Database pool initialization failed, retrying in {delay} seconds...")
            time.sleep(delay)
            continue
        else:
            logger.error("Failed to initialize database pool after all attempts")
            return False
    
    # Create tables with improved schema
    queries = [
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
            priority INTEGER DEFAULT 1,
            UNIQUE(user_id, movie_title)
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
    
    for query in queries:
        try:
            Database.execute_query(query)
        except Exception as e:
            # Table might already exist, which is fine
            if "already exists" not in str(e).lower():
                logger.error(f"Error creating table: {e}")
                raise
    
    logger.info("Database setup completed successfully")
    return True
    
except Exception as e:
    logger.error(f"Database setup attempt {attempt + 1} failed: {e}")
    if attempt < retries - 1:
        time.sleep(delay * (attempt + 1))  # Exponential backoff
    else:
        logger.error("All database setup attempts failed")
        return False

# Store user request function
def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
"""Store user movie request in database"""
try:
Database.execute_query('''
    INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
    VALUES (%s, %s, %s, %s, %s, %s)
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
    db_results = Database.execute_query(
        "SELECT title, url, file_id, quality, size FROM movies WHERE title ILIKE %s OR id IN (SELECT movie_id FROM movie_aliases WHERE alias ILIKE %s) LIMIT %s",
        (f'%{title}%', f'%{title}%', max_results),
        fetch=True
    )
    
    if db_results:
        for result in db_results:
            results.append({
'title': result,
'url': result,
'file_id': result,
'quality': result,
'size': result,
'source': 'database'
})

except Exception as e:
    logger.error(f"Database search error: {e}")

# If not enough results, try external sources
if len(results) < max_results:
    external_results = await MovieSearch._search_external_sources(title, max_results - len(results))
    results.extend(external_results)
    
return results

@staticmethod
async def _search_external_sources(title, max_results):
results = []
# Implement searches from external APIs here
return results

# AI-powered intent recognition with fallback
class AIAssistant:
def __init__(self):
self.model = None
if Config.GEMINI_API_KEY:
    try:
        genai.configure(api_key=Config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(model_name='gemini-pro')
        logger.info("AI model initialized successfully")
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
def track_activity(user_id, username, first_name, action_type):
try:
    # Update user stats
    Database.execute_query('''
        INSERT INTO user_stats (user_id, username, first_name, last_active)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id) DO UPDATE SET
        last_active = CURRENT_TIMESTAMP,
        username = EXCLUDED.username,
        first_name = EXCLUDED.first_name
    ''', (user_id, username, first_name))
    
    # Increment specific counter
    if action_type == 'search':
        Database.execute_query('''
            UPDATE user_stats SET search_count = search_count + 1 
            WHERE user_id = %s
        ''', (user_id,))
    elif action_type == 'request':
        Database.execute_query('''
            UPDATE user_stats SET request_count = request_count + 1 
            WHERE user_id = %s
        ''', (user_id,))
        
except Exception as e:
    logger.error(f"Error tracking user activity: {e}")

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
    users = Database.execute_query(
        "SELECT user_id, group_id FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
        (f'%{movie_title}%',),
        fetch=True
    )
    
    notified_count = 0
    for user_id, group_id in users:
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
            Database.execute_query(
                "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
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
    UserManager.track_activity(user.id, user.username, user.first_name, 'start')
    
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
    await update.message.reply_text("Welcome! Please use the menu below.", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU

async def search_movies(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
try:
    user_message = update.message.text.strip()
    user = update.effective_user
    
    # Apply rate limiting if Redis is available
    if redis_conn:
        try:
            key = f"rate_limit:{user.id}"
            current = redis_conn.get(key)
            if current and int(current) >= Config.REQUEST_LIMIT:
                await update.message.reply_text("🚫 You've reached your hourly request limit. Please try again later.")
                return States.MAIN_MENU
            
            if current:
                redis_conn.incr(key)
            else:
                redis_conn.setex(key, Config.REQUEST_WINDOW, 1)
        except Exception as e:
            logger.error(f"Redis rate limiting error: {e}")
    
    try:
        UserManager.track_activity(user.id, user.username, user.first_name, 'search')
    except Exception as e:
        logger.error(f"Error tracking activity: {e}")
    
    if len(user_message) < 3:
        await update.message.reply_text("Please enter at least 3 characters to search.")
        return States.SEARCHING
    
    # Show typing indicator
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
    
    results = await MovieSearch.search_movie(user_message)
    
    if results:
        # Send first result immediately
        first_result = results
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
        try:
            store_user_request(
                user.id, user.username, user.first_name, user_message,
                update.effective_chat.id if update.effective_chat.type != "private" else None,
                update.message.message_id
            )
        except Exception as e:
            logger.error(f"Error storing request: {e}")
        
        response = f"😔 Sorry, '{user_message}' is not in our collection yet. Would you like to request it?"
        keyboard = [[InlineKeyboardButton("✅ Yes, Request It", callback_data=f"request_{user_message[:50]}")]]
        await update.message.reply_text(
            response, 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU
    
except Exception as e:
    logger.error(f"Error in search movies: {e}")
    await update.message.reply_text("Sorry, something went wrong. Please try again.", reply_markup=Keyboards.main_menu())
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
    stats = Database.execute_query(
        "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = %s",
        (user.id,),
        fetch=True
    )
    if stats:
        search_count, request_count, last_active = stats
        response = f"📊 Your Stats:\n\n🔍 Searches: {search_count}\n🙋 Requests: {request_count}\n🕐 Last Active: {last_active.strftime('%Y-%m-%d %H:%M')}"
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
try:
success = store_user_request(user.id, user.username, user.first_name, movie_title)

if success:
    # Notify admin
    try:
        await NotificationSystem.notify_admin(context, user, movie_title)
    except Exception as e:
        logger.error(f"Error notifying admin: {e}")
    
    await update.message.reply_text(f"✅ Your request for '{movie_title}' has been recorded! We'll notify you when it's available.")
else:
    await update.message.reply_text("❌ Sorry, there was an error processing your request. Please try again.")
except Exception as e:
logger.error(f"Error in request_movie: {e}")
await update.message.reply_text("❌ Sorry, there was an error processing your request. Please try again.")

await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
return States.MAIN_MENU

async def process_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
"""Process user feedback"""
feedback_text = update.message.text.strip()
user = update.effective_user

try:
Database.execute_query(
    "INSERT INTO feedback (user_id, message) VALUES (%s, %s)",
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

if data:
rating = int(data.split('_'))
user = update.effective_user

try:
    Database.execute_query(
        "INSERT INTO feedback (user_id, message, rating) VALUES (%s, %s, %s)",
        (user.id, f"User rating: {rating} stars", rating)
    )
    await query.edit_message_text(f"✅ Thank you for your {rating} star rating!")
except Exception as e:
    logger.error(f"Error storing rating: {e}")
    await query.edit_message_text("❌ Error saving your rating.")

elif data.startswith('request_'):
movie_title = data.replace('request_', '')
user = update.effective_user

try:
    success = store_user_request(user.id, user.username, user.first_name, movie_title)
    if success:
        await query.edit_message_text(f"✅ Your request for '{movie_title}' has been recorded!")
    else:
        await query.edit_message_text("❌ Error processing your request.")
except Exception as e:
    logger.error(f"Error in button callback: {e}")
    await query.edit_message_text("❌ Error processing your request.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
"""Cancel conversation"""
await update.message.reply_text("Operation cancelled.", reply_markup=Keyboards.main_menu())
return States.MAIN_MENU

async def user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
"""Show user statistics"""
user = update.effective_user
try:
stats = Database.execute_query(
    "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = %s",
    (user.id,),
    fetch=True
)
if stats:
    search_count, request_count, last_active = stats
    response = f"📊 Your Stats:\n\n🔍 Searches: {search_count}\n🙋 Requests: {request_count}\n🕐 Last Active: {last_active.strftime('%Y-%m-%d %H:%M')}"
else:
    response = "📊 Your Stats:\n\nNo activity recorded yet."
await update.message.reply_text(response)
except Exception as e:
logger.error(f"Error getting user stats: {e}")
await update.message.reply_text("❌ Error retrieving your stats.")

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

# Run bot function
def run_bot():
"""Run the Telegram bot synchronously"""
try:
logger.info("Starting bot...")

if not Config.TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not configured!")
    return

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
application.add_handler(CommandHandler("feedback", feedback))
application.add_handler(CommandHandler("help", help_command))

# Run the bot
logger.info("Bot starting polling...")
application.run_polling(drop_pending_updates=True)

except Exception as e:
logger.error(f"Bot failed to start: {e}")

# Flask routes for web interface
@app.route('/')
def home():
return jsonify({
"status": "online",
"service": "MovieFinder Bot",
"version": "2.0",
"timestamp": datetime.now().isoformat()
})

@app.route('/health')
def health():
"""Health check endpoint for Render"""
return jsonify({"status": "healthy"}), 200

@app.route('/stats')
def stats():
try:
# Check database connectivity first
if not Database._connection_pool:
    Database.initialize_pool()

users = Database.execute_query(
    "SELECT COUNT(*) FROM user_stats",
    fetch=True
)
users = users if users else 0

movies = Database.execute_query(
    "SELECT COUNT(*) FROM movies",
    fetch=True
)
movies = movies if movies else 0

requests = Database.execute_query(
    "SELECT COUNT(*) FROM user_requests WHERE notified = FALSE",
    fetch=True
)
requests = requests if requests else 0

return jsonify({
    "users": users,
    "movies": movies,
    "pending_requests": requests
})
except Exception as e:
logger.error(f"Stats endpoint error: {e}")
return jsonify({"error": "Database connection error"}), 503

@app.route('/admin/update', methods=['POST'])
def admin_update():
if request.headers.get('Authorization') != f"Bearer {Config.UPDATE_SECRET}":
return jsonify({"error": "Unauthorized"}), 401

try:
return jsonify({"status": "success", "message": "Update endpoint ready"})
except Exception as e:
return jsonify({"error": str(e)}), 500

# Main entry point
def main():
"""Main function to run the bot"""
logger.info("Starting MovieFinder Bot v2.0")

# Check environment variables
if not Config.TELEGRAM_BOT_TOKEN:
logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
return

if not Config.DATABASE_URL and not Config.DATABASE_POOLER_URL:
logger.error("No database URL configured! Set either DATABASE_URL or DATABASE_POOLER_URL")
return

# Log configuration info
if Config.USE_POOLER and Config.DATABASE_POOLER_URL:
logger.info("Using Supabase pooler connection (IPv4 compatible)")
else:
logger.info("Using direct database connection")

# Setup database with retries
logger.info("Initializing database...")
if not setup_database(retries=5, delay=3):
logger.warning("Database setup failed, but bot will continue to run")
# Don't exit - bot can still function for basic operations

# Run the bot
run_bot()

if __name__ == "__main__":
main()
