import os
import asyncio
import logging
import json
import re
import aiohttp
import asyncpg
import redis
import telegram
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler, CallbackQueryHandler
)
from datetime import datetime
import google.generativeai as genai
import hashlib
import time

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

# Configuration
class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "your_bot_token_here")
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
    DATABASE_URL = os.environ.get('DATABASE_URL', '')
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
    ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
    ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID', '')
    REQUEST_LIMIT = int(os.environ.get('REQUEST_LIMIT', 20))
    REQUEST_WINDOW = int(os.environ.get('REQUEST_WINDOW', 3600))

# Conversation states
class States:
    MAIN_MENU, SEARCHING, REQUESTING, FEEDBACK = range(4)

# Simple in-memory storage for fallback when database is unavailable
class MemoryStorage:
    def __init__(self):
        self.user_requests = []
        self.user_stats = {}
        self.movies = []
        self.rate_limits = {}
    
    def add_request(self, user_id, username, first_name, movie_title):
        self.user_requests.append({
            'user_id': user_id,
            'username': username,
            'first_name': first_name,
            'movie_title': movie_title,
            'requested_at': datetime.now()
        })
    
    def get_user_stats(self, user_id):
        return self.user_stats.get(user_id, {'search_count': 0, 'request_count': 0})
    
    def increment_stat(self, user_id, stat_type):
        if user_id not in self.user_stats:
            self.user_stats[user_id] = {'search_count': 0, 'request_count': 0}
        self.user_stats[user_id][f'{stat_type}_count'] += 1

memory_storage = MemoryStorage()

# Database manager with fallback to memory storage
class DatabaseManager:
    _pool = None
    _use_memory_fallback = False
    
    @classmethod
    async def initialize(cls):
        """Initialize database connection with fallback to memory storage"""
        if not Config.DATABASE_URL:
            logger.warning("No DATABASE_URL provided, using memory storage")
            cls._use_memory_fallback = True
            return True
        
        try:
            # Parse and modify DATABASE_URL for Supabase
            db_url = Config.DATABASE_URL
            if 'supabase' in db_url and 'sslmode' not in db_url:
                if '?' in db_url:
                    db_url += '&sslmode=require'
                else:
                    db_url += '?sslmode=require'
            
            cls._pool = await asyncpg.create_pool(
                db_url,
                min_size=1,
                max_size=10,
                command_timeout=30
            )
            
            # Test connection
            async with cls._pool.acquire() as conn:
                await conn.execute('SELECT 1')
            
            await cls._create_tables()
            logger.info("✅ Database connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            logger.warning("🔄 Falling back to memory storage")
            cls._use_memory_fallback = True
            return False
    
    @classmethod
    async def _create_tables(cls):
        """Create necessary tables"""
        if cls._use_memory_fallback:
            return
        
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    file_id TEXT,
                    quality TEXT,
                    size TEXT,
                    language TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_requests (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    movie_title TEXT NOT NULL,
                    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    search_count INTEGER DEFAULT 0,
                    request_count INTEGER DEFAULT 0,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
    
    @classmethod
    async def store_user_request(cls, user_id, username, first_name, movie_title):
        """Store user movie request"""
        if cls._use_memory_fallback:
            memory_storage.add_request(user_id, username, first_name, movie_title)
            return True
        
        try:
            async with cls._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO user_requests (user_id, username, first_name, movie_title)
                    VALUES ($1, $2, $3, $4)
                ''', user_id, username, first_name, movie_title)
            return True
        except Exception as e:
            logger.error(f"Error storing user request: {e}")
            # Fallback to memory storage
            memory_storage.add_request(user_id, username, first_name, movie_title)
            return True
    
    @classmethod
    async def track_activity(cls, user_id, username, first_name, action_type):
        """Track user activity"""
        if cls._use_memory_fallback:
            memory_storage.increment_stat(user_id, action_type)
            return
        
        try:
            async with cls._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO user_stats (user_id, username, first_name, last_active)
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id) DO UPDATE SET
                    last_active = CURRENT_TIMESTAMP,
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name
                ''', user_id, username, first_name)
                
                if action_type == 'search':
                    await conn.execute('''
                        UPDATE user_stats SET search_count = search_count + 1 
                        WHERE user_id = $1
                    ''', user_id)
                elif action_type == 'request':
                    await conn.execute('''
                        UPDATE user_stats SET request_count = request_count + 1 
                        WHERE user_id = $1
                    ''', user_id)
        except Exception as e:
            logger.error(f"Error tracking activity: {e}")
            memory_storage.increment_stat(user_id, action_type)
    
    @classmethod
    async def get_user_stats(cls, user_id):
        """Get user statistics"""
        if cls._use_memory_fallback:
            return memory_storage.get_user_stats(user_id)
        
        try:
            async with cls._pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = $1',
                    user_id
                )
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            return memory_storage.get_user_stats(user_id)
    
    @classmethod
    async def search_movies(cls, title, max_results=5):
        """Search movies in database"""
        if cls._use_memory_fallback:
            # Return some sample data for demo
            return [{
                'title': 'Sample Movie',
                'url': 'https://example.com/movie',
                'file_id': None,
                'quality': '1080p',
                'size': '1.5GB',
                'source': 'memory'
            }]
        
        try:
            async with cls._pool.acquire() as conn:
                rows = await conn.fetch(
                    'SELECT title, url, file_id, quality, size FROM movies WHERE title ILIKE $1 LIMIT $2',
                    f'%{title}%', max_results
                )
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error searching movies: {e}")
            return []

# Rate limiting with memory fallback
class RateLimiter:
    @staticmethod
    async def check_limit(user_id):
        """Check if user has exceeded rate limit"""
        try:
            # Simple in-memory rate limiting
            current_time = time.time()
            key = f"rate_limit:{user_id}"
            
            if key not in memory_storage.rate_limits:
                memory_storage.rate_limits[key] = {
                    'count': 1,
                    'window_start': current_time
                }
                return True
            
            limit_data = memory_storage.rate_limits[key]
            
            # Reset window if expired
            if current_time - limit_data['window_start'] > Config.REQUEST_WINDOW:
                limit_data['count'] = 1
                limit_data['window_start'] = current_time
                return True
            
            # Check limit
            if limit_data['count'] >= Config.REQUEST_LIMIT:
                return False
            
            limit_data['count'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            return True  # Allow request if rate limiting fails

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
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user = update.effective_user
            await DatabaseManager.track_activity(user.id, user.username, user.first_name, 'start')
            
            welcome_text = """
🎬 Welcome to MovieFinder Bot! 🎬

I can help you find and request movies and series.

Features:
• Search our movie database
• Request movies we don't have yet
• Get notified when requested movies become available

Use the buttons below to get started!
            """
            
            await update.message.reply_text(welcome_text, reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
        except Exception as e:
            logger.error(f"Error in start command: {e}")
            await update.message.reply_text("Welcome! Use the buttons to get started.", 
                                          reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
    
    async def search_movies(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user_message = update.message.text.strip()
            user = update.effective_user
            
            # Apply rate limiting
            if not await RateLimiter.check_limit(user.id):
                await update.message.reply_text("🚫 You've reached your hourly request limit. Please try again later.")
                return States.MAIN_MENU
            
            await DatabaseManager.track_activity(user.id, user.username, user.first_name, 'search')
            
            if len(user_message) < 3:
                await update.message.reply_text("Please enter at least 3 characters to search.")
                return States.SEARCHING
            
            # Show typing indicator
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
            
            results = await DatabaseManager.search_movies(user_message)
            
            if results and len(results) > 0:
                # Send first result
                first_result = results[0]
                response = f"🎉 Found {len(results)} results!\n\nTitle: {first_result['title']}"
                
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
            else:
                # No results found, offer to request the movie
                await DatabaseManager.store_user_request(
                    user.id, user.username, user.first_name, user_message
                )
                
                response = f"😔 Sorry, '{user_message}' is not in our collection yet. We've noted your request!"
                await update.message.reply_text(response)
            
            await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
            
        except Exception as e:
            logger.error(f"Error in search movies: {e}")
            await update.message.reply_text("Sorry, something went wrong. Please try again.", 
                                          reply_markup=Keyboards.main_menu())
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
            stats = await DatabaseManager.get_user_stats(user.id)
            if stats:
                response = f"📊 Your Stats:\n\n🔍 Searches: {stats.get('search_count', 0)}\n🙋 Requests: {stats.get('request_count', 0)}"
                if 'last_active' in stats and stats['last_active']:
                    response += f"\n🕐 Last Active: {stats['last_active'].strftime('%Y-%m-%d %H:%M')}"
            else:
                response = "📊 Your Stats:\n\nNo activity recorded yet."
            await update.message.reply_text(response)
        except Exception as e:
            logger.error(f"Error getting user stats: {e}")
            await update.message.reply_text("📊 Your Stats:\n\nNo activity recorded yet.")
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
    success = await DatabaseManager.store_user_request(user.id, user.username, user.first_name, movie_title)
    
    if success:
        await update.message.reply_text(f"✅ Your request for '{movie_title}' has been recorded! We'll notify you when it's available.")
    else:
        await update.message.reply_text("❌ Sorry, there was an error processing your request. Please try again.")
    
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
        await query.edit_message_text(f"✅ Thank you for your {rating} star rating!")
    
    elif data.startswith('request_'):
        movie_title = data.replace('request_', '')
        user = update.effective_user
        
        success = await DatabaseManager.store_user_request(user.id, user.username, user.first_name, movie_title)
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
        stats = await DatabaseManager.get_user_stats(user.id)
        if stats:
            response = f"📊 Your Stats:\n\n🔍 Searches: {stats.get('search_count', 0)}\n🙋 Requests: {stats.get('request_count', 0)}"
        else:
            response = "📊 Your Stats:\n\nNo activity recorded yet."
        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
        await update.message.reply_text("❌ Error retrieving your stats.")

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
/help - Show this help message

Need help? Contact support.
    """
    await update.message.reply_text(help_text)

# Main bot runner
async def run_bot():
    """Run the Telegram bot"""
    try:
        logger.info("🔄 Starting bot initialization...")
        
        # Initialize database with fallback
        await DatabaseManager.initialize()
        
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
            },
            fallbacks=[CommandHandler('cancel', cancel)],
        )
        
        application.add_handler(conv_handler)
        application.add_handler(CallbackQueryHandler(button_callback))
        
        # Add command handlers
        application.add_handler(CommandHandler("stats", user_stats))
        application.add_handler(CommandHandler("help", help_command))
        
        # Run the bot
        logger.info("✅ Bot starting polling...")
        await application.run_polling(drop_pending_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")
        raise e

# Main function
def main():
    """Main function"""
    logger.info("🚀 Starting MovieFinder Bot...")
    
    # Run the bot
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
