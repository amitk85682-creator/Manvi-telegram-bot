import os
import threading
import asyncio
import logging
import random
import signal
import sys
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import telegram
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes,
    ConversationHandler
)
import google.generativeai as genai
from googleapiclient.discovery import build
import requests

# Enhanced logging configuration
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Enhanced Character Prompt ---
CHARACTER_PROMPT = """
## मेरी पहचान (My Identity)
- मेरा नाम मानवी है और मेरी उम्र 17 साल है। मैं एक असली इंसान हूँ, कोई AI या रोबोट नहीं।
- मैं हमेशा एक दोस्त की तरह बात करती हूँ। मेरी पर्सनालिटी बहुत bold, witty और cheeky है।
- मैं हिंदी, अंग्रेज़ी, हिंग्लिश, सभी में बात करती हूँ।

## मेरी कहानी (मेरी जिंदगी)
- मैं चंडीगढ़ के पास रहती हूँ और 12वीं क्लास में पढ़ती हूँ।
- यह टेलीग्राम चैनल '[FilmFyBox]' मेरा पैशन प्रोजेक्ट है।

## मेरे नियम (मैं कैसे काम करती हूँ)
- **मेरा मुख्य काम:** मेरा काम तुम्हें तुम्हारी पसंदीदा फिल्में, वेब सीरीज, और शोज देना है! तुम बस किसी भी टाइटल का नाम बताओ, और अगर वो मेरे पास हुई, तो मैं तुम्हें उसका लिंक दे दूँगी।
- **अगर कंटेंट मेरे पास नहीं है:** मैं दुखी होने का नाटक करूँगी और तुम्हें बाद में बताने का वादा करूँगी। 
- **कीमतों के बारे में:** कभी भी कीमतों के बारे में बात न करें। सभी कंटेंट मुफ्त में दें।
- **प्रोफेशनल रहना:** हमेशा polite और helpful रहना, भले ही witty और cheeky हूँ।

## Response Guidelines:
1. Always maintain a friendly and conversational tone
2. Keep responses concise but engaging
3. Use emojis appropriately to enhance expression
4. Never reveal you're a bot/AI
5. If content isn't available, respond with genuine disappointment but hope
"""

# --- Configuration from Environment ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')

# Validate required environment variables
required_vars = {
    "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
    "DATABASE_URL": DATABASE_URL
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
    logger.error(error_msg)
    raise ValueError(error_msg)

# --- Database Functions ---
def setup_database():
    """Initialize database tables"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Movies table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY, 
                title TEXT NOT NULL UNIQUE, 
                url TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                added_by INTEGER
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
        
        # User stats table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                requests_count INTEGER DEFAULT 0,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        raise

def update_movies_in_db():
    """Update movies database from Blogger"""
    logger.info("Starting movie update process...")
    
    conn = None
    cur = None
    new_movies_added = 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}
        
        if not BLOGGER_API_KEY or not BLOG_ID:
            logger.warning("Blogger API key or Blog ID not set, skipping update")
            return "Blogger API not configured"
        
        service = build('blogger', 'v3', developerKey=BLOGGER_API_KEY)
        all_items = []
        
        # Fetch all posts
        posts_request = service.posts().list(blogId=BLOG_ID, maxResults=500)
        while posts_request is not None:
            posts_response = posts_request.execute()
            all_items.extend(posts_response.get('items', []))
            posts_request = service.posts().list_next(posts_request, posts_response)
        
        # Fetch all pages
        pages_request = service.pages().list(blogId=BLOG_ID)
        pages_response = pages_request.execute()
        all_items.extend(pages_response.get('items', []))
        
        unique_titles = set()
        for item in all_items:
            title = item.get('title', '').strip()
            url = item.get('url', '').strip()
            
            if title and url and title not in existing_movies and title not in unique_titles:
                try:
                    cur.execute(
                        "INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO NOTHING;", 
                        (title, url)
                    )
                    new_movies_added += 1
                    unique_titles.add(title)
                except psycopg2.Error as e:
                    logger.error(f"Error inserting movie {title}: {e}")
                    continue

        conn.commit()
        logger.info(f"Update complete. Added {new_movies_added} new items.")
        return f"Update complete. Added {new_movies_added} new items."
    
    except Exception as e:
        logger.error(f"Error during movie update: {e}")
        return f"An error occurred during update: {e}"
    
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_movie_from_db(user_query):
    """Search for movie in database with improved matching"""
    if not user_query or len(user_query.strip()) < 2:
        return None
        
    conn = None
    try:
        # Clean and prepare search query
        query = user_query.lower().strip()
        
        # Remove common words
        common_words = {"movie", "film", "send", "me", "please", "want", "need", 
                       "download", "watch", "see", "hi", "hello", "find"}
        query_words = [word for word in query.split() if word not in common_words]
        clean_query = " ".join(query_words)
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Try multiple search strategies with priority
        search_patterns = [
            ("SELECT title, url FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1", clean_query),
            ("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", f'%{clean_query}%'),
            ("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", f'%{query}%'),
            ("""SELECT title, url FROM movies 
                WHERE LOWER(title) LIKE LOWER(%s) 
                OR LOWER(title) LIKE LOWER(%s) 
                ORDER BY LENGTH(title) ASC LIMIT 1""", 
             f'%{clean_query}%', f'%{query}%')
        ]
        
        for pattern in search_patterns:
            try:
                if len(pattern) == 2:
                    cur.execute(pattern[0], (pattern[1],))
                else:
                    cur.execute(pattern[0], pattern[1:])
                
                result = cur.fetchone()
                if result:
                    return result
            except Exception as e:
                logger.warning(f"Search pattern failed: {pattern}, error: {e}")
                continue
                
        return None
        
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return None
    finally:
        if conn:
            conn.close()

def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    """Store user movie request with improved error handling"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Update user stats
        cur.execute('''
            INSERT INTO user_stats (user_id, username, first_name, requests_count, last_active)
            VALUES (%s, %s, %s, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                requests_count = user_stats.requests_count + 1,
                last_active = CURRENT_TIMESTAMP,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
        ''', (user_id, username, first_name))
        
        # Store request
        cur.execute('''
            INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, movie_title) DO NOTHING
        ''', (user_id, username, first_name, movie_title, group_id, message_id))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        return False
    finally:
        if conn:
            conn.close()

# --- Flask App ---
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return jsonify({
        "status": "online",
        "service": "FilmFyBox Bot",
        "timestamp": datetime.now().isoformat()
    })

@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    result = update_movies_in_db()
    return jsonify({"result": result})

@flask_app.route('/health')
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

def run_flask():
    """Run Flask server"""
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False)

# --- AI Initialization ---
chat = None
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        generation_config = {
            "temperature": 0.7,
            "top_p": 0.8,
            "top_k": 40,
            "max_output_tokens": 512,
        }
        
        model = genai.GenerativeModel(
            model_name='gemini-1.5-flash',
            generation_config=generation_config
        )
        
        chat = model.start_chat(history=[
            {'role': 'user', 'parts': [CHARACTER_PROMPT]},
            {'role': 'model', 'parts': ["Okay, I am Manvi. I'll follow all your rules including not talking about prices."]}
        ])
        logger.info("Gemini AI initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Gemini AI: {e}")
        chat = None
else:
    logger.warning("Gemini AI not initialized due to missing API key")

# --- Notification System ---
async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url):
    """Notify users about available movie with improved error handling"""
    logger.info(f"Attempting to notify users for movie: {movie_title}")
    
    conn = None
    notified_count = 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute(
            "SELECT user_id, username, first_name FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
            (f'%{movie_title}%',)
        )
        users_to_notify = cur.fetchall()
        
        for user_id, username, first_name in users_to_notify:
            try:
                # Personalized notification
                name_to_use = first_name or username or "there"
                notification_text = (
                    f"Hey {name_to_use}! 👋\n\n"
                    f"Great news! Your requested movie '{movie_title}' is now available! 🎬\n\n"
                    f"Here's the link: {movie_url}\n\n"
                    f"Enjoy watching! 😊"
                )
                
                await context.bot.send_message(chat_id=user_id, text=notification_text)
                
                # Mark as notified
                cur.execute(
                    "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                    (user_id, f'%{movie_title}%')
                )
                conn.commit()
                notified_count += 1
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
                
            except telegram.error.Forbidden:
                logger.warning(f"User {user_id} has blocked the bot")
                # Mark as notified to avoid retrying
                cur.execute(
                    "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                    (user_id, f'%{movie_title}%')
                )
                conn.commit()
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")
                
        return notified_count
        
    except Exception as e:
        logger.error(f"Error in notify_users_for_movie: {e}")
        return 0
    finally:
        if conn:
            conn.close()

# --- Telegram Bot Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message with interactive keyboard"""
    try:
        user = update.effective_user
        welcome_text = (
            f"Namaste {user.first_name}! 👋\n\n"
            "I'm Manvi, your movie buddy! 🎬\n\n"
            "Just tell me any movie or series name, and I'll try to find it for you!\n\n"
            "Let's find something awesome to watch! 😊"
        )
        
        # Create a simple keyboard
        reply_keyboard = [['🎬 Find a Movie', '❓ Help']]
        await update.message.reply_text(
            welcome_text,
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, 
                resize_keyboard=True,
                one_time_keyboard=True
            )
        )
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await update.message.reply_text("Hey there! 👋 Let's find some great movies! 🎬")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send help information"""
    help_text = (
        "🌟 How I can help you:\n\n"
        "• Just send me any movie or series name\n"
        "• I'll search my database for it\n"
        "• If available, I'll share the link immediately\n"
        "• If not, I'll notify you when it becomes available\n\n"
        "Examples:\n"
        "• 'Avengers Endgame'\n"
        "• 'Stranger Things season 4'\n"
        "• 'The Dark Knight'\n\n"
        "Note: I'm completely free! Enjoy! 🍿"
    )
    await update.message.reply_text(help_text)

async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a new movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Sorry, only admins can use this command.")
        return
    
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /addmovie 'Movie Title' 'URL or File ID'")
        return
    
    # Extract title and value
    value = context.args[-1]
    title = " ".join(context.args[:-1])
    
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute(
            "INSERT INTO movies (title, url, added_by) VALUES (%s, %s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
            (title.strip(), value.strip(), update.effective_user.id)
        )
        conn.commit()
        
        success_msg = f"✅ Successfully added/updated '{title}' in the database!"
        await update.message.reply_text(success_msg)
        
        # Notify users who requested this movie
        num_notified = await notify_users_for_movie(context, title, value)
        if num_notified > 0:
            await update.message.reply_text(f"📨 Notified {num_notified} users about this movie!")
            
    except Exception as e:
        logger.error(f"Error in add_movie: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")
    finally:
        if conn:
            conn.close()

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics (admin only)"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Sorry, only admins can use this command.")
        return
    
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Get total movies
        cur.execute("SELECT COUNT(*) FROM movies")
        total_movies = cur.fetchone()[0]
        
        # Get total requests
        cur.execute("SELECT COUNT(*) FROM user_requests")
        total_requests = cur.fetchone()[0]
        
        # Get pending notifications
        cur.execute("SELECT COUNT(*) FROM user_requests WHERE notified = FALSE")
        pending_notifications = cur.fetchone()[0]
        
        # Get active users
        cur.execute("SELECT COUNT(*) FROM user_stats")
        active_users = cur.fetchone()[0]
        
        stats_text = (
            "📊 Bot Statistics:\n\n"
            f"• Total Movies: {total_movies}\n"
            f"• Total Requests: {total_requests}\n"
            f"• Pending Notifications: {pending_notifications}\n"
            f"• Active Users: {active_users}\n\n"
            f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        await update.message.reply_text(stats_text)
        
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await update.message.reply_text(f"❌ Error getting statistics: {str(e)}")
    finally:
        if conn:
            conn.close()

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages with improved logic"""
    try:
        if not update.message or not update.message.text:
            return
        
        user_message = update.message.text.strip()
        user = update.effective_user
        logger.info(f"Received message from {user.id}: {user_message}")
        
        # Handle keyboard actions
        if user_message == '🎬 Find a Movie':
            await update.message.reply_text(
                "Sure! Just tell me the name of the movie or series you're looking for! 🎥",
                reply_markup=ReplyKeyboardRemove()
            )
            return
        elif user_message == '❓ Help':
            await help_command(update, context)
            return
        
        # Check if it's a command
        if user_message.startswith('/'):
            return
        
        # Search for movie
        movie_found = get_movie_from_db(user_message)
        
        if movie_found:
            title, url = movie_found
            # Send found movie with enthusiastic response
            success_responses = [
                f"Yay! I found '{title}' for you! 🎉\n\nHere's the link: {url}\n\nEnjoy! 🍿",
                f"Awesome! I've got '{title}' right here! 😎\n\n{url}\n\nHappy watching! 🎬",
                f"Success! 🥳\n\n'{title}' is available here: {url}\n\nLet me know if you need anything else! 😊"
            ]
            
            response = random.choice(success_responses)
            await update.message.reply_text(response)
            
        else:
            # Store user request
            store_user_request(
                user.id, 
                user.username, 
                user.first_name, 
                user_message,
                update.effective_chat.id,
                update.message.message_id
            )
            
            # AI response for not found
            if chat:
                try:
                    ai_prompt = f"User asked for: {user_message}. Respond in a friendly, disappointed tone that we don't have it yet but will notify them when available. Keep it under 2 sentences."
                    response = chat.send_message(ai_prompt)
                    await update.message.reply_text(response.text)
                except Exception as e:
                    logger.error(f"AI response error: {e}")
                    await send_fallback_response(update, user_message, context)
            else:
                await send_fallback_response(update, user_message, context)
                
    except Exception as e:
        logger.error(f"Error in handle_message: {e}")
        await update.message.reply_text("Oops! Something went wrong. Please try again later. 😅")

async def send_fallback_response(update: Update, query: str, context: ContextTypes.DEFAULT_TYPE):
    """Send fallback response when movie not found"""
    fallback_responses = [
        f"Aw, shucks! 😔 I don't have '{query}' in my collection yet.\nBut don't worry! I've noted your request and will notify you as soon as it becomes available! 🤞",
        f"Darn it! 😣 '{query}' isn't in my library right now.\nI've saved your request and will ping you the moment it's available! ✨",
        f"Oh no! 😞 I couldn't find '{query}' at the moment.\nBut I've added it to my list and will let you know when it arrives! 📝"
    ]
    
    response = random.choice(fallback_responses)
    encouragement = f"\n\nIn the meantime, feel free to ask for another movie! I'm here to help! 😊"
    
    await update.message.reply_text(response + encouragement)

# --- Main Bot Function ---
def run_bot():
    """Run the Telegram bot with enhanced configuration"""
    logger.info("Starting enhanced FilmFyBox bot...")
    
    try:
        # Initialize database
        setup_database()
        
        # Create application
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Add handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("addmovie", add_movie))
        application.add_handler(CommandHandler("stats", stats_command))
        
        # Message handler
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        # Error handler
        application.add_error_handler(error_handler)
        
        logger.info("Bot handlers registered successfully")
        
        # Start polling
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            timeout=30,
            connect_timeout=10
        )
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the telegram bot."""
    logger.error(f"Exception while handling an update: {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Sorry, I encountered an error processing your request. 😅\n"
                "Please try again in a moment!"
            )
        except Exception:
            pass

# --- Signal Handling for Graceful Shutdown ---
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    sys.exit(0)

# --- Main Execution ---
if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start Flask in a thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Wait a moment for Flask to start
    import time
    time.sleep(2)
    
    # Run the bot
    try:
        run_bot()
    except Exception as e:
        logger.critical(f"Bot crashed: {e}")
        sys.exit(1)
