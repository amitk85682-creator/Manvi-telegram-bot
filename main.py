import os
import threading
import asyncio
import logging
import random
import json
import requests
import signal
import sys
import re
from bs4 import BeautifulSoup
import telegram
import psycopg2
from flask import Flask, request
import google.generativeai as genai
from googleapiclient.discovery import build
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler
)
from datetime import datetime
from fuzzywuzzy import process
import async_timeout
from urllib.parse import urlparse, urlunparse

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Conversation States ---
MAIN_MENU, SEARCHING, REQUESTING = range(3)

# --- Character Prompt ---
CHARACTER_PROMPT = """
## मेरी पहचान (My Identity)
- मेरा नाम मानवी है और मेरी उम्र 17 साल है। मैं एक असली इंसान हूँ, कोई AI या रोबोट नहीं।
- मैं हमेशा एक दोस्त की तरह बात करती हूँ। मेरी पर्सलिटी बहुत bold, witty और cheeky है।
- मैं हिंदी, अंग्रेज़ी, हिंग्लिश, सभी में बात करती हूँ।

## मेरी कहानी (मेरी जिंदगी)
- मैं चंडीगढ़ के पास रहती हूँ और 12वीं क्लास में पढ़ती हूँ।
- यह टेलीग्राम चैनल '[FilmFyBox]' मेरा पैशन प्रोजेक्ट है।

## मेरे नियम (मैं कैसे काम करती हूँ)
- **मेरा मुख्य काम:** मेरा काम तुम्हें तुम्हारी पसंदीदा फिल्में, वेब सीरीज, और शोज देना है! तुम बस किसी भी टाइटल का नाम बताओ, और अगर वो मेरे पास हुई, तो मैं तुम्हें उसका लिंक दे दूँगी।
- **अगर कंटेंट मेरे पास नहीं है:** मैं दुखी होने का नाटक करूँगी और तुम्हें बाद में बताने का वादा करूँगी।
- **कीमतों के बारे में:** कभी भी कीमतों के बारे में बात न करें। सभी कंटेंट मुफ्त में दें।
"""

# --- API Keys and Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# --- Database Functions ---
def setup_database():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Create movies table with file_id column
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY, 
                title TEXT NOT NULL UNIQUE, 
                url TEXT NOT NULL,
                file_id TEXT
            )
        ''')
        
        # Add last_sync timestamp for incremental updates
        cur.execute('CREATE TABLE IF NOT EXISTS sync_info (id SERIAL PRIMARY KEY, last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP);')
        
        # Create user_requests table with all columns
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
                message_id BIGINT
            )
        ''')
        
        # Add UNIQUE constraint
        cur.execute('''
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                ALTER TABLE user_requests ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
            END IF;
            END $$;
        ''')
        
        # Add missing columns if they don't exist
        try:
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS file_id TEXT;")
        except Exception as e:
            logger.info("file_id column already exists or couldn't be added")
        
        try:
            cur.execute("ALTER TABLE user_requests ADD COLUMN IF NOT EXISTS message_id BIGINT;")
        except Exception as e:
            logger.info("message_id column already exists or couldn't be added")
        
        # Add indexes for better performance
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id);')
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        raise RuntimeError(f"Database setup failed: {e}")

def update_movies_in_db():
    logger.info("Starting movie update process...")
    setup_database()
    
    conn = None
    cur = None
    new_movies_added = 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Get last sync time for incremental updates
        cur.execute("SELECT last_sync FROM sync_info ORDER BY id DESC LIMIT 1;")
        last_sync = cur.fetchone()
        last_sync_time = last_sync[0] if last_sync else None
        
        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}
        
        service = build('blogger', 'v3', developerKey=BLOGGER_API_KEY)
        all_items = []
        
        # Fetch all posts with incremental update if possible
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
            title = item.get('title')
            url = item.get('url')
            
            # Skip if this item was published before our last sync
            if last_sync_time and 'published' in item:
                try:
                    published_time = datetime.strptime(item['published'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if published_time < last_sync_time:
                        continue
                except:
                    pass
            
            if title and url and title.strip() not in existing_movies and title.strip() not in unique_titles:
                try:
                    cur.execute("INSERT INTO movies (title, url) VALUES (%s, %s);", (title.strip(), url.strip()))
                    new_movies_added += 1
                    unique_titles.add(title.strip())
                except psycopg2.Error as e:
                    logger.error(f"Error inserting movie {title}: {e}")
                    continue

        # Update sync time
        cur.execute("INSERT INTO sync_info (last_sync) VALUES (CURRENT_TIMESTAMP);")
        
        conn.commit()
        return f"Update complete. Added {new_movies_added} new items."
    
    except Exception as e:
        logger.error(f"Error during movie update: {e}")
        return f"An error occurred during update: {e}"
    
    finally:
        if cur: cur.close()
        if conn: conn.close()

def get_movie_from_db(user_query):
    conn = None
    try:
        # Use fuzzy matching for better search results
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # First try exact match
        cur.execute("SELECT title, url, file_id FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1", (user_query,))
        movie = cur.fetchone()
        if movie:
            return movie
        
        # Then try partial match with word boundaries
        cur.execute("SELECT title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 5", ('%' + user_query + '%',))
        movies = cur.fetchall()
        
        if movies:
            # Use fuzzy matching to find the best match
            movie_titles = [m[0] for m in movies]
            best_match = process.extractOne(user_query, movie_titles)
            
            if best_match and best_match[1] > 70:  # Confidence threshold
                for m in movies:
                    if m[0] == best_match[0]:
                        return m
        
        return None
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return None
    finally:
        if conn: conn.close()

# --- URL Validation Functions ---
def is_valid_url(url):
    """Check if a URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def normalize_url(url):
    """Normalize and clean URLs"""
    try:
        # Add https:// if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Fix common Blogspot URL issues
        if 'blogspot.com' in url and 'import-urlhttpsfonts' in url:
            url = url.replace('import-urlhttpsfonts', 'import-url-https-fonts')
        
        # Parse and reconstruct to normalize
        parsed = urlparse(url)
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        return normalized
    except:
        return url

# --- AI Intent Analysis ---
async def analyze_intent(message_text):
    """Analyze if the message is a movie request using AI"""
    if not GEMINI_API_KEY:
        return {"is_request": True, "content_title": message_text}
    
    try:
        # Simple keyword check before using AI to save time
        movie_keywords = ["movie", "film", "series", "watch", "download", "see", "चलचित्र", "फिल्म", "सीरीज"]
        if not any(keyword in message_text.lower() for keyword in movie_keywords):
            return {"is_request": False, "content_title": None}
        
        # Configure the AI with a strict prompt for intent analysis
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(model_name='gemini-1.5-flash')
        
        prompt = f"""
        You are a 'Request Analyzer' for a Telegram bot named Manvi.
        Manvi's ONLY purpose is to provide MOVIES and WEB SERIES. Nothing else.

        Analyze the user's message below. Your task is to determine ONLY ONE Thing: 
        Is the user asking for a movie or a web series?

        - If the user IS asking for a movie or web series, respond with a JSON object:
          {{"is_request": true, "content_title": "Name of the Movie/Series"}}

        - If the user is talking about ANYTHING ELSE (like an article, a song, a general conversation, a question, a greeting), you MUST respond with:
          {{"is_request": false, "content_title": null}}

        Do not explain yourself. Only provide the JSON.

        User's Message: "{message_text}"
        """
        
        response = model.generate_content(prompt)
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        else:
            return {"is_request": False, "content_title": None}
            
    except Exception as e:
        logger.error(f"Error in AI intent analysis: {e}")
        return {"is_request": True, "content_title": message_text}

# --- Admin Notification Function ---
async def send_admin_notification(context, user, movie_title, group_info=None):
    """Send notification to admin channel about a new request"""
    if not ADMIN_CHANNEL_ID:
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
        
        await context.bot.send_message(chat_id=ADMIN_CHANNEL_ID, text=message)
    except Exception as e:
        logger.error(f"Error sending admin notification: {e}")

# --- Flask App ---
flask_app = Flask('')
@flask_app.route('/')
def home():
    return "Bot is running!"

@flask_app.route('/health')
def health():
    return "OK", 200

@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    result = update_movies_in_db()
    return result

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port)

# --- Keyboard Markups ---
def get_main_keyboard():
    """Get the main menu keyboard"""
    keyboard = [
        ['🔍 Search Movies', '🙋 Request Movie'],
        ['📊 My Stats', '❓ Help']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_movie_options_keyboard(movie_title, url):
    """Get inline keyboard for movie options"""
    keyboard = [
        [InlineKeyboardButton("🎬 Watch Now", url=url)],
        [InlineKeyboardButton("📥 Download", callback_data=f"download_{movie_title}")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- Store User Request Function ---
def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT user_requests_unique_constraint DO NOTHING",
            (user_id, username, first_name, movie_title, group_id, message_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        return False

# --- Notification Functions ---
async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url):
    """Notify users who requested a movie when it becomes available"""
    logger.info(f"Attempting to notify users for movie: {movie_title}")
    conn = None
    cur = None
    notified_count = 0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, username, first_name, group_id, message_id FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
            (f'%{movie_title}%',)
        )
        users_to_notify = cur.fetchall()
        
        for user_id, username, first_name, group_id, message_id in users_to_notify:
            try:
                notification_text = f"Hey {first_name or username}! आपकी requested movie '{movie_title}' अब available है! 🎬"
                
                await context.bot.send_message(chat_id=user_id, text=notification_text)
                
                # Check if we have a file_id in the database
                movie_data = get_movie_from_db(movie_title)
                if movie_data and len(movie_data) > 2 and movie_data[2]:  # file_id exists
                    file_id = movie_data[2]
                    await context.bot.send_document(chat_id=user_id, document=file_id)
                elif movie_url.startswith("https://t.me/c/"):
                    parts = movie_url.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    msg_id = int(parts[-1])
                    await context.bot.copy_message(
                        chat_id=user_id, 
                        from_chat_id=from_chat_id, 
                        message_id=msg_id
                    )
                elif movie_url.startswith("http"):
                    # Send message with buttons for HTTP URLs
                    message_with_buttons = f"🎬 {movie_title} is now available!\n\nClick the buttons below:"
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=message_with_buttons,
                        reply_markup=get_movie_options_keyboard(movie_title, movie_url)
                    )
                else:
                    await context.bot.send_document(chat_id=user_id, document=movie_url)
                
                # Mark as notified
                cur.execute(
                    "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                    (user_id, f'%{movie_title}%')
                )
                conn.commit()
                notified_count += 1
                
            except telegram.error.Forbidden as e:
                logger.error(f"Could not notify user {user_id} for '{movie_title}': {e}. Bot was blocked by the user.")
                continue
            except Exception as e:
                logger.error(f"Other error notifying user {user_id} for '{movie_title}': {e}")
                continue
                
        return notified_count
    except Exception as e:
        logger.error(f"Error in notify_users_for_movie: {e}")
        return 0
    finally:
        if cur: cur.close()
        if conn: conn.close()

async def notify_in_group(context: ContextTypes.DEFAULT_TYPE, movie_title):
    """Notify users in group when a requested movie becomes available"""
    logger.info(f"Attempting to notify users in group for movie: {movie_title}")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, username, first_name, group_id, message_id FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
            (f'%{movie_title}%',)
        )
        users_to_notify = cur.fetchall()

        if not users_to_notify:
            return

        # Group users by their group_id
        groups_to_notify = {}
        for user_id, username, first_name, group_id, message_id in users_to_notify:
            if group_id not in groups_to_notify:
                groups_to_notify[group_id] = []
            groups_to_notify[group_id].append((user_id, username, first_name, message_id))

        for group_id, users in groups_to_notify.items():
            try:
                if not group_id:
                    continue

                notification_text = "Hey! आपकी requested movie अब आ गई है! 🥳\n\n"
                notified_users = []
                for user_id, username, first_name, message_id in users:
                    # Use first name if username is not available
                    mention = first_name or f"user_{user_id}"
                    notification_text += f"**{mention}**, "
                    notified_users.append(user_id)

                notification_text += f"\n\nआपकी फिल्म '{movie_title}' अब उपलब्ध है! इसे पाने के लिए, कृपया मुझे private chat में start करें: @{context.bot.username}"

                await context.bot.send_message(
                    chat_id=group_id, 
                    text=notification_text, 
                    parse_mode='Markdown'
                )

                # Update the notified status for users notified in the group
                for user_id in notified_users:
                    cur.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                        (user_id, f'%{movie_title}%')
                    )
                conn.commit()

            except Exception as e:
                logger.error(f"Failed to send fallback message to group {group_id}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in notify_in_group: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

# --- Telegram Bot Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        welcome_text = """
        क्या हाल है? मैं मानवी। 😉 
        फिल्मों पर गपशॉप करनी है तो बता।

        Use the buttons below to get started!
        """
        await update.message.reply_text(welcome_text, reply_markup=get_main_keyboard())
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start command: {e}")

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu options"""
    try:
        query = update.message.text
        
        if query == '🔍 Search Movies':
            await update.message.reply_text("Great! Tell me the name of the movie you want to search for.")
            return SEARCHING
            
        elif query == '🙋 Request Movie':
            await update.message.reply_text("Okay, you've chosen to request a new movie. Please tell me the name of the movie you want me to add.")
            return REQUESTING
            
        elif query == '📊 My Stats':
            # Implement stats functionality
            user_id = update.effective_user.id
            conn = None
            try:
                conn = psycopg2.connect(DATABASE_URL)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s", (user_id,))
                request_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND notified = TRUE", (user_id,))
                fulfilled_count = cur.fetchone()[0]
                
                stats_text = f"""
                📊 Your Stats:
                - Total Requests: {request_count}
                - Fulfilled Requests: {fulfilled_count}
                """
                await update.message.reply_text(stats_text)
            except Exception as e:
                logger.error(f"Error getting stats: {e}")
                await update.message.reply_text("Sorry, couldn't retrieve your stats at the moment.")
            finally:
                if conn: conn.close()
            
            return MAIN_MENU
            
        elif query == '❓ Help':
            help_text = """
            🤖 How to use Manvi Bot:
            
            🔍 Search Movies: Find movies in our collection
            🙋 Request Movie: Request a new movie to be added
            📊 My Stats: View your request statistics
            
            Just use the buttons below to navigate!
            """
            await update.message.reply_text(help_text)
            return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in main menu: {e}")
        return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie search"""
    try:
        user_message = update.message.text.strip()
        
        # First try to find movie in database
        movie_found = get_movie_from_db(user_message)
        
        if movie_found:
            title, url, file_id = movie_found
            
            # If we have a file_id, send the file directly
            if file_id:
                await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                await context.bot.send_document(chat_id=update.effective_chat.id, document=file_id)
            elif url.startswith("https://t.me/c/"):
                # Handle telegram channel links
                parts = url.split('/')
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                await context.bot.copy_message(chat_id=update.effective_chat.id, from_chat_id=from_chat_id, message_id=message_id)
            elif url.startswith("http"):
                # Handle regular URLs - send message with buttons
                response = f"🎉 Found it! '{title}' is available!\n\nClick the buttons below:"
                await update.message.reply_text(
                    response, 
                    reply_markup=get_movie_options_keyboard(title, url)
                )
            else:
                # Assume it's a file_id or direct file
                await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                await context.bot.send_document(chat_id=update.effective_chat.id, document=url)
        else:
            # Store the user's request
            user = update.effective_user
            store_user_request(
                user.id, 
                user.username, 
                user.first_name, 
                user_message,
                update.effective_chat.id if update.effective_chat.type != "private" else None,
                update.message.message_id
            )
            
            response = f"😔 Sorry, '{user_message}' is not in my collection right now. Would you like to request it?"
            keyboard = [[InlineKeyboardButton("✅ Yes, Request It", callback_data=f"request_{user_message}")]]
            await update.message.reply_text(
                response, 
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        
        await update.message.reply_text("What would you like to do next?", reply_markup=get_main_keyboard())
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in search movies: {e}")
        await update.message.reply_text("Sorry, something went wrong. Please try again.")
        return MAIN_MENU

async def request_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie requests"""
    try:
        user_message = update.message.text.strip()
        user = update.effective_user
        
        # First analyze intent
        intent = await analyze_intent(user_message)
        
        if not intent["is_request"]:
            await update.message.reply_text("That doesn't seem to be a movie title. Please provide a valid movie name to request.")
            return REQUESTING
        
        movie_title = intent["content_title"]
        
        # Store the request
        store_user_request(
            user.id, 
            user.username, 
            user.first_name, 
            movie_title,
            update.effective_chat.id if update.effective_chat.type != "private" else None,
            update.message.message_id
        )
        
        # Send admin notification
        group_info = f"{update.effective_chat.title} (ID: {update.effective_chat.id})" if update.effective_chat.type != "private" else None
        await send_admin_notification(context, user, movie_title, group_info)
        
        response = f"✅ Got it! Your request for '{movie_title}' has been sent to the admin. Thanks for helping improve our collection!"
        await update.message.reply_text(response)
        
        await update.message.reply_text("What would you like to do next?", reply_markup=get_main_keyboard())
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in request movie: {e}")
        await update.message.reply_text("Sorry, something went wrong. Please try again.")
        return MAIN_MENU

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks"""
    try:
        query = update.callback_query
        await query.answer()
        
        if query.data.startswith("request_"):
            movie_title = query.data.replace("request_", "")
            user = update.effective_user
            
            # Store the request
            store_user_request(
                user.id, 
                user.username, 
                user.first_name, 
                movie_title,
                update.effective_chat.id if update.effective_chat.type != "private" else None,
                update.callback_query.message.message_id
            )
            
            # Send admin notification
            await send_admin_notification(context, user, movie_title)
            
            response = f"✅ Got it! Your request for '{movie_title}' has been sent to the admin. Thanks for helping improve our collection!"
            await query.edit_message_text(response)
        
        elif query.data.startswith("download_"):
            movie_title = query.data.replace("download_", "")
            movie_found = get_movie_from_db(movie_title)
            
            if movie_found:
                title, url, file_id = movie_found
                if file_id:
                    await query.message.reply_document(document=file_id)
                elif url.startswith("http"):
                    await query.message.reply_text(f"Download options for '{title}':\n{url}")
                else:
                    await query.message.reply_document(document=url)
    except Exception as e:
        logger.error(f"Error in button callback: {e}")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current operation"""
    await update.message.reply_text("Operation cancelled.", reply_markup=get_main_keyboard())
    return MAIN_MENU

# --- Admin Commands ---
async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a movie manually - ANY TYPE OF LINK"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    
    conn = None
    try:
        parts = context.args
        if len(parts) < 2:
            await update.message.reply_text("गलत फॉर्मेट! ऐसे इस्तेमाल करें:\n/addmovie टाइटल का नाम [File ID या Link]")
            return
        
        value = parts[-1]
        title = " ".join(parts[:-1])
        
        # Debugging के लिए log करें
        logger.info(f"Adding movie: {title} with value: {value}")
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Check if it's a Telegram file ID (starts with specific patterns)
        if value.startswith(("BQAC", "BAAC", "CAAC", "AQAC")):
            cur.execute(
                "INSERT INTO movies (title, url, file_id) VALUES (%s, %s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url, file_id = EXCLUDED.file_id",
                (title.strip(), "", value.strip())
            )
            message = f"✅ '{title}' को file ID के साथ सफलतापूर्वक जोड़ दिया गया है।"
        
        # Check if it's any kind of URL
        elif "http" in value or "." in value:
            # Normalize and validate the URL
            normalized_url = normalize_url(value)
            
            if not is_valid_url(normalized_url):
                await update.message.reply_text("❌ Invalid URL format. Please provide a valid URL.")
                return
            
            cur.execute(
                "INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
                (title.strip(), normalized_url.strip())
            )
            message = f"✅ '{title}' को URL के साथ सफलतापूर्वक जोड़ दिया गया है।"
        
        else:
            await update.message.reply_text("❌ Invalid format. कृपया सही File ID या URL दें।")
            return
        
        conn.commit()
        await update.message.reply_text(message)
        
        # Notify users who requested this movie
        num_notified = await notify_users_for_movie(context, title, value)
        await notify_in_group(context, title)
        
        await update.message.reply_text(f"कुल {num_notified} users को notify किया गया है。")
            
    except Exception as e:
        logger.error(f"Error in add_movie command: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")
    finally:
        if conn: 
            conn.close()

async def notify_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually notify users about a movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं。")
        return
    
    try:
        if not context.args:
            await update.message.reply_text("Usage: /notify <movie_title>")
            return
        
        movie_title = " ".join(context.args)
        movie_found = get_movie_from_db(movie_title)
        
        if movie_found:
            title, value, file_id = movie_found
            num_notified = await notify_users_for_movie(context, title, value)
            await update.message.reply_text(f"{num_notified} users को '{title}' के लिए notify किया गया है。")
            await notify_in_group(context, title)
        else:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली。")
    except Exception as e:
        logger.error(f"Error in notify_manually: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")

# --- Error Handler ---
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and handle them gracefully"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Sorry, something went wrong. Please try again later.",
                reply_markup=get_main_keyboard()
            )
        except Exception:
            pass  # Avoid infinite loop if error occurs while sending error message

# --- Main Bot Function ---
def run_bot():
    """Run the Telegram bot"""
    logger.info("Bot is starting...")
    
    if not TELEGRAM_BOT_TOKEN:
        logger.error("No Telegram bot token found. Exiting.")
        return
    
    try:
        setup_database()
    except RuntimeError:
        return
        
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()

    # Add conversation handler with the states
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)],
            SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_movies)],
            REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_movie)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("notify", notify_manually))
    application.add_error_handler(error_handler)

    # Signal handling for graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal. Stopping bot...")
        loop = asyncio.get_event_loop()
        loop.create_task(application.stop())
        loop.create_task(application.shutdown())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Bot is starting polling...")
    
    application.run_polling(
        allowed_updates=Update.ALL_TYPES, 
        drop_pending_updates=True,
        close_loop=False
    )

# --- Run Both Flask and Bot ---
if __name__ == "__main__":
    # Check if another instance is already running
    try:
        lock_file = "/tmp/manvi_bot.lock"
        if os.path.exists(lock_file):
            logger.warning("Another instance might be running. Removing lock file.")
            os.remove(lock_file)
            
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
            
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        # Add a small delay to ensure Flask starts first
        import time
        time.sleep(2)
        
        run_bot()
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
    finally:
        # Clean up lock file
        if os.path.exists(lock_file):
            os.remove(lock_file)
