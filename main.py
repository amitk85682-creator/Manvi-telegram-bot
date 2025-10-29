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
from datetime import datetime, timedelta
from fuzzywuzzy import process, fuzz
import async_timeout
from urllib.parse import urlparse, urlunparse, quote
from collections import defaultdict

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

# Rate limiting dictionary
user_last_request = defaultdict(lambda: datetime.min)

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# --- Database URL Fix ---
def fix_database_url():
    """Fix database URL by properly encoding special characters"""
    try:
        # Parse the original URL
        parsed = urlparse(DATABASE_URL)
        
        # Extract components
        username = parsed.username
        password = parsed.password
        hostname = parsed.hostname
        port = parsed.port
        database = parsed.path[1:]  # Remove leading slash
        
        # If password contains special characters that need encoding
        if password and any(c in password for c in ['*', '!', '@', '#', '$', '%', '^', '&', '(', ')', '=', '+', '?']):
            # Encode the password
            encoded_password = quote(password)
            
            # Reconstruct the URL with encoded password
            fixed_url = f"postgresql://{username}:{encoded_password}@{hostname}:{port}/{database}"
            logger.info("Database URL fixed for special characters")
            return fixed_url
        else:
            return DATABASE_URL
    except Exception as e:
        logger.error(f"Error fixing database URL: {e}")
        return DATABASE_URL

# Use the fixed database URL
FIXED_DATABASE_URL = fix_database_url()

# --- Query Preprocessing ---
def preprocess_query(query):
    """Clean and normalize user query"""
    # Remove emojis and special symbols
    query = re.sub(r'[^\w\s-]', '', query)
    
    # Remove extra whitespaces
    query = ' '.join(query.split())
    
    # Remove common words that don't help in matching
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    
    return ' '.join(words).strip()

# --- Rate Limiting ---
async def check_rate_limit(user_id):
    """Check if user is rate limited"""
    now = datetime.now()
    last_request = user_last_request[user_id]
    
    if now - last_request < timedelta(seconds=2):
        return False
    
    user_last_request[user_id] = now
    return True

# --- Database Functions ---
def setup_database():
    try:
        # Use the fixed database URL
        conn = psycopg2.connect(FIXED_DATABASE_URL)
        cur = conn.cursor()
        
        # Enable pg_trgm extension for better fuzzy search
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
        
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
        
        # Create movie_aliases table for alias system
        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_aliases (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                alias TEXT NOT NULL,
                UNIQUE(movie_id, alias)
            )
        ''')
        
        # Add UNIQUE constraint for user_requests
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
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movie_aliases_alias ON movie_aliases (alias);')
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully with alias support")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        # Don't raise error, just log and continue
        logger.info("Continuing without database setup...")

def get_db_connection():
    """Get database connection with error handling"""
    try:
        return psycopg2.connect(FIXED_DATABASE_URL)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def update_movies_in_db():
    logger.info("Starting movie update process...")
    setup_database()
    
    conn = None
    cur = None
    new_movies_added = 0
    
    try:
        conn = get_db_connection()
        if not conn:
            return "Database connection failed"
            
        cur = conn.cursor()
        
        # Get last sync time for incremental updates
        cur.execute("SELECT last_sync FROM sync_info ORDER BY id DESC LIMIT 1;")
        last_sync = cur.fetchone()
        last_sync_time = last_sync if last_sync else None
        
        cur.execute("SELECT title FROM movies;")
        existing_movies = {row for row in cur.fetchall()}
        
        # Only proceed if Blogger API keys are available
        if not BLOGGER_API_KEY or not BLOG_ID:
            return "Blogger API keys not configured"
            
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
        conn = get_db_connection()
        if not conn:
            return None
            
        cur = conn.cursor()
        
        logger.info(f"Searching for: '{user_query}'")
        
        # First try exact match in movies table
        cur.execute("SELECT title, url, file_id FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1", (user_query,))
        movie = cur.fetchone()
        if movie:
            logger.info(f"Exact match found: '{movie}'")
            return movie
        
        # Then try alias match
        cur.execute("""
            SELECT m.title, m.url, m.file_id 
            FROM movies m 
            JOIN movie_aliases ma ON m.id = ma.movie_id 
            WHERE LOWER(ma.alias) = LOWER(%s) 
            LIMIT 1
        """, (user_query,))
        movie = cur.fetchone()
        if movie:
            logger.info(f"Alias match found: '{movie}'")
            return movie
        
        # Get ALL movies for fuzzy matching
        cur.execute("SELECT title, url, file_id FROM movies")
        all_movies = cur.fetchall()
        
        if not all_movies:
            return None
        
        # Use fuzzy matching with multiple strategies
        movie_titles = [m for m in all_movies]
        
        # Strategy 1: Token Sort Ratio (handles word order)
        best_match = process.extractOne(user_query, movie_titles, scorer=fuzz.token_sort_ratio)
        
        # Strategy 2: Partial Ratio (handles substrings)
        partial_match = process.extractOne(user_query, movie_titles, scorer=fuzz.partial_ratio)
        
        # Strategy 3: Token Set Ratio (handles extra/missing words)
        token_match = process.extractOne(user_query, movie_titles, scorer=fuzz.token_set_ratio)
        
        # Choose the best match with highest score
        matches = [best_match, partial_match, token_match]
        best_overall = max(matches, key=lambda x: x if x else 0)
        
        logger.info(f"Fuzzy matches - Token Sort: {best_match if best_match else 0}, Partial: {partial_match if partial_match else 0}, Token Set: {token_match if token_match else 0}")
        
        # Lower threshold to 65 for better typo handling
        if best_overall and best_overall >= 65:
            for m in all_movies:
                if m == best_overall:
                    logger.info(f"Fuzzy match found: '{user_query}' matched to '{m}' with score {best_overall}")
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
        
        # Handle anchor tags properly
        if '#' in url:
            base, anchor = url.split('#', 1)
            # Ensure the base URL is properly formatted
            parsed = urlparse(base)
            normalized_base = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                ''
            ))
            url = f"{normalized_base}#{anchor}"
        else:
            # Parse and reconstruct to normalize
            parsed = urlparse(url)
            url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
        
        return url
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

        Analyze the user's message below. Your task is to determine ONLY ONE THING: 
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
        conn = get_db_connection()
        if not conn:
            return False
            
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

# --- Auto-Delete Callback ---
async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after specified delay - NO JobQueue needed"""
    try:
        await asyncio.sleep(delay)
        for msg_id in message_ids:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                logger.info(f"✅ Deleted message {msg_id} from chat {chat_id}")
            except Exception as e:
                logger.error(f"Failed to delete message {msg_id}: {e}")
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")

# --- Notification Functions ---
async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url):
    """Notify users who requested a movie - FIXED"""
    logger.info(f"Attempting to notify users for movie: {movie_title}")
    conn = None
    cur = None
    notified_count = 0
    
    try:
        conn = get_db_connection()
        if not conn:
            return 0
            
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, username, first_name FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
            (f'%{movie_title}%',)
        )
        users_to_notify = cur.fetchall()
        
        for user_id, username, first_name in users_to_notify:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"🎉 Hey {first_name or username}! Your requested movie '{movie_title}' is now available!"
                )
                
                warning_msg = await context.bot.send_message(
                    chat_id=user_id,
                    text="⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌\n\nJoin » [FilmfyBox](http://t.me/filmfybox)",
                    parse_mode='Markdown'
                )
                
                movie_data = get_movie_from_db(movie_title)
                sent_msg = None
                
                if movie_data and len(movie_data) > 2:
                    sent_msg = await context.bot.send_document(chat_id=user_id, document=movie_data)
                elif movie_url.startswith("https://t.me/c/"):
                    parts = movie_url.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    msg_id = int(parts[-1])
                    sent_msg = await context.bot.copy_message(
                        chat_id=user_id, 
                        from_chat_id=from_chat_id, 
                        message_id=msg_id
                    )
                elif movie_url.startswith("http"):
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"🎬 {movie_title} is now available!",
                        reply_markup=get_movie_options_keyboard(movie_title, movie_url)
                    )
                else:
                    sent_msg = await context.bot.send_document(chat_id=user_id, document=movie_url)
                
                # Auto-delete
                if sent_msg:
                    asyncio.create_task(
                        delete_messages_after_delay(
                            context,
                            user_id,
                            [sent_msg.message_id, warning_msg.message_id],
                            60
                        )
                    )
                
                cur.execute(
                    "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                    (user_id, f'%{movie_title}%')
                )
                conn.commit()
                notified_count += 1
                
            except telegram.error.Forbidden:
                logger.error(f"User {user_id} blocked the bot")
                continue
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")
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
        conn = get_db_connection()
        if not conn:
            return
            
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
📨 Sᴇɴᴅ Mᴏᴠɪᴇ Oʀ Sᴇʀɪᴇs Nᴀᴍᴇ ᴀɴᴅ Yᴇᴀʀ Aꜱ Pᴇʀ Gᴏᴏɢʟᴇ Sᴘᴇʟʟɪɴɢ..!! 👍

⚠️ Exᴀᴍᴘʟᴇ Fᴏʀ Mᴏᴠɪᴇ 👇

👉 Jailer
👉 Jailer 2023

⚠️ Exᴀᴍᴘʟᴇ Fᴏʀ WᴇʙSᴇʀɪᴇs 👇

👉 Stranger Things 
👉 Stranger Things S02 E04

⚠️ ᴅᴏɴ'ᴛ ᴀᴅᴅ ᴇᴍᴏᴊɪꜱ ᴀɴᴅ ꜱʏᴍʙᴏʟꜱ ɪɴ ᴍᴏᴠɪᴇ ɴᴀᴍᴇ, ᴜꜱᴇ ʟᴇᴛᴛᴇʀꜱ ᴏɴʟʏ..!! ❌
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
                conn = get_db_connection()
                if conn:
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s", (user_id,))
                    request_count = cur.fetchone()
                    
                    cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND notified = TRUE", (user_id,))
                    fulfilled_count = cur.fetchone()
                    
                    stats_text = f"""
📊 Your Stats:
- Total Requests: {request_count}
- Fulfilled Requests: {fulfilled_count}
                    """
                    await update.message.reply_text(stats_text)
                else:
                    await update.message.reply_text("Sorry, database connection failed.")
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
    """Handle movie search - FIXED auto-delete"""
    try:
        # Rate limiting
        if not await check_rate_limit(update.effective_user.id):
            await update.message.reply_text("⚠️ Please wait a moment before searching again.")
            return SEARCHING
        
        user_message = update.message.text.strip()
        processed_query = preprocess_query(user_message) if user_message else user_message
        search_query = processed_query if processed_query else user_message
        
        # Search in database
        movie_found = get_movie_from_db(search_query)
        
        if movie_found:
            title, url, file_id = movie_found
            
            # Send warning message
            warning_msg = await update.message.reply_text(
                "⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌\n\n"
                "Join » [FilmfyBox](http://t.me/filmfybox)",
                parse_mode='Markdown'
            )
            
            sent_msg = None
            
            # Send movie based on type
            if file_id:
                sent_msg = await context.bot.send_document(
                    chat_id=update.effective_chat.id, 
                    document=file_id,
                    caption=f"🎬 {title}"
                )
            elif url.startswith("https://t.me/c/"):
                parts = url.split('/')
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                sent_msg = await context.bot.copy_message(
                    chat_id=update.effective_chat.id, 
                    from_chat_id=from_chat_id, 
                    message_id=message_id
                )
            elif url.startswith("http"):
                await update.message.reply_text(
                    f"🎉 Found it! '{title}' is available!\n\nClick the buttons below:",
                    reply_markup=get_movie_options_keyboard(title, url)
                )
            else:
                sent_msg = await context.bot.send_document(
                    chat_id=update.effective_chat.id, 
                    document=url,
                    caption=f"🎬 {title}"
                )
            
            # Schedule auto-delete using asyncio (NOT JobQueue)
            if sent_msg:
                asyncio.create_task(
                    delete_messages_after_delay(
                        context,
                        update.effective_chat.id,
                        [sent_msg.message_id, warning_msg.message_id],
                        60
                    )
                )
                logger.info(f"🕐 Auto-delete scheduled for messages {sent_msg.message_id}, {warning_msg.message_id}")
        else:
            # Movie not found - store request
            user = update.effective_user
            store_user_request(
                user.id, 
                user.username, 
                user.first_name, 
                user_message,
                update.effective_chat.id if update.effective_chat.type != "private" else None,
                update.message.message_id
            )
            
            # Send error messages
            await update.message.reply_text(
                f"😔 Sorry, '{user_message}' is not in my collection right now. Would you like to request it?",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, Request It", callback_data=f"request_{user_message[:50]}")]])
            )
            
            error_msg = """
● I could not find the file you requested 😕

● Is the movie you asked about released on OTT..?

● Pay attention to the following…

● Ask for correct spelling.

● Do not ask for movies that are not released on OTT platforms.

● Also ask [movie name, language] like this..‌‌.

📝 Example:
👉 Kalki 2898 AD Tamil
👉 Thamma Hindi
👉 Stranger Things S02 E04
"""
            await update.message.reply_text(error_msg)
        
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
    """Handle inline button callbacks - FIXED"""
    try:
        query = update.callback_query
        await query.answer()
        
        if query.data.startswith("request_"):
            movie_title = query.data.replace("request_", "")
            user = update.effective_user
            
            store_user_request(
                user.id, 
                user.username, 
                user.first_name, 
                movie_title,
                update.effective_chat.id if update.effective_chat.type != "private" else None,
                query.message.message_id
            )
            
            if ADMIN_CHANNEL_ID:
                await send_admin_notification(context, user, movie_title)
            
            await query.edit_message_text(f"✅ Got it! Your request for '{movie_title}' has been sent to the admin!")
        
        elif query.data.startswith("download_"):
            movie_title = query.data.replace("download_", "")
            movie_found = get_movie_from_db(movie_title)
            
            if movie_found:
                title, url, file_id = movie_found
                
                warning_msg = await query.message.reply_text(
                    "⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌\n\n"
                    "Join » [FilmfyBox](http://t.me/filmfybox)",
                    parse_mode='Markdown'
                )
                
                sent_msg = None
                
                if file_id:
                    sent_msg = await query.message.reply_document(document=file_id)
                elif url.startswith("http"):
                    await query.message.reply_text(f"Download link:\n{url}")
                else:
                    sent_msg = await query.message.reply_document(document=url)
                
                # Auto-delete
                if sent_msg:
                    asyncio.create_task(
                        delete_messages_after_delay(
                            context,
                            query.message.chat_id,
                            [sent_msg.message_id, warning_msg.message_id],
                            60
                        )
                    )
                    logger.info(f"🕐 Auto-delete scheduled for download messages")
                    
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
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
            
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
            # Simply use the URL as provided by admin
            normalized_url = value.strip()
            
            # सिर्फ basic validation
            if not value.startswith(('http://', 'https://')):
                await update.message.reply_text("❌ Invalid URL format. URL must start with http:// or https://")
                return
            
            cur.execute(
                "INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
                (title.strip(), normalized_url)
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
        
        await update.message.reply_text(f"कुल {num_notified} users को notify किया गया है।")
            
    except Exception as e:
        logger.error(f"Error in add_movie command: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")
    finally:
        if conn: 
            conn.close()

async def bulk_add_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add multiple movies at once using bulk command"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    
    try:
        if not context.args:
            await update.message.reply_text("""
गलत फॉर्मेट! ऐसे इस्तेमाल करें:

/bulkadd
/addmovie Movie1 https://link1.com
/addmovie Movie2 https://link2.com  
/addmovie Movie3 https://link3.com

या फिर:

/bulkadd
Movie1 https://link1.com
Movie2 https://link2.com
Movie3 https://link3.com
""")
            return

        # Get the entire message text
        full_text = update.message.text
        lines = full_text.split('\n')
        
        success_count = 0
        failed_count = 0
        results = []
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('/bulkadd'):
                continue
                
            # Handle both formats: with or without /addmovie prefix
            if line.startswith('/addmovie'):
                parts = line.split()
                if len(parts) >= 3:
                    title = ' '.join(parts[1:-1])
                    url = parts[-1]
                else:
                    continue
            else:
                parts = line.split()
                if len(parts) >= 2:
                    title = ' '.join(parts[:-1])
                    url = parts[-1]
                else:
                    continue
            
            # Add the movie to database
            try:
                conn = get_db_connection()
                if not conn:
                    failed_count += 1
                    results.append(f"❌ {title} - Database connection failed")
                    continue
                    
                cur = conn.cursor()
                
                # Normalize URL
                normalized_url = normalize_url(url)
                
                cur.execute(
                    "INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
                    (title.strip(), normalized_url.strip())
                )
                conn.commit()
                conn.close()
                
                success_count += 1
                results.append(f"✅ {title}")
            except Exception as e:
                failed_count += 1
                results.append(f"❌ {title} - Error: {str(e)}")
        
        # Send results
        result_message = f"""
📊 Bulk Add Results:

Successfully added: {success_count}
Failed: {failed_count}

Details:
""" + "\n".join(results[:10])  # Show first 10 results to avoid message too long error
        
        if len(results) > 10:
            result_message += f"\n\n... और {len(results) - 10} more items"
        
        await update.message.reply_text(result_message)
        
    except Exception as e:
        logger.error(f"Error in bulk_add_movies: {e}")
        await update.message.reply_text(f"Bulk add में error: {e}")

# --- Alias System Commands ---
async def add_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add an alias for an existing movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    
    try:
        if not context.args or len(context.args) < 2:
            await update.message.reply_text("गलत फॉर्मेट! ऐसे इस्तेमाल करें:\n/addalias मूवी_का_असली_नाम alias_name")
            return
        
        # Extract movie title and alias
        parts = context.args
        alias = parts[-1]
        movie_title = " ".join(parts[:-1])
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
            
        cur = conn.cursor()
        
        # First find the movie ID
        cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
        movie = cur.fetchone()
        
        if not movie:
            await update.message.reply_text(f"❌ '{movie_title}' डेटाबेस में नहीं मिली। पहले मूवी को add करें।")
            return
        
        movie_id = movie
        
        # Add the alias
        cur.execute(
            "INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING",
            (movie_id, alias.lower())
        )
        
        conn.commit()
        await update.message.reply_text(f"✅ Alias '{alias}' successfully added for '{movie_title}'")
        
    except Exception as e:
        logger.error(f"Error adding alias: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            conn.close()

async def list_aliases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all aliases for a movie"""
    conn = None
    try:
        if not context.args:
            await update.message.reply_text("कृपया मूवी का नाम दें:\n/aliases मूवी_का_नाम")
            return
        
        movie_title = " ".join(context.args)
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
            
        cur = conn.cursor()
        
        # Get movie and its aliases
        cur.execute("""
            SELECT m.title, COALESCE(array_agg(ma.alias), '{}'::text[]) 
            FROM movies m 
            LEFT JOIN movie_aliases ma ON m.id = ma.movie_id 
            WHERE m.title = %s 
            GROUP BY m.title
        """, (movie_title,))
        
        result = cur.fetchone()
        
        if not result:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")
            return
        
        title, aliases = result
        aliases_list = "\n".join(aliases) if aliases else "कोई aliases नहीं हैं"
        
        await update.message.reply_text(f"🎬 {title}\n\nAliases:\n{aliases_list}")
        
    except Exception as e:
        logger.error(f"Error listing aliases: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            conn.close()

async def bulk_add_aliases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add multiple aliases at once"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    
    conn = None
    try:
        if not context.args:
            await update.message.reply_text("""
गलत फॉर्मेट! ऐसे इस्तेमाल करें:

/aliasbulk
Movie1: alias1, alias2, alias3
Movie2: alias4, alias5
Movie3: alias6, alias7, alias8
""")
            return

        full_text = update.message.text
        lines = full_text.split('\n')
        
        success_count = 0
        failed_count = 0
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
            
        cur = conn.cursor()
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('/aliasbulk'):
                continue
                
            if ':' not in line:
                continue
                
            movie_title, aliases_str = line.split(':', 1)
            movie_title = movie_title.strip()
            aliases = [alias.strip() for alias in aliases_str.split(',')]
            
            # Find movie ID
            cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
            movie = cur.fetchone()
            
            if not movie:
                failed_count += 1
                continue
                
            movie_id = movie
            
            # Add all aliases
            for alias in aliases:
                if alias:  # Skip empty aliases
                    try:
                        cur.execute(
                            "INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING",
                            (movie_id, alias.lower())
                        )
                        success_count += 1
                    except:
                        failed_count += 1
        
        conn.commit()
        
        await update.message.reply_text(f"""
📊 Alias Bulk Add Results:

Successfully added: {success_count}
Failed: {failed_count}
""")
        
    except Exception as e:
        logger.error(f"Error in bulk alias add: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            conn.close()

async def notify_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually notify users about a movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
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
            await update.message.reply_text(f"{num_notified} users को '{title}' के लिए notify किया गया है।")
            await notify_in_group(context, title)
        else:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")
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

            # Notify User by Username

async def notify_user_with_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin command to notify user with media by replying to a message
    Usage: Reply to any media file/video/audio with:
           /notify @username Optional message here
    
    Example:
    1. Reply to a video file
    2. Type: /notify @amit002 Here's the movie you requested!
    """
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    try:
        # Check if command is a reply
        if not update.message.reply_to_message:
            await update.message.reply_text(
                "❌ Please reply to a message (file/video/audio/photo) with:\n"
                "/notify @username Optional message"
            )
            return
        
        # Parse command arguments
        if not context.args:
            await update.message.reply_text(
                "Usage: /notify @username [optional message]\n"
                "Example: /notify @amit002 Here's your requested movie!"
            )
            return
        
        # Extract username
        target_username = context.args.replace('@', '')
        
        # Extract optional message (everything after username)
        optional_message = ' '.join(context.args[1:]) if len(context.args) > 1 else None
        
        # Get the replied message
        replied_message = update.message.reply_to_message
        
        # Find user in database
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()
        
        if not user:
            await update.message.reply_text(f"❌ User @{target_username} not found in database.")
            cur.close()
            conn.close()
            return
        
        user_id, first_name = user
        
        # Prepare notification message
        notification_header = f"📬 **Message from Admin**\n"
        if optional_message:
            notification_header += f"\n{optional_message}\n"
        
        # Send warning message first
        warning_msg = await context.bot.send_message(
            chat_id=user_id,
            text=notification_header + "\n⚠️ ❌👉This file automatically❗️delete after 1 minute❗️so please forward in another chat👈❌",
            parse_mode='Markdown'
        )
        
        sent_msg = None
        media_type = "unknown"
        
        # Detect media type and forward accordingly
        if replied_message.document:
            # Document/File
            media_type = "file"
            sent_msg = await context.bot.send_document(
                chat_id=user_id,
                document=replied_message.document.file_id,
                caption=optional_message if optional_message else None
            )
        
        elif replied_message.video:
            # Video
            media_type = "video"
            sent_msg = await context.bot.send_video(
                chat_id=user_id,
                video=replied_message.video.file_id,
                caption=optional_message if optional_message else None
            )
        
        elif replied_message.audio:
            # Audio
            media_type = "audio"
            sent_msg = await context.bot.send_audio(
                chat_id=user_id,
                audio=replied_message.audio.file_id,
                caption=optional_message if optional_message else None
            )
        
        elif replied_message.photo:
            # Photo
            media_type = "photo"
            # Get largest photo
            photo = replied_message.photo[-1]
            sent_msg = await context.bot.send_photo(
                chat_id=user_id,
                photo=photo.file_id,
                caption=optional_message if optional_message else None
            )
        
        elif replied_message.voice:
            # Voice message
            media_type = "voice"
            sent_msg = await context.bot.send_voice(
                chat_id=user_id,
                voice=replied_message.voice.file_id,
                caption=optional_message if optional_message else None
            )
        
        elif replied_message.video_note:
            # Video note (round video)
            media_type = "video_note"
            sent_msg = await context.bot.send_video_note(
                chat_id=user_id,
                video_note=replied_message.video_note.file_id
            )
        
        elif replied_message.animation:
            # GIF/Animation
            media_type = "animation"
            sent_msg = await context.bot.send_animation(
                chat_id=user_id,
                animation=replied_message.animation.file_id,
                caption=optional_message if optional_message else None
            )
        
        elif replied_message.text:
            # Plain text message
            media_type = "text"
            text_to_send = replied_message.text
            if optional_message:
                text_to_send = f"{optional_message}\n\n{text_to_send}"
            sent_msg = await context.bot.send_message(
                chat_id=user_id,
                text=text_to_send
            )
        
        else:
            await update.message.reply_text("❌ Unsupported media type in the replied message.")
            cur.close()
            conn.close()
            return
        
        # Schedule auto-delete for media (not for text)
        if sent_msg and media_type != "text":
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    user_id,
                    [sent_msg.message_id, warning_msg.message_id],
                    60
                )
            )
            logger.info(f"🕐 Auto-delete scheduled for {media_type} sent to {user_id}")
        
        # Send confirmation to admin
        confirmation = f"✅ **Notification Sent!**\n\n"
        confirmation += f"To: @{target_username} ({first_name})\n"
        confirmation += f"Media Type: {media_type.capitalize()}\n"
        if optional_message:
            confirmation += f"Message: {optional_message[:50]}...\n"
        confirmation += f"Auto-delete: {'Yes (60s)' if media_type != 'text' else 'No'}"
        
        await update.message.reply_text(confirmation, parse_mode='Markdown')
        
        cur.close()
        conn.close()
        
    except telegram.error.Forbidden:
        await update.message.reply_text(f"❌ User @{target_username} blocked the bot.")
    except Exception as e:
        logger.error(f"Error in notify_user_with_media: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Broadcast to All Users
    async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
    Admin command to broadcast message to all users
    Usage: /broadcast Your message here
    """
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    try:
        if not context.args:
            await update.message.reply_text(
                "Usage: /broadcast Your message here\n"
                "Example: /broadcast 🎉 New feature added!"
            )
            return
        
        message_text = ' '.join(context.args)
        
        # Get all unique users from database
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id, first_name, username FROM user_requests")
        all_users = cur.fetchall()
        
        if not all_users:
            await update.message.reply_text("No users found in database.")
            cur.close()
            conn.close()
            return
        
        # Send status message
        status_msg = await update.message.reply_text(
            f"📤 Broadcasting to {len(all_users)} users...\n⏳ Please wait..."
        )
        
        success_count = 0
        failed_count = 0
        
        broadcast_text = f"📢 **Broadcast Message**\n\n{message_text}"
        
        for user_id, first_name, username in all_users:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=broadcast_text,
                    parse_mode='Markdown'
                )
                success_count += 1
                await asyncio.sleep(0.05)  # Rate limiting
            except telegram.error.Forbidden:
                failed_count += 1
                logger.info(f"User {user_id} blocked the bot")
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to send to {user_id}: {e}")
        
        await status_msg.edit_text(
            f"📊 **Broadcast Complete**\n\n"
            f"✅ Sent: {success_count}\n"
            f"❌ Failed: {failed_count}\n"
            f"📝 Total: {len(all_users)}"
        )
        
        cur.close()
        conn.close()
        
    except Exception as e:
            logger.error(f"Error in broadcast_message: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

# Get User Info (यह फंक्शन बाहर और सही इंडेंटेशन पर होना चाहिए)
async def get_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin command to get user information
    Usage: /userinfo @username
    """
    try:
        # एडमिन चेक
        if update.effective_user.id != ADMIN_USER_ID:
            await update.message.reply_text("⛔ Admin only command.")
            return

        # आर्गुमेंट (argument) चेक
        if not context.args:
            await update.message.reply_text("Usage: /userinfo @username")
            return
        
        target_username = context.args.replace('@', '')
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        
        # Get user stats
        cur.execute("""
            SELECT 
                user_id,
                username,
                first_name,
                COUNT(*) as total_requests,
                SUM(CASE WHEN notified = TRUE THEN 1 ELSE 0 END) as fulfilled,
                MAX(requested_at) as last_request
            FROM user_requests 
            WHERE username ILIKE %s 
            GROUP BY user_id, username, first_name
        """, (target_username,))
        
        user_info = cur.fetchone()
        
        if not user_info:
            await update.message.reply_text(f"❌ No data found for @{target_username}")
            cur.close()
            conn.close()
            return
        
        user_id, username, first_name, total, fulfilled, last_request = user_info
        
        # Get recent requests
        cur.execute("""
            SELECT movie_title, requested_at, notified 
            FROM user_requests 
            WHERE user_id = %s 
            ORDER BY requested_at DESC 
            LIMIT 5
        """, (user_id,))
        recent_requests = cur.fetchall()
        
        info_text = f"""
👤 **User Information**

**Basic Info:**
• Name: {first_name}
• Username: @{username}
• User ID: `{user_id}`

**Statistics:**
• Total Requests: {total}
• Fulfilled: {fulfilled}
• Pending: {total - fulfilled}
• Last Request: {last_request.strftime('%Y-%m-%d %H:%M') if last_request else 'N/A'}

**Recent Requests:**
"""
        
        for movie, req_time, notified in recent_requests:
            status = "✅" if notified else "⏳"
            info_text += f"{status} {movie} - {req_time.strftime('%m/%d %H:%M')}\n"
        
        await update.message.reply_text(info_text, parse_mode='Markdown')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in get_user_info: {e}")
        await update.message.reply_text(f"❌ Error: {e}")
        # List All Users
async def list_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin command to list all bot users
    Usage: /listusers [page_number]
    """
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    try:
        page = 1
        # ✅ सही तरीका: लिस्ट के पहले एलिमेंट को चेक करें
        if context.args and context.args[0].isdigit():
            page = int(context.args[0])
        
        per_page = 10
        offset = (page - 1) * per_page
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        
        # Get total count
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        total_users = cur.fetchone()
        
        # Get users for current page
        cur.execute("""
            SELECT 
                user_id,
                username,
                first_name,
                COUNT(*) as requests,
                MAX(requested_at) as last_seen
            FROM user_requests 
            GROUP BY user_id, username, first_name
            ORDER BY MAX(requested_at) DESC
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        
        users = cur.fetchall()
        
        total_pages = (total_users + per_page - 1) // per_page
        
        users_text = f"👥 **Bot Users** (Page {page}/{total_pages})\n\n"
        
        for idx, (user_id, username, first_name, req_count, last_seen) in enumerate(users, start=offset+1):
            username_str = f"@{username}" if username else "N/A"
            users_text += f"{idx}. {first_name} ({username_str})\n"
            users_text += f"   ID: `{user_id}` | Requests: {req_count}\n"
            users_text += f"   Last seen: {last_seen.strftime('%Y-%m-%d %H:%M')}\n\n"
        
        users_text += f"\n📊 Total Users: {total_users}"
        
        await update.message.reply_text(users_text, parse_mode='Markdown')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in list_all_users: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Schedule Notifications
async def schedule_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin command to schedule a notification
    Usage: /schedulenotify <minutes> <@username> <message>
    """
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    try:
        # ✅ आर्गुमेंट्स की संख्या चेक करें
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "Usage: /schedulenotify <minutes> <@username> <message>\n"
                "Example: /schedulenotify 30 @john New movie arriving soon!"
            )
            return
        
        delay_minutes = int(context.args)
        target_username = context.args<!--citation:1-->.replace('@', '')
        message_text = ' '.join(context.args[2:])
        
        # Get user_id
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()
        
        if not user:
            await update.message.reply_text(f"❌ User @{target_username} not found.")
            cur.close()
            conn.close()
            return
        
        user_id, first_name = user
        
        async def send_scheduled_notification(context, chat_id, message, logger):
    """Sends the actual notification message."""
    try:
        notification_text = f"⏰ **Scheduled Message**\n\n{message}"
        await context.bot.send_message(
            chat_id=chat_id,
            text=notification_text,
            parse_mode='MarkdownV2'  # 'MarkdownV2' का उपयोग करना बेहतर है
        )
        logger.info(f"Scheduled notification sent to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send scheduled notification to {chat_id}: {e}")
        
        # Create task
        asyncio.create_task(send_scheduled_notification())
        
        await update.message.reply_text(
            f"⏰ Notification scheduled!\n\n"
            f"To: @{target_username} ({first_name})\n"
            f"Delay: {delay_minutes} minutes\n"
            f"Message: {message_text[:50]}..."
        )
        
        cur.close()
        conn.close()
        
    except ValueError:
        await update.message.reply_text("❌ Invalid delay. Please provide number of minutes.")
    except Exception as e:
        logger.error(f"Error in schedule_notification: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Admin Help Command
        async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show admin commands help"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    help_text = """
👑 **Admin Commands Guide**

**User Notifications:**
• `/notifyuser @username message` - Send message to specific user
• `/broadcast message` - Send to all users
• `/schedulenotify <min> @user msg` - Schedule notification

**User Management:**
• `/userinfo @username` - Get user details & stats
• `/listusers [page]` - List all bot users

**Movie Management:**
• `/addmovie Title URL/FileID` - Add movie
• `/addalias MovieTitle alias` - Add alias
• `/notify MovieTitle` - Manually notify users

**Database:**
• `/stats` - Get database statistics
• `/clearold` - Clear old notifications (30+ days)

**Examples:**

# Bot Statistics
async def get_bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get comprehensive bot statistics"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        
        # Total movies
        cur.execute("SELECT COUNT(*) FROM movies")
        total_movies = cur.fetchone()
        
        # Total users
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        total_users = cur.fetchone()
        
        # Total requests
        cur.execute("SELECT COUNT(*) FROM user_requests")
        total_requests = cur.fetchone()
        
        # Fulfilled requests
        cur.execute("SELECT COUNT(*) FROM user_requests WHERE notified = TRUE")
        fulfilled = cur.fetchone()
        
        # Today's requests
        cur.execute("SELECT COUNT(*) FROM user_requests WHERE DATE(requested_at) = CURRENT_DATE")
        today_requests = cur.fetchone()
        
        # Top requesters
        cur.execute("""
            SELECT first_name, username, COUNT(*) as req_count
            FROM user_requests
            GROUP BY user_id, first_name, username
            ORDER BY req_count DESC
            LIMIT 5
        """)
        top_users = cur.fetchall()
        
        stats_text = f"""
📊 **Bot Statistics**

**Database:**
• Movies: {total_movies}
• Users: {total_users}
• Total Requests: {total_requests}
• Fulfilled: {fulfilled} ({(fulfilled/total_requests*100):.1f}%)
• Pending: {total_requests - fulfilled}

**Activity:**
• Today's Requests: {today_requests}
• Fulfillment Rate: {(fulfilled/total_requests*100):.1f}%

**Top Requesters:**
"""
        
        for name, username, count in top_users:
            username_str = f"@{username}" if username else "N/A"
            stats_text += f"• {name} ({username_str}): {count} requests\n"
        
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in get_bot_stats: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Broadcast with Media
async def broadcast_with_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin command to broadcast media to all users
    Usage: Reply to any media with: /broadcastmedia [Optional message]
    """
    # 1. एडमिन है या नहीं, यह चेक करें
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    # 2. चेक करें कि कमांड किसी मैसेज के रिप्लाई में है या नहीं
    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Please reply to a media message to broadcast it.")
        return
        
        optional_message = ' '.join(context.args) if context.args else None
        replied_message = update.message.reply_to_message
        
        # Get all users
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id, first_name, username FROM user_requests")
        all_users = cur.fetchall()
        
        if not all_users:
            await update.message.reply_text("No users found in database.")
            cur.close()
            conn.close()
            return
        
        # Status message
        status_msg = await update.message.reply_text(
            f"📤 Broadcasting media to {len(all_users)} users...\n⏳ Please wait..."
        )
        
        success_count = 0
        failed_count = 0
        
        for user_id, first_name, username in all_users:
            try:
                # Send header message
                header = "📢 **Broadcast from Admin**\n"
                if optional_message:
                    header += f"\n{optional_message}\n"
                
                await context.bot.send_message(
                    chat_id=user_id,
                    text=header,
                    parse_mode='Markdown'
                )
                
                # Send media based on type
                if replied_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=replied_message.document.file_id
                    )
                elif replied_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=replied_message.video.file_id
                    )
                elif replied_message.audio:
                    await context.bot.send_audio(
                        chat_id=user_id,
                        audio=replied_message.audio.file_id
                    )
                elif replied_message.photo:
                    photo = replied_message.photo[-1]
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=photo.file_id
                    )
                
                success_count += 1
                await asyncio.sleep(0.1)  # Rate limiting
                
            except telegram.error.Forbidden:
                failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed broadcast to {user_id}: {e}")
        
        await status_msg.edit_text(
            f"📊 **Broadcast Complete**\n\n"
            f"✅ Sent: {success_count}\n"
            f"❌ Failed: {failed_count}\n"
            f"📝 Total: {len(all_users)}"
        )
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in broadcast_with_media: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Quick Reply Notify
async def quick_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Quick notify - sends media to specific requesters.
    Usage: Reply to media with: /qnotify <@username_or_user_id | movie_title>
    """
    # 1. एडमिन चेक
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    # 2. रिप्लाई और आर्गुमेंट चेक
    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Reply to a media message first!")
        return
        
    if not context.args:
        await update.message.reply_text("Usage: /qnotify <@username | user_id | MovieTitle>")
        return
        
        query = ' '.join(context.args)
        replied_message = update.message.reply_to_message
        
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        
        # Determine if query is username or movie title
        target_users = []
        
        if query.startswith('@'):
            # It's a username
            username = query.replace('@', '')
            cur.execute(
                "SELECT DISTINCT user_id, first_name, username FROM user_requests WHERE username ILIKE %s",
                (username,)
            )
            target_users = cur.fetchall()
        else:
            # It's a movie title - find all users who requested it
            cur.execute(
                "SELECT DISTINCT user_id, first_name, username FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
                (f'%{query}%',)
            )
            target_users = cur.fetchall()
        
        if not target_users:
            await update.message.reply_text(f"❌ No users found for '{query}'")
            cur.close()
            conn.close()
            return
        
        # Send to all found users
        success_count = 0
        
        for user_id, first_name, username in target_users:
            try:
                # Send media
                if replied_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=replied_message.document.file_id,
                        caption=f"🎬 {query}" if not query.startswith('@') else None
                    )
                elif replied_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=replied_message.video.file_id,
                        caption=f"🎬 {query}" if not query.startswith('@') else None
                    )
                
                success_count += 1
                
                # Mark as notified if it was a movie title search
                if not query.startswith('@'):
                    cur.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                        (user_id, f'%{query}%')
                    )
                    conn.commit()
                
            except Exception as e:
                logger.error(f"Failed to send to {user_id}: {e}")
        
        await update.message.reply_text(
            f"✅ Sent to {success_count} user(s)\n"
            f"Query: {query}"
        )
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in quick_notify: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Forward from Channel
async def forward_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Forward message from channel to user.
    Usage: Reply to any channel message with: /forwardto @username
    """
    # 1. एडमिन चेक
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return
    
    # 2. रिप्लाई और आर्गुमेंट चेक
    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Reply to a message first!")
        return
        
    if not context.args:
        await update.message.reply_text("Usage: /forwardto @username_or_userid")
        return
        
        target_username = context.args.replace('@', '')
        
        # Get user
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1",
            (target_username,)
        )
        user = cur.fetchone()
        
        if not user:
            await update.message.reply_text(f"❌ User @{target_username} not found.")
            cur.close()
            conn.close()
            return
        
        user_id, first_name = user
        
        # Forward the message
        await update.message.reply_to_message.forward(chat_id=user_id)
        
        await update.message.reply_text(f"✅ Forwarded to @{target_username} ({first_name})")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in forward_to_user: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

        # Updated Admin Help
async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show a detailed list of all admin commands."""
    # 1. एडमिन चेक
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ This is an admin-only command.")
        return
    
    help_text = """
👑 **Admin Commands Guide**

**Media Notifications:**
• `/notify @user [msg]` - Reply to media + send to user
• `/qnotify @user` - Quick notify (auto-detect)
• `/forwardto @user` - Forward channel message
• `/broadcastmedia [msg]` - Broadcast media to all

**Text Notifications:**
• `/notifyuser @user msg` - Send text message
• `/broadcast msg` - Text broadcast to all
• `/schedulenotify <min> @user msg` - Schedule notification

**User Management:**
• `/userinfo @username` - Get user stats
• `/listusers [page]` - List all users

**Movie Management:**
• `/addmovie Title URL` - Add movie
• `/addalias Title alias` - Add alias
• `/notify MovieTitle` - Auto-notify requesters

**Stats & Help:**
• `/stats` - Bot statistics
• `/adminhelp` - This help message

**Examples:**


# --- Main Bot Function ---
def run_bot():
    """Run the Telegram bot"""
    logger.info("Bot is starting...")
    
    if not TELEGRAM_BOT_TOKEN:
        logger.error("No Telegram bot token found. Exiting.")
        return
    
    try:
        setup_database()
    except Exception as e:
        logger.error(f"Database setup failed but continuing: {e}")
        
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()

    # Conversation handler (existing)
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)],
            SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_movies)],
            REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_movie)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        per_message=False,
        per_chat=True,
    )

    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Basic admin commands (existing)
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("bulkadd", bulk_add_movies))
    application.add_handler(CommandHandler("notify", notify_manually))
    application.add_handler(CommandHandler("addalias", add_alias))
    application.add_handler(CommandHandler("aliases", list_aliases))
    application.add_handler(CommandHandler("aliasbulk", bulk_add_aliases))
    
    # NEW: Advanced notification commands
    application.add_handler(CommandHandler("notifyuser", notify_user_by_username))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("schedulenotify", schedule_notification))
    application.add_handler(CommandHandler("notify", notify_user_with_media))
application.add_handler(CommandHandler("qnotify", quick_notify))
application.add_handler(CommandHandler("forwardto", forward_to_user))
application.add_handler(CommandHandler("broadcastmedia", broadcast_with_media))
    
    # NEW: User management commands
    application.add_handler(CommandHandler("userinfo", get_user_info))
    application.add_handler(CommandHandler("listusers", list_all_users))
    
    # NEW: Admin utility commands
    application.add_handler(CommandHandler("adminhelp", admin_help))
    application.add_handler(CommandHandler("stats", get_bot_stats))
    
    application.add_error_handler(error_handler)

    # Signal handling
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        asyncio.create_task(application.stop())
        asyncio.create_task(application.shutdown())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Bot polling started...")
    
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
