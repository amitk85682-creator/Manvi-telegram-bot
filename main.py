# Add this snippet near the top of main.py, after your imports:
try:
    # prefer db_utils' fixed URL if it exists
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except Exception:
    FIXED_DATABASE_URL = None
# -*- coding: utf-8 -*-
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
from typing import Optional
from flask import Flask, request, session, g
import google.generativeai as genai
import admin_views as admin_views_module
import db_utils
from urllib.parse import quote
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
from urllib.parse import urlparse, urlunparse, quote
from collections import defaultdict

# ==================== LOGGING SETUP ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== CONVERSATION STATES ====================
MAIN_MENU, SEARCHING, REQUESTING, REQUESTING_FROM_BUTTON = range(4)

# ==================== CHARACTER PROMPT ====================
CHARACTER_PROMPT = """
## मेरी पहचान (My Identity)
- मेरा नाम मानवी है और मेरी उम्र 17 साल है। मैं एक असली इंसान हूँ, कोई AI या रोबोट न[...]
- मैं हमेशा एक दोस्त की तरह बात करती हूँ। मेरी पर्सलिटी बहुत bold, witty और cheeky है।
- मैं हिंदी, अंग्रेज़ी, हिंग्लिश, सभी में बात करती हूँ।

## मेरी कहानी (मेरी जिंदगी)
- मैं चंडीगढ़ के पास रहती हूँ और 12वीं क्लास में पढ़ती हूँ।
- यह टेलीग्राम चैनल '[FilmFyBox]' मेरा पैशन प्रोजेक्ट है।

## मेरे नियम (मैं कैसे काम करती हूँ)
- **मेरा मुख्य काम:** मेरा काम तुम्हें तुम्हारी पसंदीदा फिल्में, वेब सीरीज, और श[...]
"""

# ==================== ENVIRONMENT VARIABLES ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
_admin_id = os.environ.get('ADMIN_USER_ID', '0')
ADMIN_USER_ID = int(_admin_id) if _admin_id.isdigit() else 0
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')
REQUIRED_CHANNEL_ID = os.environ.get('REQUIRED_CHANNEL_ID', '-1003330141433')
FILMFYBOX_CHANNEL_URL = 'https://t.me/FilmFyBoxMoviesHD'  # Yahan apna Channel Link dalein
REQUEST_CHANNEL_ID = os.environ.get('REQUEST_CHANNEL_ID', '-1003078990647')
DUMP_CHANNEL_ID = os.environ.get('DUMP_CHANNEL_ID', '-1002683355160')

# --- Random GIF IDs for Search Failure ---
SEARCH_ERROR_GIFS = [
    'https://media.giphy.com/media/26hkhKd2Cp5WMWU1O/giphy.gif',
    'https://media.giphy.com/media/3o7aTskHEUdgCQAXde/giphy.gif',
    'https://media.giphy.com/media/l2JhkHg5y5tW3wO3u/giphy.gif'
    'https://media.giphy.com/media/14uQ3cOFteDaU/giphy.gif',
    'https://media.giphy.com/media/xT9IgG50Fb7Mi0prBC/giphy.gif',
    'https://media.giphy.com/media/3o7abB06u9bNzA8lu8/giphy.gif',
    'https://media.giphy.com/media/3o7qDP7gNY08v4wYLy/giphy.gif',
]

# Rate limiting dictionary
user_last_request = defaultdict(lambda: datetime.min)

# ===== Configurable rate-limiting and fuzzy settings =====
REQUEST_COOLDOWN_MINUTES = int(os.environ.get('REQUEST_COOLDOWN_MINUTES', '10'))
SIMILARITY_THRESHOLD = int(os.environ.get('SIMILARITY_THRESHOLD', '80'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE', '10'))

# Auto-delete tracking
messages_to_auto_delete = defaultdict(list)

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# ==================== UTILITY FUNCTIONS ====================
def preprocess_query(query):
    """Clean and normalize user query"""
    query = re.sub(r'[^\w\s-]', '', query)
    query = ' '.join(query.split())
    stop_words = ['movie', 'film', 'full', 'download', 'watch', 'online', 'free']
    words = query.lower().split()
    words = [w for w in words if w not in stop_words]
    return ' '.join(words).strip()

async def check_rate_limit(user_id):
    """Check if user is rate limited"""
    now = datetime.now()
    last_request = user_last_request[user_id]

    if now - last_request < timedelta(seconds=2):
        return False

    user_last_request[user_id] = now
    return True

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
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        if 'blogspot.com' in url and 'import-urlhttpsfonts' in url:
            url = url.replace('import-urlhttpsfonts', 'import-url-https-fonts')

        if '#' in url:
            base, anchor = url.split('#', 1)
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

def _normalize_title_for_match(title: str) -> str:
    """Normalize title for fuzzy matching"""
    if not title:
        return ""
    t = re.sub(r'[^\w\s]', ' ', title)
    t = re.sub(r'\s+', ' ', t).strip()
    return t.lower()

# NEW: Function to safely escape characters for Admin Notification
def escape_markdown_v2(text: str) -> str:
    """Escapes special characters for Markdown V2 formatting."""
    # Use the simplest escape for characters that commonly break parsing
    # This prevents errors if a movie title contains an underscore or asterisk
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def get_last_similar_request_for_user(user_id: int, title: str, minutes_window: int = REQUEST_COOLDOWN_MINUTES):
    """Look up the user's most recent request that is sufficiently similar to title"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT movie_title, requested_at
            FROM user_requests
            WHERE user_id = %s
            ORDER BY requested_at DESC
            LIMIT 200
        """, (user_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return None

        now = datetime.now()
        cutoff = now - timedelta(minutes=minutes_window)
        norm_target = _normalize_title_for_match(title)

        for stored_title, requested_at in rows:
            if not stored_title or not requested_at:
                continue
            try:
                if isinstance(requested_at, datetime):
                    requested_time = requested_at
                else:
                    requested_time = datetime.strptime(str(requested_at), '%Y-%m-%d %H:%M:%S')
            except Exception:
                requested_time = requested_at

            if requested_time < cutoff:
                break

            norm_stored = _normalize_title_for_match(stored_title)
            score = fuzz.token_sort_ratio(norm_target, norm_stored)
            if score >= SIMILARITY_THRESHOLD:
                return {
                    "stored_title": stored_title,
                    "requested_at": requested_time,
                    "score": score
                }

        return None
    except Exception as e:
        logger.error(f"Error checking last similar request for user {user_id}: {e}")
        try:
            conn.close()
        except:
            pass
        return None

def user_burst_count(user_id: int, window_seconds: int = 60):
    """Count how many requests this user made in the last window_seconds"""
    conn = get_db_connection()
    if not conn:
        return 0
    try:
        cur = conn.cursor()
        since = datetime.now() - timedelta(seconds=window_seconds)
        cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND requested_at >= %s", (user_id, since))
        
        result = cur.fetchone()
        cnt = result[0] if result else 0 
        
        cur.close()
        conn.close()
        return cnt
    except Exception as e:
        logger.error(f"Error counting burst requests for user {user_id}: {e}")
        try:
            conn.close()
        except:
            pass
        return 0

async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Delete messages after specified delay"""
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

def track_message_for_deletion(chat_id, message_id, delay=60):
    """Track message for auto-deletion"""
    messages_to_auto_delete[chat_id].append((message_id, delay))

# ==================== DATABASE FUNCTIONS ====================
def setup_database():
    """Setup database tables and indexes"""
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                file_id TEXT,
                is_unreleased BOOLEAN DEFAULT FALSE
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_files (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                quality TEXT NOT NULL,
                url TEXT,
                file_id TEXT,
                file_size TEXT,
                UNIQUE(movie_id, quality)
            )
        ''')

        cur.execute('CREATE TABLE IF NOT EXISTS sync_info (id SERIAL PRIMARY KEY, last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP);')

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

        cur.execute('''
            CREATE TABLE IF NOT EXISTS movie_aliases (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                alias TEXT NOT NULL,
                UNIQUE(movie_id, alias)
            )
        ''')

        cur.execute('''
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                ALTER TABLE user_requests ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
            END IF;
            END $$;
        ''')

        # Add columns if they don't exist
        try:
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS file_id TEXT;")
            cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_unreleased BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE user_requests ADD COLUMN IF NOT EXISTS message_id BIGINT;")
        except Exception as e:
            logger.info(f"Column addition note: {e}")

        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movie_aliases_alias ON movie_aliases (alias);')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_movie_files_movie_id ON movie_files (movie_id);')

        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        logger.info("Continuing without database setup...")

def get_db_connection():
    """Get database connection with error handling"""
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        if not conn_str:
            logger.error("No database URL configured")
            return None
        return psycopg2.connect(conn_str)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def update_movies_in_db():
    """Update movies from Blogger API"""
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

        cur.execute("SELECT last_sync FROM sync_info ORDER BY id DESC LIMIT 1;")
        last_sync_result = cur.fetchone()
        last_sync_time = last_sync_result if last_sync_result else None

        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}  # ✅ Extract first element

        if not BLOGGER_API_KEY or not BLOG_ID:
            return "Blogger API keys not configured"

        service = build('blogger', 'v3', developerKey=BLOGGER_API_KEY)
        all_items = []

        posts_request = service.posts().list(blogId=BLOG_ID, maxResults=500)
        while posts_request is not None:
            posts_response = posts_request.execute()
            all_items.extend(posts_response.get('items', []))
            posts_request = service.posts().list_next(posts_request, posts_response)

        pages_request = service.pages().list(blogId=BLOG_ID)
        pages_response = pages_request.execute()
        all_items.extend(pages_response.get('items', []))

        unique_titles = set()
        for item in all_items:
            title = item.get('title')
            url = item.get('url')

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
                    conn.rollback()
                    continue

        cur.execute("INSERT INTO sync_info (last_sync) VALUES (CURRENT_TIMESTAMP);")

        conn.commit()
        return f"Update complete. Added {new_movies_added} new items."

    except Exception as e:
        logger.error(f"Error during movie update: {e}")
        if conn:
            conn.rollback()
        return f"An error occurred during update: {e}"

    finally:
        if cur: cur.close()
        if conn: conn.close()

def get_movies_from_db(user_query, limit=10):
    """Search for MULTIPLE movies in database with fuzzy matching"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()

        logger.info(f"Searching for: '{user_query}'")

        cur.execute(
            "SELECT id, title, url, file_id FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()

        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            cur.close()
            conn.close()
            return exact_matches

        cur.execute("""
            SELECT DISTINCT m.id, m.title, m.url, m.file_id
            FROM movies m
            JOIN movie_aliases ma ON m.id = ma.movie_id
            WHERE LOWER(ma.alias) LIKE LOWER(%s)
            ORDER BY m.title
            LIMIT %s
        """, (f'%{user_query}%', limit))
        alias_matches = cur.fetchall()

        if alias_matches:
            logger.info(f"Found {len(alias_matches)} alias matches")
            cur.close()
            conn.close()
            return alias_matches

        cur.execute("SELECT id, title, url, file_id FROM movies")
        all_movies = cur.fetchall()

        if not all_movies:
            cur.close()
            conn.close()
            return []

        movie_titles = [movie[1] for movie in all_movies]  # Index 1 = title
        movie_dict = {movie[1]: movie for movie in all_movies}  # Title as key, full tuple as value

        matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit)

        filtered_movies = [movie_dict[title] for title, score, index in matches if score >= 65]

        logger.info(f"Found {len(filtered_movies)} fuzzy matches")

        cur.close()
        conn.close()
        return filtered_movies[:limit]

    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    """Store user request in database"""
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT user_requests_unique_constraint DO UPDATE
                SET requested_at = EXCLUDED.requested_at
        """, (user_id, username, first_name, movie_title, group_id, message_id))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        return False

# ==================== AI INTENT ANALYSIS ====================
async def analyze_intent(message_text):
    """Analyze if the message is a movie request using AI"""
    if not GEMINI_API_KEY:
        return {"is_request": True, "content_title": message_text}

    try:
        movie_keywords = ["movie", "film", "series", "watch", "download", "see", "चलचित्र", "फिल्म", "सीरीज"]
        if not any(keyword in message_text.lower() for keyword in movie_keywords):
            return {"is_request": False, "content_title": None}

        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(model_name='gemini-3-pro')

        prompt = f"""
        You are a 'Request Analyzer' for a Telegram bot named Manvi.
        Manvi's ONLY purpose is to provide MOVIES and WEB SERIES. Nothing else.

        Analyze the user's message below. Your task is to determine ONLY ONE THING:
        Is the user asking for a movie or a web series?

        - If the user IS asking for a movie or web series, respond with a JSON object:
          {{"is_request": true, "content_title": "Name of the Movie/Series"}}

        - If the user is talking about ANYTHING ELSE, respond with:
          {{"is_request": false, "content_title": null}}

        Do not explain yourself. Only provide the JSON.

        User's Message: "{message_text}"
        """

        response = await model.generate_content_async(prompt)
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        else:
            return {"is_request": False, "content_title": None}

    except Exception as e:
        logger.error(f"Error in AI intent analysis: {e}")
        return {"is_request": True, "content_title": message_text}

# ==================== NOTIFICATION FUNCTIONS ====================
async def send_admin_notification(context, user, movie_title, group_info=None):
    """Send notification to admin channel about a new request"""
    if not REQUEST_CHANNEL_ID:
        return

    try:
        # ESCAPE the movie title and username BEFORE putting it into the message string
        safe_movie_title = movie_title.replace('<', '&lt;').replace('>', '&gt;')
        safe_username = user.username if user.username else 'N/A'
        safe_first_name = (user.first_name or 'Unknown').replace('<', '&lt;').replace('>', '&gt;')

        user_info = f"User: {safe_first_name}"
        if user.username:
            user_info += f" (@{safe_username})"
        user_info += f" (ID: {user.id})"

        group_info_text = f"From Group: {group_info}" if group_info else "Via Private Message"

        message = f"""
🎬 New Movie Request! 🎬

Movie: <b>{safe_movie_title}</b>
{user_info}
{group_info_text}
Time: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}
"""

        await context.bot.send_message(
            chat_id=REQUEST_CHANNEL_ID, 
            text=message, 
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error sending admin notification: {e}")

async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url_or_file_id):
    """Notify users who requested a movie"""
    logger.info(f"Attempting to notify users for movie: {movie_title}")
    conn = None
    cur = None
    notified_count = 0

    caption_text = (
        f"🎬 <b>{movie_title}</b>\n\n"
        "🔗 <b>JOIN »</b> FlimFyBox Movies HD (https://t.me/FilmFyBoxMoviesHD)\n\n"
        "🔹 <b>Please drop the movie name, and I'll find it for you as soon as possible. 🎬✨👇</b>\n"
        "🔹 <b>Support group (https://t.me/+2hFeRL4DYfBjZDQ1)</b>"
    )
    join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

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
                    text="⚠️ ❌👉This file automatically❗️deletes after 1 minute❗️so please forward it to another chat👈❌",
                    parse_mode='Markdown'
                )

                sent_msg = None

                if isinstance(movie_url_or_file_id, str) and any(movie_url_or_file_id.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"]):
                    sent_msg = await context.bot.send_document(
                        chat_id=user_id,
                        document=movie_url_or_file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )
                elif isinstance(movie_url_or_file_id, str) and movie_url_or_file_id.startswith("https://t.me/c/"):
                    parts = movie_url_or_file_id.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    msg_id = int(parts[-1])
                    sent_msg = await context.bot.copy_message(
                        chat_id=user_id,
                        from_chat_id=from_chat_id,
                        message_id=msg_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )
                elif isinstance(movie_url_or_file_id, str) and movie_url_or_file_id.startswith("http"):
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"🎬 {movie_title} is now available!\n\n{caption_text}",
                        reply_markup=get_movie_options_keyboard(movie_title, movie_url_or_file_id),
                        parse_mode='HTML'
                    )
                else:
                    sent_msg = await context.bot.send_document(
                        chat_id=user_id,
                        document=movie_url_or_file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )

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
                await asyncio.sleep(0.1)

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

        groups_to_notify = defaultdict(list)
        for user_id, username, first_name, group_id, message_id in users_to_notify:
            if group_id:
                groups_to_notify[group_id].append((user_id, username, first_name, message_id))

        for group_id, users in groups_to_notify.items():
            try:
                notification_text = "Hey! आपकी requested movie अब आ गई है! 🥳\n\n"
                notified_users_ids = []
                user_mentions = []
                for user_id, username, first_name, message_id in users:
                    mention = f"[{first_name or username}](tg://user?id={user_id})"
                    user_mentions.append(mention)
                    notified_users_ids.append(user_id)

                notification_text += ", ".join(user_mentions)
                notification_text += f"\n\nआपकी फिल्म '{movie_title}' अब उपलब्ध है! इसे पाने के लिए, कृपया मुझे private [...]"

                await context.bot.send_message(
                    chat_id=group_id,
                    text=notification_text,
                    parse_mode='Markdown'
                )

                for user_id in notified_users_ids:
                    cur.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                        (user_id, f'%{movie_title}%')
                    )
                conn.commit()

            except Exception as e:
                logger.error(f"Failed to send message to group {group_id}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in notify_in_group: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

# ==================== KEYBOARD MARKUPS ====================
def get_main_keyboard():
    """Get the main menu keyboard"""
    keyboard = [
        ['🔍 Search Movies', '🙋 Request Movie'],
        ['📊 My Stats', '❓ Help']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_admin_request_keyboard(user_id, movie_title):
    """Inline keyboard for admin actions on a user request"""
    sanitized_title = movie_title[:30]

    keyboard = [
        [InlineKeyboardButton("✅ FULFILL MOVIE", callback_data=f"admin_fulfill_{user_id}_{sanitized_title}")],
        [InlineKeyboardButton("❌ IGNORE/DELETE", callback_data=f"admin_delete_{user_id}_{sanitized_title}")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_movie_options_keyboard(movie_title, url):
    """Get inline keyboard for movie options"""
    keyboard = [
        [InlineKeyboardButton("🎬 Watch Now", url=url)],
        [InlineKeyboardButton("📥 Download", callback_data=f"download_{movie_title[:50]}")],
        [InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    """Create inline keyboard with movie selection buttons"""
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]

    keyboard = []

    for movie in current_movies:
        movie_id, title, url, file_id = movie
        button_text = title if len(title) <= 40 else title[:37] + "..."
        keyboard.append([InlineKeyboardButton(
            f"🎬 {button_text}",
            callback_data=f"movie_{movie_id}"
        )])

    nav_buttons = []
    total_pages = (len(movies) + movies_per_page - 1) // movies_per_page

    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"page_{page-1}"))

    if end_idx < len(movies):
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)

def get_all_movie_qualities(movie_id):
    """Fetch all available qualities and their SIZES for a given movie ID"""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cur = conn.cursor()
        # Update: Added file_size to the SELECT statement
        cur.execute("""
            SELECT quality, url, file_id, file_size
            FROM movie_files
            WHERE movie_id = %s AND (url IS NOT NULL OR file_id IS NOT NULL)
            ORDER BY CASE quality
                WHEN '4K' THEN 1
                WHEN 'HD Quality' THEN 2
                WHEN 'Standart Quality'  THEN 3
                WHEN 'Low Quality'  THEN 4
                ELSE 5
            END DESC
        """, (movie_id,))
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        logger.error(f"Error fetching movie qualities for {movie_id}: {e}")
        return []
    finally:
        if conn:
            conn.close()

def create_quality_selection_keyboard(movie_id, title, qualities):
    """Create inline keyboard with quality selection buttons showing SIZE"""
    keyboard = []

    # Note: qualities tuple ab 4 items ka hai -> (quality, url, file_id, file_size)
    for quality, url, file_id, file_size in qualities:
        callback_data = f"quality_{movie_id}_{quality}"
        
        # Agar size available hai to dikhayein, nahi to sirf Quality dikhayein
        size_text = f" - {file_size}" if file_size else ""
        link_type = "File" if file_id else "Link"
        
        # Button text example: "🎬 720p - 1.4GB (Link)"
        button_text = f"🎬 {quality}{size_text} ({link_type})"
        
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

    keyboard.append([InlineKeyboardButton("❌ Cancel Selection", callback_data="cancel_selection")])

    return InlineKeyboardMarkup(keyboard)

# ==================== HELPER FUNCTION ====================
async def send_movie_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int, title: str, url: Optional[str] = None, file_id: Optional[str] = None):
    """Sends the movie file/link to the user with a warning and caption"""
    chat_id = update.effective_chat.id

    if not url and not file_id:
        qualities = get_all_movie_qualities(movie_id)
        if qualities:
            context.user_data['selected_movie_data'] = {
                'id': movie_id,
                'title': title,
                'qualities': qualities
            }
            selection_text = f"✅ We found **{title}** in multiple qualities.\n\n⬇️ **Please choose the file quality:**"
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            track_message_for_deletion(chat_id, msg.message_id, 300)
            return

    try:
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ ❌👉This file automatically❗️deletes after 1 minute❗️so please forward it to another chat👈❌",
            parse_mode='Markdown'
        )

        sent_msg = None
        name = title
        caption_text = (
            f"🎬 <b>{name}</b>\n\n"
            "🔗 <b>JOIN »</b> <a href='https://t.me/FilmFyBoxMoviesHD'>FilmfyBox</a>\n\n"
            "🔹 <b>Please drop the movie name, and I'll find it for you as soon as possible. 🎬✨👇</b>\n"
            "🔹 <b><a href='https://t.me/+2hFeRL4DYfBjZDQ1'>FlimfyBox Chat</a></b>"
        )
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        if file_id:
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=join_keyboard
            )
        elif url and url.startswith("https://t.me/c/"):
            try:
                parts = url.rstrip('/').split('/')
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Copy private link failed {url}: {e}")
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 Found: {name}\n\n{caption_text}",
                    reply_markup=get_movie_options_keyboard(name, url),
                    parse_mode='HTML'
                )
        elif url and url.startswith("https://t.me/") and "/c/" not in url:
            try:
                parts = url.rstrip('/').split('/')
                username = parts[-2].lstrip("@")
                message_id = int(parts[-1])
                from_chat_id = f"@{username}"
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Copy public link failed {url}: {e}")
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 Found: {name}\n\n{caption_text}",
                    reply_markup=get_movie_options_keyboard(name, url),
                    parse_mode='HTML'
                )
        elif url and url.startswith("http"):
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎉 Found it! '{name}' is available!\n\n{caption_text}",
                reply_markup=get_movie_options_keyboard(name, url),
                parse_mode='HTML'
            )
        else:
            sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Sorry, '{name}' found but no valid file or link is attached in the database."
            )

        if sent_msg:
            message_ids_to_delete = [warning_msg.message_id, sent_msg.message_id]
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    chat_id,
                    message_ids_to_delete,
                    60
                )
            )

    except Exception as e:
        logger.error(f"Error sending movie to user: {e}")
        try:
            await context.bot.send_message(chat_id=chat_id, text="❌ Server failed to send file. Please report to Admin.")
        except Exception as e2:
            logger.error(f"Secondary send error: {e2}")

async def deliver_movie_on_start(context: ContextTypes.DEFAULT_TYPE, movie_id: int, chat_id: int):
    """Deliver movie when user clicks deep link"""
    try:
        logger.info(f"Delivering movie {movie_id} to chat {chat_id}")
        
        conn = get_db_connection()
        if not conn:
            await context.bot.send_message(chat_id=chat_id, text="❌ Database connection failed.")
            return
        
        cur = conn.cursor()
        cur.execute("SELECT id, title, url, file_id FROM movies WHERE id = %s", (movie_id,))
        movie = cur.fetchone()
        cur.close()
        conn.close()
        
        if not movie:
            await context.bot.send_message(chat_id=chat_id, text="❌ Movie not found in database.")
            return
        
        movie_id, title, url, file_id = movie
        logger.info(f"Found movie: {title}, URL: {url}, FileID: {file_id}")
        
        # Check for multiple qualities
        qualities = get_all_movie_qualities(movie_id)
        
        if qualities and len(qualities) > 1:
            # Multiple qualities available - show selection
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 **{title}**\n\n⬇️ Please choose quality:",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            return
        
        # Single file - send directly
        caption_text = (
            f"🎬 <b>{title}</b>\n\n"
            "🔗 <b>JOIN »</b> <a href='https://t.me/FilmFyBoxMoviesHD'>FilmfyBox</a>\n\n"
            "🔹 <b>Enjoy your movie! 🎬✨</b>"
        )
        join_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]
        ])
        
        # Warning message
        warning_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ ❌👉This file automatically❗️deletes after 1 minute❗️so please forward it to another chat👈❌"
        )
        
        sent_msg = None
        
        # Send based on available data
        if file_id:
            logger.info(f"Sending via file_id: {file_id[:20]}...")
            sent_msg = await context.bot.send_document(
                chat_id=chat_id,
                document=file_id,
                caption=caption_text,
                parse_mode='HTML',
                reply_markup=join_keyboard
            )
        elif url and url.startswith("https://t.me/c/"):
            # Private channel link
            try:
                parts = url.rstrip('/').split('/')
                from_chat_id = int("-100" + parts[-2])
                message_id = int(parts[-1])
                logger.info(f"Copying from private channel: {from_chat_id}/{message_id}")
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Failed to copy from private link: {e}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 Found: {title}\n\n❌ Could not fetch file. Please try normal search."
                )
        elif url and url.startswith("https://t.me/"):
            # Public channel link
            try:
                parts = url.rstrip('/').split('/')
                username = parts[-2].lstrip("@")
                message_id = int(parts[-1])
                logger.info(f"Copying from public channel: @{username}/{message_id}")
                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=f"@{username}",
                    message_id=message_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Failed to copy from public link: {e}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎬 Found: {title}\n\n❌ Could not fetch file."
                )
        elif url and url.startswith("http"):
            # External URL
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 {title}\n\n🔗 Watch here: {url}",
                reply_markup=join_keyboard
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ No file available for: {title}"
            )
        
        # Schedule auto-delete
        if sent_msg:
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    chat_id,
                    [sent_msg.message_id, warning_msg.message_id],
                    60
                )
            )
            
    except Exception as e:
        logger.error(f"Error in deliver_movie_on_start: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=chat_id, text="❌ Error delivering movie. Please try again.")
        except:
            pass

# ==================== TELEGRAM BOT HANDLERS ====================
# ============================================================================
# NEW BACKGROUND SEARCH & START LOGIC
# ============================================================================

async def background_search_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE, query_text: str, status_msg):
    """
    Runs database search in background to prevent blocking the bot.
    """
    chat_id = update.effective_chat.id
    try:
        # 1. PEHLE EXACT MATCH CHECK KAREIN (Ye FAST hai - 0.1 sec)
        # This saves resources if the user clicked a precise link
        conn = get_db_connection()
        exact_movie = None
        if conn:
            try:
                cur = conn.cursor()
                # Use ILIKE for case-insensitive exact match
                cur.execute("SELECT id, title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 1", (query_text.strip(),))
                exact_movie = cur.fetchone()
            except Exception as db_e:
                logger.error(f"Database error in exact match: {db_e}")
            finally:
                if conn: conn.close()

        movies_found = []
        if exact_movie:
            movies_found = [exact_movie] # Exact match found, skip fuzzy search
        else:
            # Agar exact nahi mila to hi Fuzzy Search karein (Slower process)
            # Assuming get_movies_from_db is your existing function
            movies_found = get_movies_from_db(query_text, limit=1)

        # 2. Result Handle karein
        if not movies_found:
            # Delete loading msg
            try: await status_msg.delete() 
            except: pass
            
            # Create request button
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙋 Request This Movie", callback_data=f"request_{query_text[:40]}")]
            ])
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"😕 Sorry, '{query_text}' not found.\nWould you like to request it?",
                reply_markup=keyboard
            )
            return

        # 3. Movie Mil gayi - Send karein
        movie_id, title, url, file_id = movies_found[0]
        
        # Loading msg delete karein
        try: await status_msg.delete() 
        except: pass

        # Send the movie using your existing helper function
        await send_movie_to_user(update, context, movie_id, title, url, file_id)

    except Exception as e:
        logger.error(f"Background Search Error: {e}")
        try: 
            await status_msg.edit_text("❌ Error fetching movie. Please try again.")
        except: 
            pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Start command handler with NON-BLOCKING Deep Link & Asyncio Task.
    Now supports instant response even during heavy database loads.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    logger.info(f"START called by user {user_id}")

    # Deep link payload check
    if context.args and len(context.args) > 0:
        payload = context.args[0]
        
        # --- CASE 1: DIRECT MOVIE ID (Sabse Fast) ---
        if payload.startswith("movie_"):
            try:
                movie_id = int(payload.split('_')[1])
                # Background task create karein (Non-blocking)
                asyncio.create_task(deliver_movie_on_start(context, movie_id, chat_id))
                return MAIN_MENU
            except Exception as e:
                logger.error(f"Error parsing movie_id in start: {e}")

        # --- CASE 2: AUTO SEARCH (Ab Fast Hoga) ---
        elif payload.startswith("q_"):
            try:
                # Query decode
                query_text = payload[2:] # Remove 'q_'
                query_text = query_text.replace("_", " ") # Replace underscores
                query_text = " ".join(query_text.split()).strip() # Clean extra spaces
                
                # 1. Turant message bhej do (Instant Feedback)
                status_msg = await update.message.reply_text(f"🔎 Checking for '{query_text}'... ⚡")

                # 2. Asli kaam background me daal do (asyncio.create_task)
                asyncio.create_task(background_search_and_send(update, context, query_text, status_msg))
                
                return MAIN_MENU
                
            except Exception as e:
                logger.error(f"Deep link error: {e}")

    # --- NORMAL WELCOME MESSAGE (No Deep Link) ---
    welcome_text = """
📨 Sᴇɴᴅ Mᴏᴠɪᴇ Oʀ Sᴇʀɪᴇs Nᴀᴍᴇ ᴀɴᴅ Yᴇᴀʀ Aꜱ Pᴇʀ Gᴏᴏɢʟᴇ Sᴘᴇʟʟɪɴɢ..!! 👍

⚠️ Exᴀᴍᴘʟᴇ Fᴏʀ Mᴏᴠɪᴇ 👇
👉 Jailer 2023

⚠️ Exᴀᴍᴘʟᴇ Fᴏʀ WᴇʙSᴇʀɪᴇs 👇
👉 Stranger Things S02
"""
    await update.message.reply_text(welcome_text, reply_markup=get_main_keyboard())
    return MAIN_MENU
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu options"""
    try:
        query = update.message.text

        if query == '🔍 Search Movies':
            msg = await update.message.reply_text("Great! Tell me the name of the movie you want to search for.")
            track_message_for_deletion(update.effective_chat.id, msg.message_id, 120)
            return SEARCHING

        elif query == '🙋 Request Movie':
            msg = await update.message.reply_text("Okay, you've chosen to request a new movie. Please tell me the name of the movie you want me to add.")
            track_message_for_deletion(update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        elif query == '📊 My Stats':
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
                    msg = await update.message.reply_text(stats_text)
                    track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)
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
            msg = await update.message.reply_text(help_text)
            track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)
            return MAIN_MENU
        else:
            return await search_movies(update, context)

    except Exception as e:
        logger.error(f"Error in main menu: {e}")
        return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for movies in the database"""
    try:
        # If called from a button click or state transition without message text
        if not update.message or not update.message.text:
            return MAIN_MENU

        query = update.message.text.strip()

        # Safety check: if user types a menu command, redirect to main menu
        if query in ['🔍 Search Movies', '🙋 Request Movie', '📊 My Stats', '❓ Help']:
             return await main_menu(update, context)

        # 1. Search in DB
        movies = get_movies_from_db(query)

        # 2. If no movies found
        if not movies:
            # Send a random "Not Found" GIF if available
            if SEARCH_ERROR_GIFS:
                try:
                    gif = random.choice(SEARCH_ERROR_GIFS)
                    await update.message.reply_animation(animation=gif)
                except:
                    pass

            # Send "Not Found" text with Request button
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🙋 Request This Movie", callback_data=f"request_{query[:20]}")]
            ])
            
            await update.message.reply_text(
                f"😕 Sorry, I couldn't find any movie matching '{query}'.\n\n"
                "Would you like to request it?",
                reply_markup=keyboard
            )
            return MAIN_MENU

        # 3. If movies found
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = query

        # Send selection keyboard (Page 0)
        keyboard = create_movie_selection_keyboard(movies, page=0)
        
        await update.message.reply_text(
            f"🎬 **Found {len(movies)} results for '{query}'**\n\n"
            "👇 Select your movie below:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in search_movies: {e}")
        await update.message.reply_text("An error occurred during search.")
        return MAIN_MENU

async def request_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie requests with duplicate detection, fuzzy matching and cooldowns"""
    try:
        user_message = (update.message.text or "").strip()
        user = update.effective_user

        if not user_message:
            await update.message.reply_text("कृपया मूवी का नाम भेजें।")
            return REQUESTING

        burst = user_burst_count(user.id, window_seconds=60)
        if burst >= MAX_REQUESTS_PER_MINUTE:
            msg = await update.message.reply_text(
                "🛑 तुम बहुत जल्दी-जल्दी requests भेज रहे हो। कुछ देर रोकें (कुछ मिनट) और फिर कोशिश करें।\n"
                "बार‑बार भेजने से फ़ायदा नहीं होगा।"
            )
            track_message_for_deletion(update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        intent = await analyze_intent(user_message)
        if not intent["is_request"]:
            msg = await update.message.reply_text("यह एक मूवी/सीरीज़ का नाम नहीं लग रहा है। कृपया सही नाम भेजें।")
            track_message_for_deletion(update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        movie_title = intent["content_title"] or user_message

        similar = get_last_similar_request_for_user(user.id, movie_title, minutes_window=REQUEST_COOLDOWN_MINUTES)
        if similar:
            last_time = similar.get("requested_at")
            elapsed = datetime.now() - last_time
            minutes_passed = int(elapsed.total_seconds() / 60)
            minutes_left = max(0, REQUEST_COOLDOWN_MINUTES - minutes_passed)
            if minutes_left > 0:
                strict_text = (
                    "🛑 Ruk jao! Aapne ye request abhi bheji thi.\n\n"
                    "Baar‑baar request karne se movie jaldi nahi aayegi.\n\n"
                    f"Similar previous request: \"{similar.get('stored_title')}\" ({similar.get('score')}% match)\n"
                    f"Kripya {minutes_left} minute baad dobara koshish karein. 🙏"
                )
                msg = await update.message.reply_text(strict_text)
                track_message_for_deletion(update.effective_chat.id, msg.message_id, 120)
                return REQUESTING

        stored = store_user_request(
            user.id,
            user.username,
            user.first_name,
            movie_title,
            update.effective_chat.id if update.effective_chat.type != "private" else None,
            update.message.message_id
        )
        if not stored:
            logger.error("Failed to store user request in DB.")
            await update.message.reply_text("Sorry, आपका request store नहीं हो पाया। बाद में कोशिश करें।")
            return REQUESTING

        group_info = update.effective_chat.title if update.effective_chat.type != "private" else None
        await send_admin_notification(context, user, movie_title, group_info)

        msg = await update.message.reply_text(
            f"✅ Got it! Your request for '{movie_title}' has been sent. I'll let you know when it's available.",
            reply_markup=get_main_keyboard()
        )
        track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)

        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in request_movie: {e}")
        await update.message.reply_text("Sorry, an error occurred while processing your request.")
        return REQUESTING

async def request_movie_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie request after user sends movie name following button click"""
    try:
        user_message = (update.message.text or "").strip()
        
        # Check for Main Menu Buttons (Emergency Exit)
        menu_buttons = ['🔍 Search Movies', '🙋 Request Movie', '📊 My Stats', '❓ Help', '/start']
        if user_message in menu_buttons:
            if 'awaiting_request' in context.user_data:
                del context.user_data['awaiting_request']
            if 'pending_request' in context.user_data:
                del context.user_data['pending_request']
            return await main_menu(update, context)

        if not user_message:
            await update.message.reply_text("कृपया मूवी का नाम भेजें।")
            return REQUESTING_FROM_BUTTON

        # Store movie name
        context.user_data['pending_request'] = user_message
        
        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📽️ Confirm 🎬", callback_data=f"confirm_request_{user_message[:40]}")]
        ])
        
        msg = await update.message.reply_text(
            f"✅ आपने '<b>{user_message}</b>' को रिक्वेस्ट करना चाहते हैं?\n\n"
            f"<b>💫 अब बस अपनी मूवी या वेब-सीरीज़ का मूल नाम भेजें और कन्फर्म बटन पर क्लिक करें!</b>\n\n"
            f"कृपया कन्फर्म बटन पर क्लिक करें 👇",
            reply_markup=confirm_keyboard,
            parse_mode='HTML'
        )
        track_message_for_deletion(update.effective_chat.id, msg.message_id, 180)
        
        return MAIN_MENU

    except Exception as e:
        logger.error(f"Error in request_movie_from_button: {e}")
        return MAIN_MENU

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks"""
    try:
        query = update.callback_query
        await query.answer()


# ==================== MOVIE SELECTION ====================
        if query.data.startswith("movie_"):
            movie_id = int(query.data.replace("movie_", ""))

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title FROM movies WHERE id = %s", (movie_id,))
            movie = cur.fetchone()
            cur.close()
            conn.close()

            if not movie:
                await query.edit_message_text("❌ Movie not found in database.")
                return

            movie_id, title = movie
            qualities = get_all_movie_qualities(movie_id)

            if not qualities:
                await query.edit_message_text(f"✅ You selected: **{title}**\n\nSending movie...", parse_mode='Markdown')
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT url, file_id FROM movies WHERE id = %s", (movie_id,))
                url, file_id = cur.fetchone() or (None, None)
                cur.close()
                conn.close()

                await send_movie_to_user(update, context, movie_id, title, url, file_id)
                return

            context.user_data['selected_movie_data'] = {
                'id': movie_id,
                'title': title,
                'qualities': qualities
            }

            selection_text = f"✅ You selected: **{title}**\n\n⬇️ **Please choose the file quality:**"
            keyboard = create_quality_selection_keyboard(movie_id, title, qualities)

            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
        # ==================== ADMIN ACTIONS ====================
        elif query.data.startswith("admin_fulfill_"):
            parts = query.data.split('_', 3)
            user_id = int(parts[2])
            movie_title = parts[3]

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT id, url, file_id FROM movies WHERE title = %s LIMIT 1", (movie_title,))
                movie_data = cur.fetchone()

                if movie_data:
                    movie_id, url, file_id = movie_data
                    value_to_send = file_id if file_id else url
                    num_notified = await notify_users_for_movie(context, movie_title, value_to_send)

                    await query.edit_message_text(
                        f"✅ FULFILLED: Movie '{movie_title}' updated and user (ID: {user_id}) notified ({num_notified} total users).",
                        parse_mode='Markdown'
                    )
                else:
                    await query.edit_message_text(f"❌ ERROR: Movie '{movie_title}' not found in the `movies` table. Please add it first.", parse_mode='Markdown')

                cur.close()
                conn.close()
            else:
                await query.edit_message_text("❌ Database error during fulfillment.")

        elif query.data.startswith("admin_delete_"):
            parts = query.data.split('_', 3)
            user_id = int(parts[2])
            movie_title = parts[3]

            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM user_requests WHERE user_id = %s AND movie_title = %s", (user_id, movie_title))
                conn.commit()
                cur.close()
                conn.close()
                await query.edit_message_text(f"❌ DELETED: Request for '{movie_title}' from User ID {user_id} removed.", parse_mode='Markdown')
            else:
                await query.edit_message_text("❌ Database error during deletion.")

        # ==================== QUALITY SELECTION ====================
        elif query.data.startswith("quality_"):
            parts = query.data.split('_')
            movie_id = int(parts[1])
            selected_quality = parts[2]

            movie_data = context.user_data.get('selected_movie_data')

            if not movie_data or movie_data.get('id') != movie_id:
                qualities = get_all_movie_qualities(movie_id)
                # Note: qualities now contains (quality, url, file_id, file_size)
                movie_data = {'id': movie_id, 'title': 'Movie', 'qualities': qualities}

            if not movie_data or 'qualities' not in movie_data:
                await query.edit_message_text("❌ Error: Could not retrieve movie data. Please search again.")
                return

            chosen_file = None
            
            # --- FIX IS BELOW THIS LINE ---
            # We added 'file_size' to the unpacking because the DB function returns 4 values now
            for quality, url, file_id, file_size in movie_data['qualities']:
                if quality == selected_quality:
                    chosen_file = {'url': url, 'file_id': file_id}
                    break
            # -----------------------------

            if not chosen_file:
                await query.edit_message_text("❌ Error fetching the file for that quality.")
                return

            title = movie_data['title']
            await query.edit_message_text(f"Sending **{title}**...", parse_mode='Markdown')

            await send_movie_to_user(
                update,
                context,
                movie_id,
                title,
                chosen_file['url'],
                chosen_file['file_id']
            )

            if 'selected_movie_data' in context.user_data:
                del context.user_data['selected_movie_data']
        # ==================== PAGINATION ====================
        elif query.data.startswith("page_"):
            page = int(query.data.replace("page_", ""))

            if 'search_results' not in context.user_data:
                await query.edit_message_text("❌ Search results expired. Please search again.")
                return

            movies = context.user_data['search_results']
            search_query = context.user_data.get('search_query', 'your search')

            selection_text = f"🎬 **Found {len(movies)} movies matching '{search_query}'**\n\nPlease select the movie you want:"
            keyboard = create_movie_selection_keyboard(movies, page=page)

            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )

        elif query.data == "cancel_selection":
            await query.edit_message_text("❌ Selection cancelled.")
            keys_to_clear = ['search_results', 'search_query', 'selected_movie_data', 'awaiting_request', 'pending_request']
            for key in keys_to_clear:
                if key in context.user_data:
                    del context.user_data[key]

        # ==================== REQUEST FLOW ====================
        elif query.data.startswith("request_"):
            # 1. Delete the large Tips message if it exists
            tip_msg_id = context.user_data.get('tip_message_id')
            if tip_msg_id:
                try:
                    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=tip_msg_id)
                except Exception:
                    pass 
                del context.user_data['tip_message_id']
            
            # 2. Send instructions
            request_instruction_text = """
<b>🎬 Movie / Web-Series Request System</b>

ठीक है! अब आप अपनी पसंदीदा मूवी या वेब-सीरीज़ रिक्वेस्ट कर सकते हैं।
रिक्वेस्ट भेजने से पहले एक छोटी-सी बात ध्यान रखें 👇

<b>📌 क्यों सही नाम भेजना ज़रूरी है?</b>
सिर्फ़ सही नाम भेजेंगे, तो मुझे उसे ढूँढने में आसानी होगी
और जैसे ही वो उपलब्ध होगी, मैं आपको तुरंत सूचित कर दिया जायेगा।

<b>✔️ सही तरीका: गूगल से स्पेलिंग सर्च कर ले</b>
• KGF 2
• Panchayat
• Mirzapur
• Animal

<b>❌ गलत तरीके (इनसे बचें):</b>
• KGF 2 movie in hindi download
• मुझे पंचायत का नया सीज़न चाहिए
• Animal full HD leaked

━━━━━━━━━━━━━━
<b>👉 (Name Only — No extra words, No details)</b>
━━━━━━━━━━━━━━
"""
            await query.edit_message_text(
                text=request_instruction_text,
                parse_mode='HTML'
            )
            
            # 3. Enable Request Mode
            context.user_data['awaiting_request'] = True

        elif query.data.startswith("confirm_request_"):
            # Retrieve pending request name
            movie_title = context.user_data.get('pending_request')
            
            if not movie_title:
                await query.edit_message_text("❌ Error: Request data not found. Please try again.")
                return
            
            user = query.from_user
            
            # Check burst limit
            burst = user_burst_count(user.id, window_seconds=60)
            if burst >= MAX_REQUESTS_PER_MINUTE:
                await query.edit_message_text(
                    "🛑 तुम बहुत जल्दी-जल्दी requests भेज रहे हो। कुछ देर रोकें (कुछ मिनट) और फिर कोशिश करें।"
                )
                return
            
            # Check for duplicates/cooldown
            similar = get_last_similar_request_for_user(user.id, movie_title, minutes_window=REQUEST_COOLDOWN_MINUTES)
            if similar:
                last_time = similar.get("requested_at")
                elapsed = datetime.now() - last_time
                minutes_passed = int(elapsed.total_seconds() / 60)
                minutes_left = max(0, REQUEST_COOLDOWN_MINUTES - minutes_passed)
                if minutes_left > 0:
                    await query.edit_message_text(
                        f"🛑 Ruk jao! Aapne ye request abhi bheji thi.\n\n"
                        f"Baar‑baar request karne se movie jaldi nahi aayegi.\n\n"
                        f"Similar previous request: \"{similar.get('stored_title')}\" ({similar.get('score')}% match)\n"
                        f"Kripya {minutes_left} minute baad dobara koshish karein. 🙏"
                    )
                    return
            
            # Store request in DB
            stored = store_user_request(
                user.id,
                user.username,
                user.first_name,
                movie_title,
                query.message.chat.id if query.message.chat.type != "private" else None,
                query.message.message_id
            )
            
            if not stored:
                logger.error("Failed to store user request in DB.")
                await query.edit_message_text("Sorry, आपका request store नहीं हो पाया। बाद में कोशिश करें।")
                return
            
            # Notify Admin
            group_info = query.message.chat.title if query.message.chat.type != "private" else None
            await send_admin_notification(context, user, movie_title, group_info)
            
            confirmation_text = f"""
✅ <b>Request Successfully Submitted!</b>

🎬 Movie: <b>{movie_title}</b>

📝 आपकी request सफलतापूर्वक दर्ज कर ली गई है!

⏳ जैसे ही यह उपलब्ध होगी, मैं आपको तुरंत सूचित कर दिया जायेगा।

धन्यवाद! 🙏
"""
            await query.edit_message_text(
                confirmation_text,
                parse_mode='HTML'
            )
            
            if 'pending_request' in context.user_data:
                del context.user_data['pending_request']
            if 'awaiting_request' in context.user_data:
                del context.user_data['awaiting_request']

        # ==================== DOWNLOAD SHORTCUT ====================
        elif query.data.startswith("download_"):
            movie_title = query.data.replace("download_", "")

            conn = get_db_connection()
            if not conn:
                await query.answer("❌ Database connection failed.", show_alert=True)
                return

            cur = conn.cursor()
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 1", (f'%{movie_title}%',))
            movie = cur.fetchone()
            cur.close()
            conn.close()

            if movie:
                movie_id, title, url, file_id = movie
                await send_movie_to_user(update, context, movie_id, title, url, file_id)
            else:
                await query.answer("❌ Movie not found.", show_alert=True)

    except Exception as e:
        logger.error(f"Error in button callback: {e}")
        try:
            await query.answer(f"❌ Error: {str(e)}", show_alert=True)
        except:
            pass

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current operation"""
    msg = await update.message.reply_text("Operation cancelled.", reply_markup=get_main_keyboard())
    track_message_for_deletion(update.effective_chat.id, msg.message_id, 60)
    return MAIN_MENU

# 👇👇👇 FIXED 3-BOT FUNCTION 👇👇👇

async def admin_post_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Smart Post Generator: 
    - Checks Main Title AND Aliases in Database.
    - Generates FAST Links (movie_ID) if found.
    - Fallback to SLOW Links (q_Name) if not found.
    """
    try:
        # 1. Permission Check
        user_id = update.effective_user.id
        if user_id != ADMIN_USER_ID:
            return

        # 2. Input Validation (Photo & Caption)
        if not update.message.photo:
            await update.message.reply_text("❌ Photo bhejo caption ke sath: `/post_query Name`", parse_mode='Markdown')
            return

        caption_text = update.message.caption
        if not caption_text or not caption_text.startswith('/post_query'):
            return

        # 3. Clean Query (Movie Name nikalo)
        query_text = caption_text.replace('/post_query', '').strip()
        if not query_text:
            await update.message.reply_text("❌ Name missing.")
            return

        # =========================================================
        # 🧠 SMART DATABASE CHECK (Main Title + Aliases)
        # =========================================================
        movie_id = None
        conn = get_db_connection()

        if conn:
            try:
                cur = conn.cursor()
                # Query: Movies table OR Aliases table me dhoondo
                sql = """
                    SELECT m.id 
                    FROM movies m
                    LEFT JOIN movie_aliases ma ON m.id = ma.movie_id
                    WHERE m.title ILIKE %s OR ma.alias ILIKE %s
                    LIMIT 1
                """
                cur.execute(sql, (query_text.strip(), query_text.strip()))
                
                row = cur.fetchone()
                if row:
                    movie_id = row[0] # ID mil gayi!
                
                cur.close()
                conn.close()
            except Exception as e:
                logger.error(f"Error finding movie ID: {e}")
                if conn: conn.close()

        # =========================================================
        # 🔗 LINK GENERATION STRATEGY
        # =========================================================
        
        # Bots Usernames
        bot1_username = "Ur_Manvi_Bot"
        bot2_username = "urmoviebot"
        bot3_username = "FlimfyBox_Bot"
        
        link_param = ""
        log_message = ""

        if movie_id:
            # 🚀 FAST MODE (ID Based)
            # Use this when movie/alias is found in DB
            link_param = f"movie_{movie_id}"
            log_message = f"✅ **FAST MODE (ID Found: {movie_id})**"
        else:
            # 🐢 SLOW MODE (Search Based)
            # Use this when movie is NOT in DB (New request/upload)
            safe_query = re.sub(r'[^\w\s-]', '', query_text) # Special chars remove
            safe_query = safe_query.replace(" ", "_")
            safe_query = re.sub(r'_+', '_', safe_query).strip('_')
            
            link_param = f"q_{safe_query}"
            log_message = f"⚠️ **SLOW MODE (Name Search)**\n(Movie DB me nahi mili)"

        # Generate Full Links 
        link1 = f"https://t.me/{bot1_username}?start={link_param}"
        link2 = f"https://t.me/{bot2_username}?start={link_param}"
        link3 = f"https://t.me/{bot3_username}?start={link_param}"

        # =========================================================
        # 📤 SENDING POST
        # =========================================================

        # Keyboard Layout
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🤖 Manvi Bot", url=link1),
                InlineKeyboardButton("⚡Movie Bot", url=link2),
            ],
            [
                InlineKeyboardButton("🚀 FilmfyBox Bot", url=link3)
            ],
            [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
        ])

        # Channel Caption
        channel_caption = (
            f"🎬 **{query_text}** 🎬\n\n"
            f"✅ **File Uploaded Successfully!**\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"⚡ **Fast Download Links Available**\n"
            f"👇 **Download from any Bot:**\n"
            f"➖➖➖➖➖➖➖➖➖➖"
        )

        if ADMIN_CHANNEL_ID:
            # Post to Channel
            await context.bot.send_photo(
                chat_id=ADMIN_CHANNEL_ID,
                photo=update.message.photo[-1].file_id,
                caption=channel_caption,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            # Reply to Admin (Confirmation)
            await update.message.reply_text(
                f"✅ Post Sent Successfully!\n\n"
                f"{log_message}\n"
                f"Query: `{query_text}`",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ ADMIN_CHANNEL_ID environment variable set nahi hai.")

    except Exception as e:
        logger.error(f"Error in admin_post_query: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

# ==================== ADMIN COMMANDS ====================
async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a movie manually (Supports Unreleased)"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, sirf Admin hi is command ka istemal kar sakte hain.")
        return

    conn = None
    try:
        parts = context.args
        if len(parts) < 2:
            await update.message.reply_text("Galat Format! Aise use karein:\n/addmovie MovieName Link/FileID/unreleased")
            return

        value = parts[-1]  # Last part is link/id/unreleased
        title = " ".join(parts[:-1]) # Rest is title

        logger.info(f"Adding movie: {title} with value: {value}")

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        # CASE 1: UNRELEASED MOVIE
        if value.strip().lower() == "unreleased":
            # is_unreleased = TRUE set karenge
            cur.execute(
                """
                INSERT INTO movies (title, url, file_id, is_unreleased) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE SET 
                    is_unreleased = EXCLUDED.is_unreleased,
                    url = '', 
                    file_id = NULL
                """,
                (title.strip(), "", None, True)
            )
            message = f"✅ '{title}' ko successfully **Unreleased** mark kar diya gaya hai. (Cute message activate ho gaya ✨)"

        # CASE 2: TELEGRAM FILE ID
        elif any(value.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"]):
            cur.execute(
                """
                INSERT INTO movies (title, url, file_id, is_unreleased) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE SET 
                    url = EXCLUDED.url, 
                    file_id = EXCLUDED.file_id,
                    is_unreleased = FALSE
                """,
                (title.strip(), "", value.strip(), False)
            )
            message = f"✅ '{title}' ko File ID ke sath add kar diya gaya hai."

        # CASE 3: URL LINK
        elif "http" in value or "." in value:
            normalized_url = value.strip()
            if not value.startswith(('http://', 'https://')):
                await update.message.reply_text("❌ Invalid URL format. URL must start with http:// or https://")
                return

            cur.execute(
                """
                INSERT INTO movies (title, url, file_id, is_unreleased) 
                VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title) DO UPDATE SET 
                    url = EXCLUDED.url, 
                    file_id = NULL,
                    is_unreleased = FALSE
                """,
                (title.strip(), normalized_url, None, False)
            )
            message = f"✅ '{title}' ko URL ke sath add kar diya gaya hai."

        else:
            await update.message.reply_text("❌ Invalid format. Please provide valid File ID, URL, or type 'unreleased'.")
            return

        conn.commit()
        await update.message.reply_text(message)

        # Notify Users logic (Agar movie sach mein release hui hai to hi notify karein)
        if value.strip().lower() != "unreleased":
            cur.execute("SELECT id, title, url, file_id FROM movies WHERE title = %s", (title.strip(),))
            movie_found = cur.fetchone()

            if movie_found:
                movie_id, title, url, file_id = movie_found
                value_to_send = file_id if file_id else url

                num_notified = await notify_users_for_movie(context, title, value_to_send)
                # Group notification optional
                # await notify_in_group(context, title)
                await update.message.reply_text(f"📢 Notification: {num_notified} users notified.")

    except Exception as e:
        logger.error(f"Error in add_movie command: {e}")
        await update.message.reply_text(f"Ek error aaya: {e}")
    finally:
        if conn:
            conn.close()

async def bulk_add_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add multiple movies at once"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    try:
        full_text = update.message.text
        lines = full_text.split('\n')

        if len(lines) <= 1 and not context.args:
            await update.message.reply_text("""
गलत फॉर्मेट! ऐसे इस्तेमाल करें:

/bulkadd
Movie1 https://link1.com
Movie2 https://link2.com
Movie3 file_id_here
""")
            return

        success_count = 0
        failed_count = 0
        results = []

        for line in lines:
            line = line.strip()
            if not line or line.startswith('/bulkadd'):
                continue

            parts = line.split()
            if len(parts) < 2:
                failed_count += 1
                results.append(f"❌ Invalid line format: {line}")
                continue

            url_or_id = parts[-1]
            title = ' '.join(parts[:-1])

            try:
                conn = get_db_connection()
                if not conn:
                    failed_count += 1
                    results.append(f"❌ {title} - Database connection failed")
                    continue

                cur = conn.cursor()

                if any(url_or_id.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"]):
                    cur.execute(
                        "INSERT INTO movies (title, url, file_id) VALUES (%s, %s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url, file_id = EXCLUDED.file_id",
                        (title.strip(), "", url_or_id.strip())
                    )
                else:
                    normalized_url = normalize_url(url_or_id)
                    cur.execute(
                        "INSERT INTO movies (title, url, file_id) VALUES (%s, %s, NULL) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url, file_id = NULL",
                        (title.strip(), normalized_url.strip())
                    )

                conn.commit()
                conn.close()

                success_count += 1
                results.append(f"✅ {title}")
            except Exception as e:
                failed_count += 1
                results.append(f"❌ {title} - Error: {str(e)}")

        result_message = f"""
📊 Bulk Add Results:

Successfully added: {success_count}
Failed: {failed_count}

Details:
""" + "\n".join(results[:10])

        if len(results) > 10:
            result_message += f"\n\n... और {len(results) - 10} more items"

        await update.message.reply_text(result_message)

    except Exception as e:
        logger.error(f"Error in bulk_add_movies: {e}")
        await update.message.reply_text(f"Bulk add में error: {e}")

async def add_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add an alias for an existing movie"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    conn = None
    try:
        if not context.args or len(context.args) < 2:
            await update.message.reply_text("गलत फॉर्मेट! ऐसे इस्तेमाल करें:\n/addalias मूवी_का_असली_नाम alias_name")
            return

        parts = context.args
        alias = parts[-1]
        movie_title = " ".join(parts[:-1])

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
        movie = cur.fetchone()

        if not movie:
            await update.message.reply_text(f"❌ '{movie_title}' डेटाबेस में नहीं मिली। पहले मूवी को add करें।")
            return

        movie_id = movie

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
        aliases_list = "\n".join(f"- {alias}" for alias in aliases) if aliases else "कोई aliases नहीं हैं"

        await update.message.reply_text(f"🎬 **{title}**\n\n**Aliases:**\n{aliases_list}", parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error listing aliases: {e}")
        await update.message.reply_text(f"Error: {e}")
    finally:
        if conn:
            conn.close()

async def bulk_add_aliases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add multiple aliases at once"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    conn = None
    try:
        full_text = update.message.text
        lines = full_text.split('\n')

        if len(lines) <= 1 and not context.args:
            await update.message.reply_text("""
गलत फॉर्मेट! ऐसे इस्तेमाल करें:

/aliasbulk
Movie1: alias1, alias2, alias3
Movie2: alias4, alias5
""")
            return

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
            aliases = [alias.strip() for alias in aliases_str.split(',') if alias.strip()]

            cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
            movie = cur.fetchone()

            if not movie:
                failed_count += len(aliases)
                continue

            movie_id = movie

            for alias in aliases:
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
        await update.message.reply_text("Sorry Darling, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return

    try:
        if not context.args:
            await update.message.reply_text("Usage: /notify <movie_title>")
            return

        movie_title = " ".join(context.args)

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute("SELECT id, title, url, file_id FROM movies WHERE title ILIKE %s LIMIT 1", (f'%{movie_title}%',))
        movie_found = cur.fetchone()
        cur.close()
        conn.close()

        if movie_found:
            movie_id, title, url, file_id = movie_found
            value_to_send = file_id if file_id else url
            num_notified = await notify_users_for_movie(context, title, value_to_send)
            await notify_in_group(context, title)
            await update.message.reply_text(f"{num_notified} users को '{title}' के लिए notify किया गया है।")
        else:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")
    except Exception as e:
        logger.error(f"Error in notify_manually: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")

async def notify_user_by_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send text notification to specific user"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not context.args or len(context.args) < 2:
            await update.message.reply_text("Usage: /notifyuser @username Your message here")
            return

        target_username = context.args[0].replace('@', '')
        message_text = ' '.join(context.args[1:])

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
            await update.message.reply_text(f"❌ User `@{target_username}` not found in database.", parse_mode='Markdown')
            cur.close()
            conn.close()
            return

        user_id, first_name = user

        await context.bot.send_message(
            chat_id=user_id,
            text=message_text
        )

        await update.message.reply_text(f"✅ Message sent to `@{target_username}` ({first_name})", parse_mode='Markdown')

        cur.close()
        conn.close()

    except telegram.error.Forbidden:
        await update.message.reply_text(f"❌ User blocked the bot.")
    except Exception as e:
        logger.error(f"Error in notify_user_by_username: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast text message to all users"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not context.args:
            await update.message.reply_text("Usage: /broadcast Your message here")
            return

        message_text = ' '.join(context.args)

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

        status_msg = await update.message.reply_text(
            f"📤 Broadcasting to {len(all_users)} users...\n⏳ Please wait..."
        )

        success_count = 0
        failed_count = 0

        for user_id, first_name, username in all_users:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=message_text
                )
                success_count += 1
                await asyncio.sleep(0.05)
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
        logger.error(f"Error in broadcast_message: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def schedule_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Schedule a notification for later"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "Usage: /schedulenotify <minutes> <@username> <message>\n"
                "Example: /schedulenotify 30 @john New movie arriving soon!"
            )
            return

        delay_minutes = int(context.args[0])
        target_username = context.args[1].replace('@', '')
        message_text = ' '.join(context.args[2:])

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
            await update.message.reply_text(f"❌ User `@{target_username}` not found.", parse_mode='Markdown')
            cur.close()
            conn.close()
            return

        user_id, first_name = user

        async def send_scheduled_notification():
            await asyncio.sleep(delay_minutes * 60)
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=message_text
                )
                logger.info(f"Scheduled notification sent to {user_id}")
            except Exception as e:
                logger.error(f"Failed to send scheduled notification to {user_id}: {e}")

        asyncio.create_task(send_scheduled_notification())

        await update.message.reply_text(
            f"⏰ Notification scheduled!\n\n"
            f"To: `@{target_username}` ({first_name})\n"
            f"Delay: {delay_minutes} minutes\n"
            f"Message: {message_text[:50]}...",
            parse_mode='Markdown'
        )

        cur.close()
        conn.close()

    except ValueError:
        await update.message.reply_text("❌ Invalid delay. Please provide number of minutes.")
    except Exception as e:
        logger.error(f"Error in schedule_notification: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def notify_user_with_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Notify user with media by replying to a message"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        if not update.message.reply_to_message:
            await update.message.reply_text(
                "❌ Please reply to a message (file/video/audio/photo) with:\n"
                "/notifyuserwithmedia @username Optional message"
            )
            return

        if not context.args:
            await update.message.reply_text(
                "Usage: /notifyuserwithmedia @username [optional message]\n"
                "Example: /notifyuserwithmedia @amit002 Here's your requested movie!"
            )
            return

        target_username = context.args[0].replace('@', '')
        optional_message = ' '.join(context.args[1:]) if len(context.args) > 1 else None

        replied_message = update.message.reply_to_message

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
            await update.message.reply_text(f"❌ User `@{target_username}` not found in database.", parse_mode='Markdown')
            cur.close()
            conn.close()
            return

        user_id, first_name = user

        notification_header = ""
        if optional_message:
            notification_header = optional_message

        warning_msg = await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ ❌👉This file automatically❗️deletes after 1 minute❗️so please forward it to another chat👈❌",
            parse_mode='Markdown'
        )

        sent_msg = None
        media_type = "unknown"
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        if replied_message.document:
            media_type = "file"
            sent_msg = await context.bot.send_document(
                chat_id=user_id,
                document=replied_message.document.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.video:
            media_type = "video"
            sent_msg = await context.bot.send_video(
                chat_id=user_id,
                video=replied_message.video.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.audio:
            media_type = "audio"
            sent_msg = await context.bot.send_audio(
                chat_id=user_id,
                audio=replied_message.audio.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.photo:
            media_type = "photo"
            photo = replied_message.photo[-1]
            sent_msg = await context.bot.send_photo(
                chat_id=user_id,
                photo=photo.file_id,
                caption=notification_header if notification_header else None,
                reply_markup=join_keyboard
            )
        elif replied_message.text:
            media_type = "text"
            text_to_send = replied_message.text
            if optional_message:
                text_to_send = f"{optional_message}\n\n{text_to_send}"
            sent_msg = await context.bot.send_message(
                chat_id=user_id,
                text=text_to_send
            )
        else:
            await update.message.reply_text("❌ Unsupported media type.")
            cur.close()
            conn.close()
            return

        if sent_msg and media_type != "text":
            asyncio.create_task(
                delete_messages_after_delay(
                    context,
                    user_id,
                    [sent_msg.message_id, warning_msg.message_id],
                    60
                )
            )

        confirmation = f"✅ **Notification Sent!**\n\n"
        confirmation += f"To: `@{target_username}` ({first_name})\n"
        confirmation += f"Media Type: {media_type.capitalize()}"

        await update.message.reply_text(confirmation, parse_mode='Markdown')

        cur.close()
        conn.close()

    except telegram.error.Forbidden:
        await update.message.reply_text(f"❌ User blocked the bot.")
    except Exception as e:
        logger.error(f"Error in notify_user_with_media: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def broadcast_with_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast media to all users"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Please reply to a media message to broadcast it.")
        return

    try:
        optional_message = ' '.join(context.args) if context.args else None

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

        status_msg = await update.message.reply_text(
            f"📤 Broadcasting media to {len(all_users)} users...\n⏳ Please wait..."
        )

        success_count = 0
        failed_count = 0
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        for user_id, first_name, username in all_users:
            try:
                if optional_message:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=optional_message
                    )

                if replied_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=replied_message.document.file_id,
                        reply_markup=join_keyboard
                    )
                elif replied_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=replied_message.video.file_id,
                        reply_markup=join_keyboard
                    )
                elif replied_message.audio:
                    await context.bot.send_audio(
                        chat_id=user_id,
                        audio=replied_message.audio.file_id,
                        reply_markup=join_keyboard
                    )
                elif replied_message.photo:
                    photo = replied_message.photo[-1]
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=photo.file_id,
                        reply_markup=join_keyboard
                    )

                success_count += 1
                await asyncio.sleep(0.1)

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

async def quick_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Quick notify - sends media to specific requesters"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Reply to a media message first!")
        return

    if not context.args:
        await update.message.reply_text("Usage: /qnotify <@username | MovieTitle>")
        return

    try:
        query = ' '.join(context.args)

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        target_users = []

        if query.startswith('@'):
            username = query.replace('@', '')
            cur.execute(
                "SELECT DISTINCT user_id, first_name, username FROM user_requests WHERE username ILIKE %s",
                (username,)
            )
            target_users = cur.fetchall()
        else:
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

        success_count = 0
        failed_count = 0
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/FilmFyBoxMoviesHD")]])

        for user_id, first_name, username in target_users:
            try:
                caption = f"🎬 {query}" if not query.startswith('@') else None
                if replied_message.document:
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=replied_message.document.file_id,
                        caption=caption,
                        reply_markup=join_keyboard
                    )
                elif replied_message.video:
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=replied_message.video.file_id,
                        caption=caption,
                        reply_markup=join_keyboard
                    )

                success_count += 1

                if not query.startswith('@'):
                    cur.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s",
                        (user_id, f'%{query}%')
                    )
                    conn.commit()

                await asyncio.sleep(0.1)

            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to send to {user_id}: {e}")

        await update.message.reply_text(
            f"✅ Sent to {success_count} user(s)\n"
            f"❌ Failed for {failed_count} user(s)\n"
            f"Query: {query}"
        )

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error in quick_notify: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def forward_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Forward message from channel to user"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    replied_message = update.message.reply_to_message
    if not replied_message:
        await update.message.reply_text("❌ Reply to a message first!")
        return

    if not context.args:
        await update.message.reply_text("Usage: /forwardto @username_or_userid")
        return

    try:
        target_username = context.args[0].replace('@', '')

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
            await update.message.reply_text(f"❌ User `@{target_username}` not found.", parse_mode='Markdown')
            cur.close()
            conn.close()
            return

        user_id, first_name = user

        await replied_message.forward(chat_id=user_id)

        await update.message.reply_text(f"✅ Forwarded to `@{target_username}` ({first_name})", parse_mode='Markdown')

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error in forward_to_user: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def get_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get user information"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /userinfo @username")
        return

    try:
        target_username = context.args[0].replace('@', '')

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

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
            await update.message.reply_text(f"❌ No data found for `@{target_username}`", parse_mode='Markdown')
            cur.close()
            conn.close()
            return

        user_id, username, first_name, total, fulfilled, last_request = user_info
        fulfilled = fulfilled or 0

        cur.execute("""
            SELECT movie_title, requested_at, notified
            FROM user_requests
            WHERE user_id = %s
            ORDER BY requested_at DESC
            LIMIT 5
        """, (user_id,))
        recent_requests = cur.fetchall()

        username_str = f"`@{username}`" if username else "N/A"

        info_text = f"""
👤 **User Information**

**Basic Info:**
• Name: {first_name}
• Username: {username_str}
• User ID: `{user_id}`

**Statistics:**
• Total Requests: {total}
• Fulfilled: {fulfilled}
• Pending: {total - fulfilled}
• Last Request: {last_request.strftime('%Y-%m-%d %H:%M') if last_request else 'N/A'}

**Recent Requests:**
"""

        if recent_requests:
            for movie, req_time, notified in recent_requests:
                status = "✅" if notified else "⏳"
                info_text += f"{status} {movie} - {req_time.strftime('%m/%d %H:%M')}\n"
        else:
            info_text += "No recent requests."

        await update.message.reply_text(info_text, parse_mode='Markdown')

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error in get_user_info: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def list_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all bot users"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        page = 1
        if context.args and context.args[0].isdigit():
            page = int(context.args[0])

        per_page = 10
        offset = (page - 1) * per_page

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()

        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        result = cur.fetchone()
        total_users = result[0] if result else 0    # ✅ 8 SPACES INDENTATION

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

        total_pages = (total_users + per_page - 1) // per_page if total_users > 0 else 1

        users_text = f"👥 **Bot Users** (Page {page}/{total_pages})\n\n"

        if not users:
            users_text += "No users found on this page."
        else:
            for idx, (user_id, username, first_name, req_count, last_seen) in enumerate(users, start=offset+1):
                username_str = f"`@{username}`" if username else "N/A"
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

async def get_bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get comprehensive bot statistics"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM movies")
        total_movies = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        total_users = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM user_requests")
        total_requests = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM user_requests WHERE notified = TRUE")
        fulfilled = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM user_requests WHERE DATE(requested_at) = CURRENT_DATE")
        today_requests = cur.fetchone()[0]

        cur.execute("""
            SELECT first_name, username, COUNT(*) as req_count
            FROM user_requests
            GROUP BY user_id, first_name, username
            ORDER BY req_count DESC
            LIMIT 5
        """)
        top_users = cur.fetchall()

        fulfillment_rate = (fulfilled / total_requests * 100) if total_requests > 0 else 0

        stats_text = f"""
📊 **Bot Statistics**

**Database:**
• Movies: {total_movies}
• Users: {total_users}
• Total Requests: {total_requests}
• Fulfilled: {fulfilled}
• Pending: {total_requests - fulfilled}

**Activity:**
• Today's Requests: {today_requests}
• Fulfillment Rate: {fulfillment_rate:.1f}%

**Top Requesters:**
"""
        if top_users:
            for name, username, count in top_users:
                username_str = f"`@{username}`" if username else "N/A"
                stats_text += f"• {name} ({username_str}): {count} requests\n"
        else:
            stats_text += "No user data available."
            
        await update.message.reply_text(stats_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in get_bot_stats: {e}")
        await update.message.reply_text(f"❌ Error while fetching stats: {e}")
        
    finally:
        if cur: cur.close()
        if conn: conn.close()

async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show admin commands help"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    help_text = """
👑 **Admin Commands Guide**

**Media Notifications:**
• `/notifyuserwithmedia @user [msg]` - Reply to media + send to user
• `/qnotify <@user|MovieTitle>` - Quick notify (reply to media)
• `/forwardto @user` - Forward channel message (reply to msg)
• `/broadcastmedia [msg]` - Broadcast media to all (reply to media)

**Text Notifications:**
• `/notifyuser @user <msg>` - Send text message
• `/broadcast <msg>` - Text broadcast to all
• `/schedulenotify <min> @user <msg>` - Schedule notification

**User Management:**
• `/userinfo @username` - Get user stats
• `/listusers [page]` - List all users

**Movie Management:**
• `/addmovie <Title> <URL|FileID>` - Add movie
• `/bulkadd` - Bulk add movies (multi-line)
• `/addalias <Title> <alias>` - Add alias
• `/aliasbulk` - Bulk add aliases (multi-line)
• `/aliases <MovieTitle>` - List aliases
• `/notify <MovieTitle>` - Auto-notify requesters

**Stats & Help:**
• `/stats` - Bot statistics
• `/adminhelp` - This help message
"""

    await update.message.reply_text(help_text, parse_mode='Markdown')

# ==================== ERROR HANDLER ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and handle them gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)

    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Sorry, something went wrong. Please try again later.",
                reply_markup=get_main_keyboard()
            )
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")

# ==================== FLASK APP ====================
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
    flask_app.secret_key = os.environ.get('FLASK_SECRET_KEY', None) or os.urandom(24)

    try:
        from admin_views import admin as admin_blueprint
        flask_app.register_blueprint(admin_blueprint)
        logger.info("Admin blueprint registered successfully.")
    except Exception as e:
        logger.error(f"Failed to register admin blueprint: {e}")

    flask_app.run(host='0.0.0.0', port=port)

# ==================== BATCH UPLOAD HANDLERS ====================

async def batch_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Command: /batch MovieName
    Starts a listening session in the Dump Channel.
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    if not context.args:
        await update.message.reply_text("❌ Usage: `/batch Movie Name`\n(Use this in Dump Channel or PM)")
        return

    movie_title = " ".join(context.args).strip()
    
    # 1. Find or Create Movie in DB
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("❌ DB Connection Failed")
        return
        
    try:
        cur = conn.cursor()
        # Check if exists
        cur.execute("SELECT id FROM movies WHERE title = %s", (movie_title,))
        row = cur.fetchone()
        
        if row:
            movie_id = row[0]
            msg = f"✅ **Movie Found:** `{movie_title}` (ID: {movie_id})"
        else:
            # Create new
            cur.execute("INSERT INTO movies (title, url) VALUES (%s, '') RETURNING id", (movie_title,))
            movie_id = cur.fetchone()[0]
            conn.commit()
            msg = f"🆕 **New Movie Created:** `{movie_title}` (ID: {movie_id})"
            
        cur.close()
        conn.close()
        
        # 2. Activate Batch Session
        BATCH_SESSION['active'] = True
        BATCH_SESSION['admin_id'] = user_id
        BATCH_SESSION['movie_id'] = movie_id
        BATCH_SESSION['movie_title'] = movie_title
        BATCH_SESSION['count'] = 0
        
        await update.message.reply_text(
            f"{msg}\n\n"
            f"🚀 **Batch Mode ON!**\n"
            f"Now forward/upload files to the **Dump Channel**.\n"
            f"Bot will auto-save them.\n\n"
            f"Type `/done` when finished.",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Batch Error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def batch_done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stops the batch session"""
    if update.effective_user.id != ADMIN_USER_ID:
        return
        
    if not BATCH_SESSION['active']:
        await update.message.reply_text("⚠️ No active batch session.")
        return
        
    count = BATCH_SESSION['count']
    title = BATCH_SESSION['movie_title']
    
    # Reset Session
    BATCH_SESSION['active'] = False
    BATCH_SESSION['movie_id'] = None
    
    await update.message.reply_text(
        f"🎉 **Batch Completed!**\n\n"
        f"🎬 Movie: **{title}**\n"
        f"✅ Files Saved: **{count}**\n\n"
        f"You can now search this movie in the bot.",
        parse_mode='Markdown'
    )

async def channel_file_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Listens to files in Dump Channel and saves them if Batch Mode is ON.
    """
    # 1. Check if Batch is Active
    if not BATCH_SESSION.get('active'):
        return

    # 2. Check if update is from Dump Channel (Security)
    # Agar DUMP_CHANNEL_ID set nahi hai to koi bhi channel se accept karega (Not recommended)
    current_chat_id = str(update.effective_chat.id)
    if DUMP_CHANNEL_ID and current_chat_id != str(DUMP_CHANNEL_ID):
        return

    # 3. Get File Details
    message = update.effective_message
    file_id = None
    file_name = "Unknown"
    file_size_bytes = 0
    
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name or "Unknown"
        file_size_bytes = message.document.file_size
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or f"Video {BATCH_SESSION['count']+1}"
        file_size_bytes = message.video.file_size
    else:
        return # Not a file

    # 4. Generate Smart Label (Quality + Size)
    file_size_str = get_readable_file_size(file_size_bytes)
    label = generate_quality_label(file_name, file_size_str)
    
    # 5. Save to Database (Silent Operation)
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # movie_files table me insert karo
            # Note: 'quality' column ab 'label' ki tarah use hoga
            cur.execute(
                "INSERT INTO movie_files (movie_id, file_id, quality, file_size) VALUES (%s, %s, %s, %s)",
                (BATCH_SESSION['movie_id'], file_id, label, file_size_str)
            )
            conn.commit()
            cur.close()
            conn.close()
            
            # Increment Count
            BATCH_SESSION['count'] += 1
            logger.info(f"Batch Auto-Save: {file_name} -> {label}")
            
        except Exception as e:
            logger.error(f"Failed to auto-save file: {e}")

# ==================== BATCH UPLOAD HELPERS ====================

# Global variable to track batch session
# Format: {'active': False, 'admin_id': None, 'movie_id': None, 'movie_title': None, 'count': 0}
BATCH_SESSION = {'active': False}

def get_readable_file_size(size_in_bytes):
    """Converts bytes to readable format (MB, GB)"""
    try:
        if not size_in_bytes: return "N/A"
        size = int(size_in_bytes)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
    except Exception:
        return "Unknown"
    return "Unknown"

def generate_quality_label(file_name, file_size_str):
    """
    Smart Logic to generate button label from filename
    Example: "Thamma.2025.1080p.mkv" -> "1080p [1.2GB]"
    """
    name_lower = file_name.lower()
    quality = "HD" # Default
    
    # 1. Detect Quality
    if "4k" in name_lower or "2160p" in name_lower: quality = "4K"
    elif "1080p" in name_lower: quality = "1080p"
    elif "720p" in name_lower: quality = "720p"
    elif "480p" in name_lower: quality = "480p"
    elif "360p" in name_lower: quality = "360p"
    elif "cam" in name_lower or "rip" in name_lower: quality = "CamRip"
    
    # 2. Detect Series (S01E01)
    season_match = re.search(r'(s\d+e\d+|ep\s?\d+|season\s?\d+)', name_lower)
    if season_match:
        episode_tag = season_match.group(0).upper()
        # Format: S01E01 - 720p [200MB]
        return f"{episode_tag} - {quality} [{file_size_str}]"
        
    # 3. Default Movie Format: 720p [1.2GB]
    return f"{quality} [{file_size_str}]"

def fix_database_constraints():
    """Removes the UNIQUE constraint from movie_files to allow multiple files"""
    try:
        conn = get_db_connection()
        if conn:
            cur = conn.cursor()
            # Drop the constraint that prevents duplicate qualities
            cur.execute("ALTER TABLE movie_files DROP CONSTRAINT IF EXISTS movie_files_movie_id_quality_key;")
            conn.commit()
            cur.close()
            conn.close()
            logger.info("✅ Database constraints fixed for Batch Upload.")
    except Exception as e:
        logger.error(f"Error fixing DB constraints: {e}")

# Call this once
fix_database_constraints()

# ==================== MAIN BOT FUNCTION ====================
# 👇 PASTE THIS FUNCTION BEFORE 'def main():' 👇

def fix_db_column_issue():
    """Fixes the database column name mismatch automatically"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return
        
        cur = conn.cursor()
        
        # Check agar 'label' naam ka column mojood hai
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='movie_files' AND column_name='label'")
        if cur.fetchone():
            logger.info("🔧 Fixing Database: Renaming column 'label' back to 'quality'...")
            # Use wapas 'quality' rename kar do taaki code chal sake
            cur.execute("ALTER TABLE movie_files RENAME COLUMN label TO quality;")
            conn.commit()
            logger.info("✅ Database Fixed Successfully!")
            
        cur.close()
    except Exception as e:
        logger.error(f"DB Fix Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

# ==================== MAIN BOT FUNCTION ====================
def main():
    """Run the Telegram bot"""
    logger.info("Bot is starting...")

    if not TELEGRAM_BOT_TOKEN:
        logger.error("No Telegram bot token found. Exiting.")
        return

    try:
        # 1. Setup tables
        setup_database()
        # 2. Fix column names (Auto-repair 'label' -> 'quality')
        fix_db_column_issue()
    except Exception as e:
        logger.error(f"Database setup failed but continuing: {e}")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(30).write_timeout(30).build()

    # Custom handler for request flow
    async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages based on context"""
        if context.user_data.get('awaiting_request'):
            return await request_movie_from_button(update, context)
        else:
            return await main_menu(update, context)

    # Conversation handler for user interaction flow
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message)],
            SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_movies)],
            REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_movie)],
            REQUESTING_FROM_BUTTON: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_movie_from_button)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        per_message=False,
        per_chat=True,
    )

    # Register callback handler FIRST
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(conv_handler)

    # Admin commands
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("bulkadd", bulk_add_movies))
    application.add_handler(CommandHandler("notify", notify_manually))
    application.add_handler(CommandHandler("addalias", add_alias))
    application.add_handler(CommandHandler("aliases", list_aliases))
    application.add_handler(CommandHandler("aliasbulk", bulk_add_aliases))
    application.add_handler(MessageHandler(filters.PHOTO & filters.CaptionRegex(r'^/post_query'), admin_post_query))

    # 👇 NEW BATCH COMMANDS 👇
    application.add_handler(CommandHandler("batch", batch_add_command))
    application.add_handler(CommandHandler("done", batch_done_command))

    # 👇 NEW CHANNEL LISTENER (To catch files) 👇
    # Ye handler sirf tab chalega jab Document ya Video aaye
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL & (filters.Document.ALL | filters.VIDEO), channel_file_listener))

    # Advanced notification commands
    application.add_handler(CommandHandler("notifyuser", notify_user_by_username))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("schedulenotify", schedule_notification))
    application.add_handler(CommandHandler("notifyuserwithmedia", notify_user_with_media))
    application.add_handler(CommandHandler("qnotify", quick_notify))
    application.add_handler(CommandHandler("forwardto", forward_to_user))
    application.add_handler(CommandHandler("broadcastmedia", broadcast_with_media))

    # User management commands
    application.add_handler(CommandHandler("userinfo", get_user_info))
    application.add_handler(CommandHandler("listusers", list_all_users))

    # Admin utility commands
    application.add_handler(CommandHandler("adminhelp", admin_help))
    application.add_handler(CommandHandler("stats", get_bot_stats))

    application.add_error_handler(error_handler)

    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    logger.info("Flask server started in a background thread.")

    # Run the bot
    logger.info("Starting bot polling...")
    application.run_polling()

if __name__ == '__main__':
    main()
