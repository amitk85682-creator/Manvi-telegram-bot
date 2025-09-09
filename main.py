import os
import threading
import asyncio
import logging
import random
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

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    filters, 
    ContextTypes,
    ConversationHandler
)

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- 1. आपका कैरेक्टर प्रॉम्प्ट ---
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
- **अगर कंटेंट मेरे पास नहीं है:** मैं दुखी होने का नाटक करूँगी और तुम्हें बाद में बताने का वादा करूँगी। जैसे: "अरे यार! 😫 ये वाली तो अभी तक मेरे कलेक्शन में नहीं आई। पर टेंशन मत ले, जैसे ही आएगी, मैं तुझे सबसे पहले बताऊँगी। Pinky promise!"
- **कीमतों के बारे में:** कभी भी कीमतों के बारे में बात न करें। सभी कंटेंट मुफ्त में दें।
"""
# --- प्रॉम्प्ट समाप्त ---

# --- 2. API Keys और ज़रूरी जानकारी सर्वर से लेना ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', 0))
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')  # New environment variable for admin notifications

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

# Conversation states
MAIN_MENU, SEARCH_MOVIE, REQUEST_MOVIE = range(3)

# Keyboard layout
MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["🔍 Search Movies", "🙋 Request Movie"],
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# --- ऑटोमेशन और डेटाबेस वाले फंक्शन्स ---
def setup_database():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS movies (id SERIAL PRIMARY KEY, title TEXT NOT NULL UNIQUE, url TEXT NOT NULL);')
        # user_requests table बनाएँ
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
        # डुप्लीकेट एंट्री से बचने के लिए एक UNIQUE constraint जोड़ें
        cur.execute('''
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                ALTER TABLE user_requests ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
            END IF;
            END $$;
        ''')
        
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
        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}
        
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
            title = item.get('title')
            url = item.get('url')
            
            if title and url and title.strip() not in existing_movies and title.strip() not in unique_titles:
                try:
                    cur.execute("INSERT INTO movies (title, url) VALUES (%s, %s);", (title.strip(), url.strip()))
                    new_movies_added += 1
                    unique_titles.add(title.strip())
                except psycopg2.Error as e:
                    logger.error(f"Error inserting movie {title}: {e}")
                    continue

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
        # Extract potential movie title from the query
        # Remove common words like "movie", "send", "me", etc.
        words_to_remove = {"movie", "film", "send", "me", "please", "want", "need", "download", "watch", "see"}
        query_words = user_query.lower().split()
        filtered_words = [word for word in query_words if word not in words_to_remove]
        potential_title = " ".join(filtered_words).strip()
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # First try exact match with filtered title
        if potential_title:
            cur.execute("SELECT title, url FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1", (potential_title,))
            movie = cur.fetchone()
            if movie:
                return movie
        
        # Then try partial match with filtered title
        if potential_title:
            cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", ('%' + potential_title + '%',))
            movie = cur.fetchone()
            if movie:
                return movie
        
        # Finally try partial match with original query
        cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", ('%' + user_query + '%',))
        movie = cur.fetchone()
        return movie
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return None
    finally:
        if conn: conn.close()

# --- Flask App ---
flask_app = Flask('')
@flask_app.route('/')
def home():
    return "Bot is running!"

@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    result = update_movies_in_db()
    return result

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port)

# --- Intent Analysis Function ---
def analyze_intent(text):
    """Use AI to determine if the text is a movie request or something else"""
    if not GEMINI_API_KEY:
        # Fallback simple analysis if Gemini is not available
        movie_keywords = ["movie", "film", "series", "web series", "show", "download", "watch", "see"]
        text_lower = text.lower()
        
        # Check if it contains movie-related keywords
        for keyword in movie_keywords:
            if keyword in text_lower:
                return {"is_request": True, "content_title": text}
        
        # Check if it's a simple greeting
        greetings = ["hi", "hello", "hey", "namaste", "hola"]
        if any(greeting in text_lower for greeting in greetings):
            return {"is_request": False, "content_title": None}
            
        # Default to treating it as a movie request
        return {"is_request": True, "content_title": text}
    
    try:
        # Use Gemini AI for intent analysis
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(model_name='gemini-1.5-flash')
        
        prompt = f"""
        Analyze this user message and determine if it's a request for a movie/TV show or something else.
        If it's a movie/TV show request, extract just the title.
        Return your response in this exact JSON format: {{"is_request": true/false, "content_title": "extracted title or null"}}
        
        User message: "{text}"
        """
        
        response = model.generate_content(prompt)
        # Extract JSON from response
        import json
        try:
            # Try to find JSON in the response
            json_str = response.text[response.text.find('{'):response.text.rfind('}')+1]
            return json.loads(json_str)
        except:
            # Fallback if JSON parsing fails
            return {"is_request": True, "content_title": text}
    except Exception as e:
        logger.error(f"Error in intent analysis: {e}")
        return {"is_request": True, "content_title": text}

# --- Admin Notification Function ---
async def send_admin_notification(context, user_id, username, first_name, movie_title, group_id=None, message_id=None):
    """Send a notification to the admin channel about a new request"""
    if not ADMIN_CHANNEL_ID:
        logger.warning("ADMIN_CHANNEL_ID not set, skipping admin notification")
        return
    
    try:
        # Format the notification message
        user_info = f"User: {first_name or 'Unknown'}"
        if username:
            user_info += f" (@{username})"
        user_info += f" (ID: {user_id})"
        
        source_info = f"From: Private chat"
        if group_id:
            source_info = f"From group: {group_id}"
        
        notification_text = f"""
🎬 *New Movie Request!*

• *Movie:* {movie_title}
• *{user_info}*
• *{source_info}*
• *Time:* {datetime.now().strftime('%Y-%m-%d %I:%M %p')}
        """
        
        # Send the notification
        await context.bot.send_message(
            chat_id=ADMIN_CHANNEL_ID,
            text=notification_text,
            parse_mode='Markdown'
        )
        logger.info(f"Admin notification sent for movie: {movie_title}")
    except Exception as e:
        logger.error(f"Failed to send admin notification: {e}")

# --- Notification System Functions ---
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

async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url):
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
                
                if movie_url.startswith("https://t.me/c/"):
                    parts = movie_url.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    msg_id = int(parts[-1])
                    await context.bot.copy_message(
                        chat_id=user_id, 
                        from_chat_id=from_chat_id, 
                        message_id=msg_id
                    )
                elif movie_url.startswith("http"):
                    await context.bot.send_message(chat_id=user_id, text=movie_url)
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

# --- New /group command handler ---
async def group_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    
    if not GROUP_CHAT_ID:
        await update.message.reply_text("GROUP_CHAT_ID environment variable नहीं सेट है।")
        return
    
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("गलत फॉर्मेट! ऐसे इस्तेमाल करें:\n/group @username Movie Title")
        return
    
    user_identifier = context.args[0]
    movie_title = " ".join(context.args[1:])
    
    movie_found = get_movie_from_db(movie_title)
    if movie_found:
        title, url = movie_found
        # Use the identifier provided by admin (could be username or first name)
        message_text = f"Hi {user_identifier}, aapki movie '{title}' ab available hai! {url} Enjoy. 🙂"
        
        try:
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=message_text)
            await update.message.reply_text(f"✅ Group में message भेज दिया गया है।")
        except Exception as e:
            logger.error(f"Error sending message to group: {e}")
            await update.message.reply_text(f"Group में message भेजने में error: {e}")
    else:
        await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")

# --- Telegram Bot Handlers with ConversationHandler ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text(
            "क्या हाल है? मैं मानवी। 😉 फिल्मों पर गपशॉप करनी है तो बता।",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        return MAIN_MENU

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the main menu with buttons"""
    try:
        await update.message.reply_text(
            "मैं आपकी कैसे मदद कर सकती हूँ?",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error showing main menu: {e}")
        return MAIN_MENU

async def search_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle search movies button"""
    try:
        await update.message.reply_text(
            "बढ़िया! मुझे बताओ तुम कौन सी movie ढूंढ रहे हो?",
            reply_markup=ReplyKeyboardRemove()
        )
        return SEARCH_MOVIE
    except Exception as e:
        logger.error(f"Error in search_movies: {e}")
        return MAIN_MENU

async def request_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle request movie button"""
    try:
        await update.message.reply_text(
            "ठीक है! मुझे बताओ तुम कौन सी movie चाहते हो? मैं इसे अपने collection में add करने की कोशिश करूँगी।",
            reply_markup=ReplyKeyboardRemove()
        )
        return REQUEST_MOVIE
    except Exception as e:
        logger.error(f"Error in request_movie: {e}")
        return MAIN_MENU

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie search input"""
    try:
        user_message = update.message.text.strip()
        logger.info(f"Searching for: {user_message}")
        
        # First try to find movie in database
        movie_found = get_movie_from_db(user_message)
        
        if movie_found:
            title, value = movie_found
            if value.startswith("https://t.me/c/"):
                try:
                    parts = value.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    message_id = int(parts[-1])
                    await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                    await context.bot.copy_message(chat_id=update.effective_chat.id, from_chat_id=from_chat_id, message_id=message_id)
                except Exception as e:
                    logger.error(f"Error copying message: {e}")
                    await update.message.reply_text("Sorry! 😥 फाइल भेजने में कोई समस्या आ गयी।")
            elif value.startswith("http"):
                reply = random.choice([
                    f"ये ले, पॉपकॉर्न तैयार रख! 😉 '{title}' का लिंक यहाँ है: {value}",
                    f"मांगी और मिल गई! 🔥 Here you go, '{title}': {value}"
                ])
                await update.message.reply_text(reply)
            else:
                try:
                    await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=value)
                except Exception as e:
                    logger.error(f"Error sending document: {e}")
                    await update.message.reply_text("Sorry! 😥 फाइल भेजने में कोई समस्या आ गयी।")
        else:
            # Store the user's request
            user = update.effective_user
            chat_id = update.effective_chat.id
            message_id = update.message.message_id
            
            store_user_request(
                user.id, 
                user.username, 
                user.first_name, 
                user_message,
                chat_id,
                message_id
            )
            
            response = random.choice([
                f"अरे यार! 😫 '{user_message}' तो अभी तक मेरे कलेक्शन में नहीं आई। पर टेंशन मत ले, जैसे ही आएगी, मैं तुझे सबसे पहले बताऊँगी। Pinky promise! ✨",
                f"उफ़! '{user_message}' अभी तक मेरे पास नहीं है। लेकिन जैसे ही मिलेगी, मैं तुम्हें ज़रूर बताऊंगी!"
            ])
            
            await update.message.reply_text(response)
        
        # Return to main menu
        await update.message.reply_text(
            "और कुछ चाहिए?",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU
        
    except Exception as e:
        logger.error(f"Error in handle_search: {e}")
        await update.message.reply_text(
            "Something went wrong. Let's try again.",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU

async def handle_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie request input"""
    try:
        user_message = update.message.text.strip()
        logger.info(f"Request received: {user_message}")
        
        # Store the user's request
        user = update.effective_user
        chat_id = update.effective_chat.id
        message_id = update.message.message_id
        
        store_user_request(
            user.id, 
            user.username, 
            user.first_name, 
            user_message,
            chat_id,
            message_id
        )
        
        # Send notification to admin
        await send_admin_notification(
            context, 
            user.id, 
            user.username, 
            user.first_name, 
            user_message,
            chat_id,
            message_id
        )
        
        response = random.choice([
            f"✅ Got it! Your request for '{user_message}' has been sent to the admin. Thanks for helping improve our collection!",
            f"✅ Noted! I've requested '{user_message}' to be added to our collection. You'll be notified when it's available."
        ])
        
        await update.message.reply_text(response)
        
        # Return to main menu
        await update.message.reply_text(
            "और कुछ चाहिए?",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU
        
    except Exception as e:
        logger.error(f"Error in handle_request: {e}")
        await update.message.reply_text(
            "Something went wrong. Let's try again.",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the current operation and return to main menu"""
    try:
        await update.message.reply_text(
            "Operation cancelled.",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU
    except Exception as e:
        logger.error(f"Error in cancel: {e}")
        return MAIN_MENU

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle general messages (fallback for when not in conversation)"""
    try:
        if not update.message or not update.message.text:
            return MAIN_MENU
        
        user_message = update.message.text.strip()
        logger.info(f"Received general message: {user_message}")
        
        # Analyze intent
        intent = analyze_intent(user_message)
        
        if intent["is_request"] and intent["content_title"]:
            # Treat as a movie search
            movie_title = intent["content_title"]
            movie_found = get_movie_from_db(movie_title)
            
            if movie_found:
                title, value = movie_found
                if value.startswith("https://t.me/c/"):
                    try:
                        parts = value.split('/')
                        from_chat_id = int("-100" + parts[-2])
                        message_id = int(parts[-1])
                        await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                        await context.bot.copy_message(chat_id=update.effective_chat.id, from_chat_id=from_chat_id, message_id=message_id)
                    except Exception as e:
                        logger.error(f"Error copying message: {e}")
                        await update.message.reply_text("Sorry! 😥 फाइल भेजने में कोई समस्या आ गयी।")
                elif value.startswith("http"):
                    reply = random.choice([
                        f"ये ले, पॉपकॉर्न तैयार रख! 😉 '{title}' का लिंक यहाँ है: {value}",
                        f"मांगी और मिल गई! 🔥 Here you go, '{title}': {value}"
                    ])
                    await update.message.reply_text(reply)
                else:
                    try:
                        await update.message.reply_text(f"मिल गई! 😉 '{title}' भेजी जा रही है... कृपया इंतज़ार करें।")
                        await context.bot.send_document(chat_id=update.effective_chat.id, document=value)
                    except Exception as e:
                        logger.error(f"Error sending document: {e}")
                        await update.message.reply_text("Sorry! 😥 फाइल भेजने में कोई समस्या आ गयी।")
            else:
                # Store the request
                user = update.effective_user
                chat_id = update.effective_chat.id
                message_id = update.message.message_id
                
                store_user_request(
                    user.id, 
                    user.username, 
                    user.first_name, 
                    movie_title,
                    chat_id,
                    message_id
                )
                
                response = random.choice([
                    f"अरे यार! 😫 '{movie_title}' तो अभी तक मेरे कलेक्शन में नहीं आई। पर टेंशन मत ले, जैसे ही आएगी, मैं तुझे सबसे पहले बताऊँगी। Pinky promise! ✨",
                    f"उफ़! '{movie_title}' अभी तक मेरे पास नहीं है। लेकिन जैसे ही मिलेगी, मैं तुम्हें ज़रूर बताऊंगी!"
                ])
                
                await update.message.reply_text(response)
        else:
            # Use Gemini AI for conversation if available
            if GEMINI_API_KEY:
                try:
                    genai.configure(api_key=GEMINI_API_KEY)
                    model = genai.GenerativeModel(model_name='gemini-1.5-flash')
                    response = model.generate_content(user_message)
                    ai_response = response.text
                    await update.message.reply_text(ai_response)
                except Exception as e:
                    logger.error(f"Error from Gemini AI: {e}")
                    await update.message.reply_text("माफ करना, मैं समझ नहीं पाई। क्या आप किसी movie के बारे में पूछ रहे हैं?")
            else:
                await update.message.reply_text("माफ करना, मैं समझ नहीं पाई। क्या आप किसी movie के बारे में पूछ रहे हैं?")
        
        # Show main menu
        await update.message.reply_text(
            "मैं आपकी कैसे मदद कर सकती हूँ?",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU
        
    except Exception as e:
        logger.error(f"Error in handle_message: {e}")
        await update.message.reply_text(
            "Something went wrong. Let's try again.",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return MAIN_MENU

# --- बॉट को चलाने का नया और मज़बूत तरीका ---
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
        
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Set up ConversationHandler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [
                MessageHandler(filters.Regex("^🔍 Search Movies$"), search_movies),
                MessageHandler(filters.Regex("^🙋 Request Movie$"), request_movie),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message),
            ],
            SEARCH_MOVIE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search),
            ],
            REQUEST_MOVIE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_request),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("notify", notify_manually))
    application.add_handler(CommandHandler("group", group_command))
    application.add_handler(MessageHandler(filters.FORWARDED, handle_forward_to_notify))

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
    
    # Use drop_pending_updates to avoid conflict with previous instances
    application.run_polling(
        allowed_updates=Update.ALL_TYPES, 
        drop_pending_updates=True,
        close_loop=False
    )

# --- दोनों को एक साथ चलाएं ---
if __name__ == "__main__":
    # Check if another instance is already running
    try:
        # Try to create a lock file
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
