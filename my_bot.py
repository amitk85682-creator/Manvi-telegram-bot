import os
import threading
import asyncio
import logging
import random
import requests
from bs4 import BeautifulSoup
import telegram

import psycopg2
from flask import Flask, request

import google.generativeai as genai
from googleapiclient.discovery import build

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

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

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")

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
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", (user_query + '%',))
        movie = cur.fetchone()
        if not movie:
            cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", ('%' + user_query + '%',))
            movie = cur.fetchone()
        cur.close()
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

# --- Telegram Bot का लॉजिक ---
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(model_name='gemini-1.5-flash')
        chat = model.start_chat(history=[
            {'role': 'user', 'parts': [CHARACTER_PROMPT]},
            {'role': 'model', 'parts': ["Okay, I am Manvi."]}
        ])
        logger.info("Gemini AI initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Gemini AI: {e}")
        chat = None
else:
    chat = None
    logger.warning("Gemini AI not initialized due to missing API key")

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
                    mention = f"@{username}" if username else first_name
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
        await update.message.reply_text("क्या हाल है? मैं मानवी। 😉 फिल्मों पर गपशॉप करनी है तो बता।")
    except Exception as e:
        logger.error(f"Error in start command: {e}")

async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    
    conn = None
    cur = None
    try:
        parts = context.args
        if len(parts) < 2:
            await update.message.reply_text("गलत फॉर्मेट! ऐसे इस्तेमाल करें:\n/addmovie टाइटल का नाम [File ID या Link]")
            return
        
        value = parts[-1]
        title = " ".join(parts[:-1])
        
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url;", 
                    (title.strip(), value.strip()))
        conn.commit()
        
        await update.message.reply_text(f"बढ़िया! '{title}' को डेटाबेस में सफलतापूर्वक जोड़ दिया गया है। ✅")
        
        # Notify users who requested this movie
        num_notified = await notify_users_for_movie(context, title, value)
        
        # Also notify in group for users who couldn't be reached privately
        await notify_in_group(context, title)
            
        await update.message.reply_text(f"कुल {num_notified} users को notify किया गया है।")
            
    except Exception as e:
        logger.error(f"Error in add_movie command: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

async def notify_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            title, value = movie_found
            num_notified = await notify_users_for_movie(context, title, value)
            await update.message.reply_text(f"{num_notified} users को '{title}' के लिए notify किया गया है।")
            await notify_in_group(context, title)
        else:
            await update.message.reply_text(f"'{movie_title}' डेटाबेस में नहीं मिली।")
    except Exception as e:
        logger.error(f"Error in notify_manually: {e}")
        await update.message.reply_text(f"एक एरर आया: {e}")

async def handle_forward_to_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_user.id != ADMIN_USER_ID:
            return
            
        if not update.message or not hasattr(update.message, 'forward_origin'):
            logger.warning("Message doesn't have forward_origin attribute")
            await update.message.reply_text("यह फॉरवर्ड किसी ऐसे स्रोत से है जिसे मैं प्रोसेस नहीं कर सकती।")
            return
            
        origin = update.message.forward_origin
        original_user = None
        original_chat = None

        if origin.type == "user":
            original_user = origin.sender_user
        elif origin.type == "chat":
            original_chat = origin.sender_chat
            if hasattr(origin, 'sender_user'):
                original_user = origin.sender_user
        else:
            logger.warning(f"Unhandled forward origin type: {origin.type}")
            return
        
        if not original_user:
            await update.message.reply_text("मैं इस फॉरवर्ड मैसेज के मूल यूजर को आइडेंटिफाई नहीं कर पा रही हूँ।")
            return
        
        movie_query = update.message.text
        if not movie_query:
            await update.message.reply_text("कृपया फिल्म का नाम भी फॉरवर्ड करें।")
            return
        
        movie_found = get_movie_from_db(movie_query)
        if movie_found:
            title, value = movie_found
            notification_text = f"Hey {original_user.first_name}! आप '{title}' ढूंढ रहे थे। यह अब उपलब्ध है! ✨"
            
            try:
                await context.bot.send_message(chat_id=original_user.id, text=notification_text)
                
                if value.startswith("https://t.me/c/"):
                    parts = value.split('/')
                    from_chat_id = int("-100" + parts[-2])
                    message_id = int(parts[-1])
                    await context.bot.copy_message(chat_id=original_user.id, from_chat_id=from_chat_id, message_id=message_id)
                elif value.startswith("http"):
                    await context.bot.send_message(chat_id=original_user.id, text=value)
                else:
                    await context.bot.send_document(chat_id=original_user.id, document=value)
                
                await update.message.reply_text(f"✅ यूजर ({original_user.first_name}) को प्राइवेट में सूचित कर दिया गया है।")
            except Exception as e:
                logger.error(f"Could not send PM to {original_user.id}: {e}")
                if original_chat:
                    user_mention = original_user.mention_html()
                    bot_username = context.bot.username
                    fallback_text = f"Hey {user_mention}, आपकी मूवी/वेबसीरीज '{title}' आ गयी है!\n\nइसे पाने के लिए, कृपया मुझे प्राइवेट में स्टार्ट करके मैसेज करें 👉 @{bot_username} और अपने कंटेंट का मज़ा लें।"
                    try:
                        await context.bot.send_message(chat_id=original_chat.id, text=fallback_text, parse_mode='HTML')
                        await update.message.reply_text(f"⚠️ यूजर ({original_user.first_name}) ने बॉट को स्टार्ट नहीं किया है। उसे ग्रुप में सूचित कर दिया गया है।")
                    except Exception as group_e:
                        logger.error(f"Could not send group message: {group_e}")
                        await update.message.reply_text("यूजर को प्राइवेट मैसेज नहीं भेजा जा सका और ग्रुप में भी मैसेज भेजने में समस्या आ रही है।")
                else:
                    await update.message.reply_text(f"⚠️ यूजर ({original_user.first_name}) ने बॉट को स्टार्ट नहीं किया है और मैं उन्हें ग्रुप के through भी नहीं बता सकता क्योंकि यह मैसेज किसी ग्रुप से फॉरवर्ड नहीं हुआ है।")
        else:
            await update.message.reply_text(f"'{movie_query}' अभी भी डेटाबेस में नहीं मिली। पहले उसे /addmovie कमांड से जोड़ें।")
    except Exception as e:
        logger.error(f"Error in handle_forward_to_notify: {e}")
        await update.message.reply_text(f"फॉरवर्ड मैसेज प्रोसेस करने में एरर आया: {str(e)}")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message or not update.message.text:
            return
        
        user_message = update.message.text.strip()
        logger.info(f"Received message: {user_message}")
        
        # Avoid processing commands
        if user_message.startswith('/'):
            return
        
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
            # Store the user's request with group and message context
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
            
            # Check if Gemini AI is available
            if not chat:
                # Fallback response if AI is not available
                fallback_responses = [
                    f"अरे यार! 😫 '{user_message}' तो अभी तक मेरे कलेक्शन में नहीं आई। पर टेंशन मत ले, जैसे ही आएगी, मैं तुझे सबसे पहले बताऊँगी। Pinky promise! ✨",
                    f"उफ़! '{user_message}' अभी तक मेरे पास नहीं है। लेकिन जैसे ही मिलेगी, मैं तुम्हें ज़रूर बताऊंगी!",
                    f"माफ करना, '{user_message}' अभी मेरे कलेक्शन में नहीं है। मैं इसे जल्द ही ढूंढने की कोशिश करूंगी!"
                ]
                response = random.choice(fallback_responses)
                
                # Add encouragement to start the bot
                encouragement = f"\n\n**Note:** यदि आप चाहते हैं कि आपकी फिल्म/वेब सीरीज उपलब्ध होते ही आपको सूचित किया जाए, तो कृपया मुझे private chat में start करें: @{context.bot.username}"
                
                await update.message.reply_text(response + encouragement)
                return
                
            try:
                # Use Gemini AI for conversation
                response = chat.send_message(user_message)
                ai_response = response.text
                
                # Add encouragement to start the bot for notifications
                if "नहीं आई" in ai_response or "not available" in ai_response.lower():
                    encouragement = f"\n\n**Note:** यदि आप चाहते हैं कि आपकी फिल्म/वेब सीरीज उपलब्ध होते ही आपको सूचित किया जाए, तो कृपया मुझे private chat में start करें: @{context.bot.username}"
                    ai_response += encouragement
                
                await update.message.reply_text(ai_response)
            except Exception as e:
                logger.error(f"Error from Gemini AI: {e}")
                # Fallback response if AI fails
                fallback_response = f"अरे यार! 😫 '{user_message}' तो अभी तक मेरे कलेक्शन में नहीं आई। पर टेंशन मत ले, जैसे ही आएगी, मैं तुझे सबसे पहले बताऊँगी। Pinky promise! ✨\n\n**Note:** यदि आप चाहते हैं कि आपकी फिल्म/वेब सीरीज उपलब्ध होते ही आपको सूचित किया जाए, तो कृपया मुझे private chat में start करें: @{context.bot.username}"
                await update.message.reply_text(fallback_response)
    except Exception as e:
        logger.error(f"Error in handle_message: {e}")


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

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("notify", notify_manually))
    application.add_handler(MessageHandler(filters.FORWARDED, handle_forward_to_notify))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot is starting polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# --- दोनों को एक साथ चलाएं ---
if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    run_bot()
