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

# ----------------------  Logging  ----------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----------------------  Character Prompt  ----------------------
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

# ----------------------  Env variables  ----------------------
TELEGRAM_BOT_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY       = os.environ.get("GEMINI_API_KEY")
DATABASE_URL         = os.environ.get("DATABASE_URL")
BLOGGER_API_KEY      = os.environ.get("BLOGGER_API_KEY")
BLOG_ID              = os.environ.get("BLOG_ID")
UPDATE_SECRET_CODE   = os.environ.get("UPDATE_SECRET_CODE", "default_secret_123")
ADMIN_USER_ID        = int(os.environ.get("ADMIN_USER_ID", 0))
GROUP_CHAT_ID        = int(os.environ.get("GROUP_CHAT_ID", 0))   # NEW

# ----------------------  Validate  ----------------------
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set.")

# ----------------------  DB Setup  ----------------------
def setup_database():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS movies (id SERIAL PRIMARY KEY, title TEXT NOT NULL UNIQUE, url TEXT NOT NULL);')
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
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                ALTER TABLE user_requests ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
            END IF;
            END $$;
        ''')
        conn.commit()
        cur.close(); conn.close()
        logger.info("Database setup completed")
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise RuntimeError("Database setup failed")

# ----------------------  Auto-update Movies  ----------------------
def update_movies_in_db():
    logger.info("Starting movie update...")
    setup_database()
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT title FROM movies;")
    existing = {row[0] for row in cur.fetchall()}
    try:
        service = build('blogger', 'v3', developerKey=BLOGGER_API_KEY)
        items = []
        posts_req = service.posts().list(blogId=BLOG_ID, maxResults=500)
        while posts_req:
            posts = posts_req.execute()
            items.extend(posts.get('items', []))
            posts_req = service.posts().list_next(posts_req, posts)
        pages = service.pages().list(blogId=BLOG_ID).execute()
        items.extend(pages.get('items', []))
        added = 0
        for item in items:
            title, url = item.get('title'), item.get('url')
            if title and url and title.strip() not in existing:
                try:
                    cur.execute("INSERT INTO movies (title, url) VALUES (%s, %s);", (title.strip(), url.strip()))
                    added += 1
                except psycopg2.Error as e:
                    logger.error(f"Insert error: {e}")
        conn.commit()
        return f"Update complete. Added {added} new items."
    except Exception as e:
        logger.error(f"Update error: {e}")
        return f"Update error: {e}"
    finally:
        cur.close(); conn.close()

def get_movie_from_db(query):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", (query + '%',))
    movie = cur.fetchone()
    if not movie:
        cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 1", ('%' + query + '%',))
        movie = cur.fetchone()
    cur.close(); conn.close()
    return movie

# ----------------------  Flask  ----------------------
flask_app = Flask('')
@flask_app.route('/')
def home():
    return "Bot is running!"
@flask_app.route(f'/{UPDATE_SECRET_CODE}')
def trigger_update():
    return update_movies_in_db()
def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port)

# ----------------------  Gemini  ----------------------
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(model_name='gemini-1.5-flash')
        chat = model.start_chat(history=[
            {'role': 'user', 'parts': [CHARACTER_PROMPT]},
            {'role': 'model', 'parts': ["Okay, I am Manvi."]}
        ])
        logger.info("Gemini initialized")
    except Exception as e:
        logger.error(f"Gemini init failed: {e}")
        chat = None
else:
    chat = None
    logger.warning("Gemini not initialized")

# ----------------------  Notify functions  ----------------------
def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO user_requests 
               (user_id, username, first_name, movie_title, group_id, message_id) 
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (user_id, movie_title) DO NOTHING""",
            (user_id, username, first_name, movie_title, group_id, message_id)
        )
        conn.commit()
        cur.close(); conn.close()
        return True
    except Exception as e:
        logger.error(f"Storing request failed: {e}")
        return False

async def notify_users_for_movie(context, movie_title, movie_url):
    logger.info(f"Notifying users for: {movie_title}")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, username, first_name FROM user_requests WHERE movie_title ILIKE %s AND notified = FALSE",
        (f'%{movie_title}%',)
    )
    users = cur.fetchall()
    notified = 0
    for uid, uname, fname in users:
        try:
            await context.bot.send_message(chat_id=uid, text=f"Hey {fname or uname}! आपकी requested movie '{movie_title}' अब available है! 🎬")
            if movie_url.startswith("https://t.me/c/"):
                parts = movie_url.split('/')
                from_chat_id = int("-100" + parts[-2])
                msg_id = int(parts[-1])
                await context.bot.copy_message(chat_id=uid, from_chat_id=from_chat_id, message_id=msg_id)
            elif movie_url.startswith("http"):
                await context.bot.send_message(chat_id=uid, text=movie_url)
            else:
                await context.bot.send_document(chat_id=uid, document=movie_url)
            cur.execute("UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s", (uid, f'%{movie_title}%'))
            conn.commit()
            notified += 1
        except Exception as e:
            logger.error(f"Notify user {uid}: {e}")
    cur.close(); conn.close()
    return notified

# ----------------------  Telegram Handlers  ----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("क्या हाल है? मैं मानवी। 😉 फिल्मों पर गपशॉप करनी है तो बता।")

async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addmovie Title [file_id or link]")
        return
    value = context.args[-1]
    title = " ".join(context.args[:-1])
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO movies (title, url) VALUES (%s, %s) ON CONFLICT (title) DO UPDATE SET url = EXCLUDED.url",
        (title.strip(), value.strip())
    )
    conn.commit(); cur.close(); conn.close()
    await update.message.reply_text(f"'{title}' added. ✅")
    n = await notify_users_for_movie(context, title, value)
    await update.message.reply_text(f"{n} users notified.")

async def notify_manually(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry, सिर्फ एडमिन ही इस कमांड का इस्तेमाल कर सकते हैं।")
        return
    if not context.args:
        await update.message.reply_text("Usage: /notify <movie_title>")
        return
    title = " ".join(context.args)
    found = get_movie_from_db(title)
    if found:
        t, v = found
        n = await notify_users_for_movie(context, t, v)
        await update.message.reply_text(f"{n} users notified for '{t}'.")
    else:
        await update.message.reply_text(f"'{title}' not found in DB.")

# NEW COMMAND: /group
async def group_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Only admin can use /group.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /group <user_name> <movie_title>")
        return
    # The first arg is the user_name, rest is the movie title
    user_name = context.args[0]
    movie_title = " ".join(context.args[1:])
    found = get_movie_from_db(movie_title)
    if not found:
        await update.message.reply_text(f"'{movie_title}' not found in DB.")
        return
    _, movie_url = found
    try:
        msg = f"Hi {user_name}, आपकी Movie title \"{movie_title}\" अब available है! {movie_url} Enjoy.🙂"
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=msg)
        await update.message.reply_text("Notification sent to the group.")
    except Exception as e:
        logger.error(f"Group notify error: {e}")
        await update.message.reply_text("Failed to send group notification.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or update.message.text.startswith('/'):
        return
    text = update.message.text.strip()
    movie = get_movie_from_db(text)
    if movie:
        title, value = movie
        if value.startswith("https://t.me/c/"):
            parts = value.split('/')
            from_chat_id = int("-100" + parts[-2])
            msg_id = int(parts[-1])
            await update.message.reply_text("मिल गई! 😉 भेजी जा रही है...")
            await context.bot.copy_message(chat_id=update.effective_chat.id, from_chat_id=from_chat_id, message_id=msg_id)
        elif value.startswith("http"):
            await update.message.reply_text(f"ये ले 🔥 '{title}': {value}")
        else:
            await update.message.reply_text("मिल गई! 😉 भेजी जा रही है...")
            await context.bot.send_document(chat_id=update.effective_chat.id, document=value)
    else:
        user = update.effective_user
        store_user_request(user.id, user.username, user.first_name, text)
        fallback = f"अरे यार! 😫 '{text}' अभी मेरे कलेक्शन में नहीं है। जैसे ही आएगी, मैं तुझे बताऊँगी। Pinky promise! ✨\n\n**Note:** मुझे प्राइवेट में start करें ताकि मिलते ही notify मिले: @{context.bot.username}"
        await update.message.reply_text(fallback)

# ----------------------  Run Bot  ----------------------
def run_bot():
    setup_database()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addmovie", add_movie))
    app.add_handler(CommandHandler("notify", notify_manually))
    app.add_handler(CommandHandler("group", group_notify))   # NEW
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Bot started polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

# ----------------------  Main  ----------------------
if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    run_bot()
