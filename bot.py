# bot.py (Webhook Version)
import os
import logging
import asyncio
import random
from typing import Dict, Any

# --- Third-party libraries ---
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup, Chat, User
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode
from telegram.error import Forbidden, BadRequest

import google.generativeai as genai
from googleapiclient.discovery import build

# --- Load Environment Variables ---
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
BLOGGER_API_KEY = os.getenv("BLOGGER_API_KEY")
BLOG_ID = os.getenv("BLOG_ID")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

# The port number is provided by Render automatically
PORT = int(os.getenv("PORT", 8443))
# The public URL of your app is also provided by Render
APP_BASE_URL = os.getenv("RENDER_EXTERNAL_URL")


# --- Logging Configuration ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Gemini AI Configuration ---
if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-1.5-flash')
        logger.info("Gemini AI model loaded successfully. 🤖")
    except Exception as e:
        logger.error(f"Failed to initialize Gemini AI: {e}")
        gemini_model = None
else:
    gemini_model = None

# ==============================================================================
# >> DATABASE UTILITIES (No changes in this section) <<
# ==============================================================================

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"🚨 Could not connect to the database: {e}")
        return None

def setup_database():
    """Creates the necessary tables if they don't already exist."""
    conn = get_db_connection()
    if not conn:
        return
    
    with conn.cursor() as cur:
        # Movies table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            link TEXT NOT NULL,
            CONSTRAINT unique_movie_title UNIQUE (title)
        );
        """)
        
        # User requests table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_requests (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            username VARCHAR(255),
            movie_title TEXT NOT NULL,
            group_id BIGINT,
            notified BOOLEAN DEFAULT FALSE,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT unique_user_request UNIQUE (user_id, movie_title)
        );
        """)
    conn.commit()
    conn.close()
    logger.info("Database tables verified/created successfully. 🐘")

def search_movie(title: str) -> Dict[str, Any] | None:
    """Searches for a movie in the database with case-insensitive partial matching."""
    conn = get_db_connection()
    if not conn: return None
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT title, link FROM movies WHERE title ILIKE %s;", (f'%{title}%',))
        result = cur.fetchone()
    conn.close()
    return result

def add_movie(title: str, link: str) -> bool:
    """Adds or updates a movie in the database. Returns True if a new movie was added."""
    conn = get_db_connection()
    if not conn: return False
    
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO movies (title, link) VALUES (%s, %s)
        ON CONFLICT (title) DO UPDATE SET link = EXCLUDED.link;
        """, (title.strip(), link.strip()))
        
        is_new = cur.rowcount > 0
    conn.commit()
    conn.close()
    return is_new

def add_user_request(user_id: int, username: str, movie_title: str, group_id: int | None):
    """Adds a user's movie request to the database, avoiding duplicates."""
    conn = get_db_connection()
    if not conn: return
    
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO user_requests (user_id, username, movie_title, group_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id, movie_title) DO NOTHING;
        """, (user_id, username, movie_title.strip(), group_id))
    conn.commit()
    conn.close()

def get_pending_requests(movie_title: str) -> list[Dict[str, Any]]:
    """Gets all users who requested a specific movie and have not been notified."""
    conn = get_db_connection()
    if not conn: return []
    
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
        SELECT user_id, username, group_id FROM user_requests
        WHERE movie_title ILIKE %s AND notified = FALSE;
        """, (f'%{movie_title}%',))
        results = cur.fetchall()
    conn.close()
    return results

def mark_request_as_notified(user_id: int, movie_title: str):
    """Marks a user's request as notified."""
    conn = get_db_connection()
    if not conn: return
    
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE user_requests SET notified = TRUE
        WHERE user_id = %s AND movie_title ILIKE %s;
        """, (user_id, f'%{movie_title}%'))
    conn.commit()
    conn.close()

# ==============================================================================
# >> MANVI'S PERSONALITY & AI INTEGRATION (No changes in this section) <<
# ==============================================================================
# ... (This whole section is the same as before, no need to copy it here again)
FALLBACK_RESPONSES = {
    'welcome': [
        "Hey there! Manvi here. 😉 What movie are you looking for today? Just type the name!",
        "Hi! I'm Manvi. Your go-to girl for movies and series. What can I find for you?",
        "Manvi reporting for duty! 🫡 Ready to find some awesome movies. What's on your mind?",
    ],
    'movie_found': [
        "Yesss! Found it for you. Here you go, enjoy! 🎉",
        "Gotcha! One {movie_title} coming right up. 😉",
        "Look what I found! ✨ Hope you like it.",
    ],
    'movie_not_found': [
        "Arre yaar! 😫 This one isn’t in my collection yet. But don’t worry, I’ll let you know first as soon as I get it. Pinky promise! 🤙",
        "Uff, sorry! 😥 I couldn't find '{movie_title}'. I've added it to my list and will ping you the moment it's here.",
        "Oh no! My collection is missing this gem. 💎 But I promise, you'll be the first to know when I find it.",
    ],
    'admin_permission_denied': [
        "Hehe, nice try! 😉 But that's an admin-only command. You can't fool Manvi!",
        "Oops! That's a secret command for my boss. Not for you! 😜",
    ]
}

async def generate_response(prompt_type: str, **kwargs) -> str:
    """Generates a witty response using Gemini AI or a fallback."""
    fallback = random.choice(FALLBACK_RESPONSES.get(prompt_type, [""]))
    if kwargs:
        fallback = fallback.format(**kwargs)

    if not gemini_model:
        return fallback

    prompts = {
        'welcome': "You are Manvi, a witty 17-year-old girl. Write a short, friendly welcome message (in Hinglish) for a user who just started the bot.",
        'movie_found': f"You are Manvi, a witty 17-year-old girl. You just found the movie '{kwargs.get('movie_title')}' for a user. Write a short, excited message (in Hinglish) to send with the link.",
        'movie_not_found': f"You are Manvi, a witty 17-year-old girl. A user asked for '{kwargs.get('movie_title')}', but you don't have it. Write a short, cheeky, sad-but-hopeful reply (in Hinglish). Promise to notify them.",
    }
    
    prompt = prompts.get(prompt_type)
    if not prompt:
        return fallback

    try:
        response = await gemini_model.generate_content_async(prompt)
        return response.text.strip().replace('*', '')
    except Exception as e:
        logger.error(f"Gemini AI generation failed: {e}")
        return fallback

# ==============================================================================
# >> NOTIFICATION & BLOGGER LOGIC (No changes in this section) <<
# ==============================================================================
# ... (This whole section is the same as before, no need to copy it here again)
async def notify_users(context: ContextTypes.DEFAULT_TYPE, title: str, link: str):
    """Finds and notifies users waiting for a specific movie."""
    logger.info(f"Starting notification process for movie: {title}")
    pending_users = get_pending_requests(title)
    
    if not pending_users:
        logger.info(f"No pending requests found for '{title}'.")
        return

    users_to_notify_in_group = {}
    success_count = 0

    for user in pending_users:
        user_id = user['user_id']
        username = user['username']
        group_id = user['group_id']
        
        message_text = (
            f"Hey {username}! Good news! ✨\n\n"
            f"You asked for **{title}**, and I've finally found it for you. Here's the link:\n\n"
            f"🔗 {link}\n\n"
            "Enjoy the movie! 😉"
        )
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode=ParseMode.MARKDOWN
            )
            success_count += 1
            mark_request_as_notified(user_id, title)
            await asyncio.sleep(0.1)
        except Forbidden:
            logger.warning(f"User {user_id} has blocked the bot. Queuing for group notification.")
            if group_id:
                if group_id not in users_to_notify_in_group:
                    users_to_notify_in_group[group_id] = []
                users_to_notify_in_group[group_id].append(f"@{username}" if username else f"user `{user_id}`")
        except BadRequest as e:
            logger.error(f"Failed to send message to {user_id}: {e}")
            mark_request_as_notified(user_id, title)

    logger.info(f"Successfully notified {success_count} users in private.")

    for group_id, mentions in users_to_notify_in_group.items():
        mention_string = ", ".join(mentions)
        group_message = (
            f"Hey {mention_string}! 👋\n\n"
            f"The movie **{title}** you asked for is now available!\n\n"
            "Start me in a private chat so I can send you the link. I tried, but it seems you've blocked me. 😔"
        )
        try:
            await context.bot.send_message(
                chat_id=group_id,
                text=group_message,
                parse_mode=ParseMode.MARKDOWN
            )
            for user in pending_users:
                if user['group_id'] == group_id:
                    mark_request_as_notified(user['user_id'], title)
        except Exception as e:
            logger.error(f"Failed to send group notification to {group_id}: {e}")

# ==============================================================================
# >> TELEGRAM HANDLERS (No changes in this section) <<
# ==============================================================================
# ... (This whole section is the same as before, no need to copy it here again)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /start command."""
    welcome_message = await generate_response('welcome')
    await update.message.reply_text(welcome_message)

async def addmovie_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a movie and trigger notifications."""
    if update.effective_user.id != ADMIN_USER_ID:
        reply_text = random.choice(FALLBACK_RESPONSES['admin_permission_denied'])
        await update.message.reply_text(reply_text)
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: `/addmovie <Movie Title> <Link>`")
        return
        
    link = context.args[-1]
    title = " ".join(context.args[:-1])
    
    if not link.startswith(('http://', 'https://')):
         await update.message.reply_text("That doesn't look like a valid link.")
         return

    add_movie(title, link)
    await update.message.reply_text(f"Alright, boss! Added/updated '{title}'. Notifying users now...")
    
    asyncio.create_task(notify_users(context, title, link))


async def notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to manually trigger notification for an existing movie."""
    if update.effective_user.id != ADMIN_USER_ID:
        reply_text = random.choice(FALLBACK_RESPONSES['admin_permission_denied'])
        await update.message.reply_text(reply_text)
        return

    if not context.args:
        await update.message.reply_text("Usage: `/notify <Movie Title>`")
        return
        
    title = " ".join(context.args)
    movie = search_movie(title)
    
    if not movie:
        await update.message.reply_text(f"Couldn't find '{title}' in the database.")
        return

    await update.message.reply_text(f"Okay! Starting notification for '{movie['title']}'.")
    asyncio.create_task(notify_users(context, movie['title'], movie['link']))

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles regular text messages from users to search for movies."""
    if not update.message or not update.message.text:
        return
    
    user_message = update.message.text.strip()
    user: User = update.effective_user
    chat: Chat = update.effective_chat
    
    if not user_message:
        return
        
    logger.info(f"User {user.id} ({user.username}) in chat {chat.id} searched for: '{user_message}'")
    
    movie = search_movie(user_message)
    
    if movie:
        title = movie['title']
        link = movie['link']
        
        response_text = await generate_response('movie_found', movie_title=title)
        keyboard = [[InlineKeyboardButton("🍿 Get Link / Watch Now 🍿", url=link)]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            response_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        group_id = chat.id if chat.type in [Chat.GROUP, Chat.SUPERGROUP] else None
        add_user_request(user.id, user.username, user_message, group_id)
        
        response_text = await generate_response('movie_not_found', movie_title=user_message)
        await update.message.reply_text(response_text)

async def handle_forwarded_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles forwarded messages from the admin to notify a user privately."""
    if update.effective_user.id != ADMIN_USER_ID:
        return
    
    forwarded_from_user = update.message.forward_from
    if not forwarded_from_user:
        await update.message.reply_text("Cannot see original sender due to privacy settings.")
        return
        
    movie_title_request = update.message.text
    logger.info(f"Admin forwarded a request for '{movie_title_request}' from user {forwarded_from_user.id}")
    
    movie = search_movie(movie_title_request)
    
    if movie:
        title = movie['title']
        link = movie['link']
        message_text = (
            f"Hey! The admin saw your request. 😉\n\n"
            f"Here is the link for **{title}**:\n\n{link}"
        )
        try:
            await context.bot.send_message(
                chat_id=forwarded_from_user.id,
                text=message_text,
                parse_mode=ParseMode.MARKDOWN
            )
            await update.message.reply_text(f"Successfully sent the link to {forwarded_from_user.full_name}.")
            mark_request_as_notified(forwarded_from_user.id, title)
        except Forbidden:
            await update.message.reply_text(f"Couldn't message {forwarded_from_user.full_name}. They might have me blocked. 😔")
        except Exception as e:
            await update.message.reply_text(f"Something went wrong: {e}")
    else:
        await update.message.reply_text(f"Couldn't find anything matching '{movie_title_request}'.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs errors caused by updates."""
    logger.error("Exception while handling an update:", exc_info=context.error)

# ==============================================================================
# >> MAIN BOT EXECUTION (Webhook Version) <<
# ==============================================================================

def main() -> None:
    """Initializes and runs the bot via Webhook."""
    if not all([TELEGRAM_BOT_TOKEN, DATABASE_URL, ADMIN_USER_ID, APP_BASE_URL]):
        logger.critical("🚨 CRITICAL: Missing essential environment variables. Bot cannot start.")
        return

    # Ensure database is ready before starting
    setup_database()
    
    # Set up the Telegram bot application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add handlers (same as before)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("addmovie", addmovie_command))
    application.add_handler(CommandHandler("notify", notify_command))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_text_message
    ))
    application.add_handler(MessageHandler(
        filters.FORWARDED & filters.User(user_id=ADMIN_USER_ID), handle_forwarded_message
    ))
    application.add_error_handler(error_handler)
    
    # We use the bot token as a secret path to make sure nobody else can send updates
    webhook_path = f"/{TELEGRAM_BOT_TOKEN}"
    webhook_full_url = f"{APP_BASE_URL}{webhook_path}"

    # Set up and run the webhook server
    # This single command sets the webhook and starts a web server
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_path,
        webhook_url=webhook_full_url
    )

if __name__ == "__main__":
    main()
