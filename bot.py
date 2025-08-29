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

# Get database connection details from environment
DB_HOST = os.getenv("DB_HOST", "dpg-d2k29h2li9vc73dqnq20-a")
DB_NAME = os.getenv("DB_NAME", "movie_library_zkwj")
DB_USER = os.getenv("DB_USER", "movie_library_zkwj_user")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")

# Construct DATABASE_URL from individual components
# This ensures the URL is always in the correct format for psycopg2.
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

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
# >> GREETINGS AND SPECIAL COMMANDS HANDLING <<
# ==============================================================================

GREETINGS = {
    'hello', 'hi', 'hey', 'hola', 'namaste', 'hello manvi', 'hi manvi', 'hey manvi',
    'hii', 'helloo', 'hellooo', 'hi there', 'hello there', 'hey there', 'good morning',
    'good afternoon', 'good evening', 'good night', 'sup', 'what\'s up', 'yo'
}

async def handle_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles greeting messages with personalized responses."""
    user = update.effective_user
    chat = update.effective_chat
    
    if chat.type in [Chat.GROUP, Chat.SUPERGROUP]:
        greeting_message = random.choice([
            "Hey everyone! 👋 Manvi here! What movie are you looking for today?",
            "Hi friends! 😊 Ready to find some awesome movies? Just tell me what you're looking for!",
            "Hello movie lovers! 🎬 Your expert Manvi is here! What can I find for you?",
            "Hey there! 🍿 Manvi at your service! What movie would you like to watch?",
            "Hi everyone! ✨ Manvi here! Let's find something great to watch! What's on your mind?"
        ])
    else:
        greeting_message = random.choice([
            f"Hey {user.first_name}! 👋 Manvi here! What movie are you looking for today?",
            f"Hi {user.first_name}! 😊 Ready to find some awesome movies? Just tell me what you're looking for!",
            f"Hello {user.first_name}! 🎬 Your movie expert Manvi is here! What can I find for you?",
            f"Hey there {user.first_name}! 🍿 Manvi at your service! What movie would you like to watch?",
            f"Hi {user.first_name}! ✨ Manvi here! Let's find something great to watch! What's on your mind?"
        ])
    await update.message.reply_text(greeting_message)

# ==============================================================================
# >> DATABASE UTILITIES <<
# ==============================================================================

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL is not set")
        return None
        
    # Debug the DATABASE_URL format (without exposing credentials)
    db_url_for_logging = DATABASE_URL
    if "@" in DATABASE_URL:
        # Mask the password in the URL for security
        parts = DATABASE_URL.split("@")
        user_pass = parts[0].split("//")[1]
        if ":" in user_pass:
            user, password = user_pass.split(":", 1)
            masked_url = DATABASE_URL.replace(password, "***")
            db_url_for_logging = masked_url
            
    logger.info(f"Attempting to connect to database: {db_url_for_logging}")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"🚨 Could not connect to the database: {e}")
        return None

def setup_database():
    """Creates the necessary tables if they don't already exist."""
    logger.info("Setting up database...")
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to connect to database during setup")
        return
    
    try:
        with conn.cursor() as cur:
            # Check if movies table exists and has the right structure
            cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'movies';
            """)
            columns = {row[0] for row in cur.fetchall()}
            
            if not columns:
                # Table doesn't exist, create it
                logger.info("Creating movies table...")
                cur.execute("""
                CREATE TABLE movies (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    link TEXT,
                    CONSTRAINT unique_movie_title UNIQUE (title)
                );
                """)
            elif 'link' not in columns:
                # Table exists but missing link column
                logger.info("Adding link column to movies table...")
                cur.execute("ALTER TABLE movies ADD COLUMN link TEXT;")
            
            # Check if user_requests table exists
            cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'user_requests';
            """)
            columns = {row[0] for row in cur.fetchall()}
            
            if not columns:
                # Table doesn't exist, create it
                logger.info("Creating user_requests table...")
                cur.execute("""
                CREATE TABLE user_requests (
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
        logger.info("Database tables verified/created successfully. 🐘")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        conn.rollback()
    finally:
        conn.close()

def search_movie(title: str) -> Dict[str, Any] | None:
    """Searches for a movie in the database with case-insensitive partial matching."""
    conn = get_db_connection()
    if not conn: return None
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT title, link FROM movies WHERE title ILIKE %s;", (f'%{title}%',))
            result = cur.fetchone()
        return result
    except Exception as e:
        logger.error(f"Error searching for movie: {e}")
        return None
    finally:
        conn.close()

def add_movie(title: str, link: str) -> bool:
    """Adds or updates a movie in the database. Returns True if a new movie was added."""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO movies (title, link) VALUES (%s, %s)
            ON CONFLICT (title) DO UPDATE SET link = EXCLUDED.link
            RETURNING (xmax = 0) AS inserted;
            """, (title.strip(), link.strip()))
            
            result = cur.fetchone()
            is_new = result[0] if result else False
        conn.commit()
        return is_new
    except Exception as e:
        logger.error(f"Error adding movie: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def add_user_request(user_id: int, username: str, movie_title: str, group_id: int | None):
    """Adds a user's movie request to the database, avoiding duplicates."""
    conn = get_db_connection()
    if not conn: return
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO user_requests (user_id, username, movie_title, group_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, movie_title) DO NOTHING;
            """, (user_id, username, movie_title.strip(), group_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding user request: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_pending_requests(movie_title: str) -> list[Dict[str, Any]]:
    """Gets all users who requested a specific movie and have not been notified."""
    conn = get_db_connection()
    if not conn: return []
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
            SELECT user_id, username, group_id FROM user_requests
            WHERE movie_title ILIKE %s AND notified = FALSE;
            """, (f'%{movie_title}%',))
            results = cur.fetchall()
        return results
    except Exception as e:
        logger.error(f"Error getting pending requests: {e}")
        return []
    finally:
        conn.close()

def mark_request_as_notified(user_id: int, movie_title: str):
    """Marks a user's request as notified."""
    conn = get_db_connection()
    if not conn: return
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE user_requests SET notified = TRUE
            WHERE user_id = %s AND movie_title ILIKE %s;
            """, (user_id, f'%{movie_title}%'))
        conn.commit()
    except Exception as e:
        logger.error(f"Error marking request as notified: {e}")
        conn.rollback()
    finally:
        conn.close()

# ==============================================================================
# >> MANVI'S PERSONALITY & AI INTEGRATION <<
# ==============================================================================

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
        "Arre yaar! 😫 This one isn't in my collection yet. But don't worry, I'll let you know first as soon as I get it. Pinky promise! 🤙",
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
# >> NOTIFICATION & BLOGGER LOGIC <<
# ==============================================================================

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
# >> TELEGRAM HANDLERS <<
# ==============================================================================

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

    is_new = add_movie(title, link)
    if is_new:
        await update.message.reply_text(f"Alright, boss! Added '{title}'. Notifying users now...")
        asyncio.create_task(notify_users(context, title, link))
    else:
        await update.message.reply_text(f"Updated '{title}' in the database.")


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
    
    user_message = update.message.text.strip().lower()
    user: User = update.effective_user
    chat: Chat = update.effective_chat
    
    if not user_message:
        return
        
    # Check if it's a greeting
    if user_message in GREETINGS:
        await handle_greeting(update, context)
        return
        
    # Check if it's a thank you message
    if any(word in user_message for word in ['thank', 'thanks', 'gracias', 'merci']):
        await update.message.reply_text("You're welcome! 😊 Let me know if you need anything else!")
        return
        
    # Check if it's a how are you message
    if any(word in user_message for word in ['how are you', 'how do you do', 'how\'s it going']):
        await update.message.reply_text("I'm doing great! Ready to help you find some awesome movies! 🎬 What are you in the mood for?")
        return
    
    logger.info(f"User {user.id} ({user.username}) in chat {chat.id} searched for: '{user_message}'")
    
    movie = search_movie(user_message)
    
    if movie:
        title = movie['title']
        link = movie['link']
        
        response_text = await generate_response('movie_found', movie_title=title)
        
        # Validate the link before creating the button
        if link and link.startswith(('http://', 'https://')):
            try:
                keyboard = [[InlineKeyboardButton("🍿 Get Link / Watch Now 🍿", url=link)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(
                    response_text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.MARKDOWN
                )
            except BadRequest as e:
                logger.error(f"Failed to create inline keyboard for movie '{title}': {e}")
                # Fallback: send the link as plain text
                response_text += f"\n\n🔗 Link: {link}"
                await update.message.reply_text(
                    response_text,
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            logger.warning(f"Invalid link for movie '{title}': {link}")
            response_text += f"\n\nSorry, the link for this movie is not available right now."
            await update.message.reply_text(
                response_text,
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
    # Check for essential environment variables
    missing_vars = []
    if not TELEGRAM_BOT_TOKEN:
        missing_vars.append("TELEGRAM_BOT_TOKEN")
    if not DATABASE_URL:
        missing_vars.append("DATABASE_URL")
    if not ADMIN_USER_ID:
        missing_vars.append("ADMIN_USER_ID")
    if not APP_BASE_URL:
        missing_vars.append("APP_BASE_URL")
    
    if missing_vars:
        logger.critical(f"🚨 CRITICAL: Missing essential environment variables: {', '.join(missing_vars)}. Bot cannot start.")
        return

    # Ensure database is ready before starting
    setup_database()
    
    # Set up the Telegram bot application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Add handlers
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
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=webhook_path,
        webhook_url=webhook_full_url
    )

if __name__ == "__main__":
    main()
