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
# Fix for Render's PostgreSQL connection string format
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
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
# >> DATABASE UTILITIES (With better error handling) <<
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
                    link TEXT NOT NULL,
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

# ... (rest of your code remains the same, including all the other functions)

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
