import os
import asyncio
import logging
import json
import re
import time
import hashlib
import threading
from datetime import datetime
from collections import defaultdict
from contextlib import contextmanager

# Optional/3rd-party
import redis
import google.generativeai as genai
import psycopg2
from psycopg2 import pool as pg_pool
import sqlite3

# Telegram (python-telegram-bot v20)
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ChatAction
from telegram.error import Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# ========= Logging =========
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()],
)
logger = logging.getLogger("MovieFinder")

# ========= Config =========
class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

    # If using Supabase/other Postgres, include sslmode=require in DSN or we’ll add it.
    DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

    # Optional Redis
    REDIS_URL = os.environ.get("REDIS_URL", "").strip()

    # Admins/Channels
    ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", "0"))
    ADMIN_CHANNEL_ID = os.environ.get("ADMIN_CHANNEL_ID", "").strip()

    # App tuning
    REQUEST_LIMIT = int(os.environ.get("REQUEST_LIMIT", "20"))
    REQUEST_WINDOW = int(os.environ.get("REQUEST_WINDOW", "3600"))  # seconds

    # Local fallback DB path (ephemeral on Render)
    SQLITE_PATH = os.environ.get("SQLITE_PATH", "/tmp/moviefinder.db")


# ========= Conversation States =========
class States:
    MAIN_MENU, SEARCHING, REQUESTING, FEEDBACK = range(4)


# ========= Optional Redis =========
redis_conn = None
if Config.REDIS_URL:
    try:
        redis_conn = redis.from_url(Config.REDIS_URL, decode_responses=True)
        redis_conn.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        redis_conn = None

# In-memory rate limit fallback (per-process)
class MemoryRateLimiter:
    def __init__(self, limit: int, window_seconds: int):
        self.limit = limit
        self.window = window_seconds
        self._hits = defaultdict(list)  # user_id -> [timestamps]
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.time()
        with self._lock:
            bucket = self._hits[key]
            # drop old
            cutoff = now - self.window
            while bucket and bucket[0] < cutoff:
                bucket.pop(0)
            if len(bucket) >= self.limit:
                return False
            bucket.append(now)
            return True

memory_limiter = MemoryRateLimiter(Config.REQUEST_LIMIT, Config.REQUEST_WINDOW)


# ========= Database Abstraction (Postgres with SSL, fallback to SQLite) =========
class Database:
    _mode = None  # "postgres" or "sqlite"
    _pg_pool: pg_pool.SimpleConnectionPool | None = None
    _sqlite_conn: sqlite3.Connection | None = None
    _lock = threading.Lock()

    @classmethod
    def init(cls) -> None:
        # Try Postgres first
        dsn = Config.DATABASE_URL
        if dsn:
            try:
                # Ensure sslmode=require for Supabase-like hosts
                # psycopg2.connect(dsn, sslmode="require") works even for URL DSNs.
                cls._pg_pool = pg_pool.SimpleConnectionPool(
                    1, 10, dsn, sslmode="require", connect_timeout=10
                )
                cls._mode = "postgres"
                logger.info("Postgres pool initialized")
                cls._create_tables()
                return
            except Exception as e:
                logger.error(f"Postgres unavailable ({e}); falling back to SQLite.")

        # Fallback: SQLite
        try:
            cls._sqlite_conn = sqlite3.connect(
                Config.SQLITE_PATH, check_same_thread=False, isolation_level=None
            )
            cls._sqlite_conn.execute("PRAGMA foreign_keys = ON;")
            cls._mode = "sqlite"
            logger.info(f"SQLite initialized at {Config.SQLITE_PATH}")
            cls._create_tables()
        except Exception as e:
            logger.critical(f"Failed to initialize any database: {e}")
            raise

    @classmethod
    def mode(cls) -> str:
        if not cls._mode:
            cls.init()
        return cls._mode

    @classmethod
    @contextmanager
    def connection(cls):
        if cls.mode() == "postgres":
            conn = None
            try:
                conn = cls._pg_pool.getconn()
                yield conn
                conn.commit()
            except Exception:
                if conn:
                    conn.rollback()
                raise
            finally:
                if conn:
                    cls._pg_pool.putconn(conn)
        else:
            # SQLite is autocommit when isolation_level=None; use explicit BEGIN for write if needed
            yield cls._sqlite_conn

    @classmethod
    def _create_tables(cls) -> None:
        mode = cls.mode()
        if mode == "postgres":
            stmts = [
                """
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    file_id TEXT,
                    quality TEXT,
                    size TEXT,
                    language TEXT,
                    year INTEGER,
                    imdb_rating FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(title, quality, language)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS movie_aliases (
                    id SERIAL PRIMARY KEY,
                    movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                    alias TEXT NOT NULL,
                    UNIQUE(movie_id, alias)
                );
                """,
                """
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
                    priority INTEGER DEFAULT 1,
                    UNIQUE(user_id, movie_title)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    search_count INTEGER DEFAULT 0,
                    request_count INTEGER DEFAULT 0,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS feedback (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    message TEXT NOT NULL,
                    rating INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
            ]
        else:
            stmts = [
                """
                CREATE TABLE IF NOT EXISTS movies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    file_id TEXT,
                    quality TEXT,
                    size TEXT,
                    language TEXT,
                    year INTEGER,
                    imdb_rating REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(title, quality, language)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS movie_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    movie_id INTEGER NOT NULL,
                    alias TEXT NOT NULL,
                    UNIQUE(movie_id, alias),
                    FOREIGN KEY(movie_id) REFERENCES movies(id) ON DELETE CASCADE
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS user_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    movie_title TEXT NOT NULL,
                    requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    notified INTEGER DEFAULT 0,
                    group_id INTEGER,
                    message_id INTEGER,
                    priority INTEGER DEFAULT 1,
                    UNIQUE(user_id, movie_title)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    search_count INTEGER DEFAULT 0,
                    request_count INTEGER DEFAULT 0,
                    last_active DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    message TEXT NOT NULL,
                    rating INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """,
            ]
        with cls.connection() as conn:
            cur = conn.cursor()
            for s in stmts:
                cur.execute(s)
            cur.close()

    @classmethod
    def _adapt_sql(cls, query: str):
        # Convert %s placeholders to ? for SQLite
        if cls.mode() == "sqlite":
            return query.replace("%s", "?")
        return query

    @classmethod
    def execute(cls, query: str, params=None, fetch: bool = False):
        query = cls._adapt_sql(query)
        with cls.connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params or [])
            result = cur.fetchall() if fetch else None
            cur.close()
            return result


# ========= AI Intent (optional Gemini) =========
class AIAssistant:
    def __init__(self):
        self.model = None
        if Config.GEMINI_API_KEY:
            try:
                genai.configure(api_key=Config.GEMINI_API_KEY)
                self.model = genai.GenerativeModel(model_name="gemini-pro")
                logger.info("Gemini initialized")
            except Exception as e:
                logger.warning(f"Gemini init failed: {e}")
                self.model = None

    async def analyze_intent(self, message_text: str):
        if not self.model:
            return self._fallback(message_text)
        try:
            prompt = (
                'Analyze if this message is requesting a movie or series: "'
                + message_text
                + '" Respond with JSON: {"is_request": boolean, "content_title": string|null, "confidence": float}'
            )
            # Gemini SDK is synchronous; safe to call in thread
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, self.model.generate_content, prompt)
            # Extract JSON
            match = re.search(r"\{.*\}", response.text or "", re.DOTALL)
            if match:
                return json.loads(match.group())
            return self._fallback(message_text)
        except Exception as e:
            logger.warning(f"AI analysis failed: {e}")
            return self._fallback(message_text)

    def _fallback(self, message_text: str):
        movie_keywords = ["movie", "film", "series", "watch", "download", "see", "चलचित्र", "फिल्म", "सीरीज"]
        if any(k in message_text.lower() for k in movie_keywords):
            return {"is_request": True, "content_title": message_text, "confidence": 0.7}
        return {"is_request": False, "content_title": None, "confidence": 0.6}


# ========= User tracking =========
class UserManager:
    @staticmethod
    def track_activity(user_id, username, first_name, action_type):
        try:
            Database.execute(
                """
                INSERT INTO user_stats (user_id, username, first_name, last_active)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET
                    last_active = CURRENT_TIMESTAMP,
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name;
                """,
                (user_id, username, first_name),
            )
            if action_type == "search":
                Database.execute(
                    "UPDATE user_stats SET search_count = search_count + 1 WHERE user_id = %s;",
                    (user_id,),
                )
            elif action_type == "request":
                Database.execute(
                    "UPDATE user_stats SET request_count = request_count + 1 WHERE user_id = %s;",
                    (user_id,),
                )
        except Exception as e:
            logger.error(f"track_activity error: {e}")


# ========= Movie search (DB first; external stubs) =========
class MovieSearch:
    @staticmethod
    async def search_movie(title: str, max_results=5):
        results = []
        try:
            db_rows = Database.execute(
                """
                SELECT title, url, file_id, quality, size
                FROM movies
                WHERE title ILIKE %s
                   OR id IN (SELECT movie_id FROM movie_aliases WHERE alias ILIKE %s)
                LIMIT %s;
                """,
                (f"%{title}%", f"%{title}%", max_results),
                fetch=True,
            )
        except Exception:
            # SQLite doesn't support ILIKE; retry with LIKE for sqlite mode
            if Database.mode() == "sqlite":
                db_rows = Database.execute(
                    """
                    SELECT title, url, file_id, quality, size
                    FROM movies
                    WHERE title LIKE %s
                       OR id IN (SELECT movie_id FROM movie_aliases WHERE alias LIKE %s)
                    LIMIT %s;
                    """,
                    (f"%{title}%", f"%{title}%", max_results),
                    fetch=True,
                )
            else:
                raise

        for r in db_rows or []:
            results.append(
                {
                    "title": r[0],
                    "url": r[1],
                    "file_id": r[2],
                    "quality": r[3],
                    "size": r[4],
                    "source": "database",
                }
            )

        if len(results) < max_results:
            ext = await MovieSearch._search_external_sources(title, max_results - len(results))
            results.extend(ext)
        return results

    @staticmethod
    async def _search_external_sources(title: str, max_results: int):
        # Placeholder for any API/site scraping you might add
        return []


# ========= Notifications =========
class NotificationSystem:
    @staticmethod
    async def notify_admin(context: ContextTypes.DEFAULT_TYPE, user, movie_title, group_info=None):
        if not Config.ADMIN_CHANNEL_ID:
            return
        try:
            user_info = f"User: {user.first_name or 'Unknown'}"
            if user.username:
                user_info += f" (@{user.username})"
            user_info += f" (ID: {user.id})"
            group_text = f"From Group: {group_info}" if group_info else "Via Private Message"

            message = (
                "🎬 New Movie Request! 🎬\n\n"
                f"Movie: {movie_title}\n"
                f"{user_info}\n"
                f"{group_text}\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}"
            )
            await context.bot.send_message(chat_id=Config.ADMIN_CHANNEL_ID, text=message)
        except Exception as e:
            logger.error(f"notify_admin error: {e}")

    @staticmethod
    async def notify_users(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url, file_id=None):
        logger.info(f"Notifying users about: {movie_title}")
        try:
            users = Database.execute(
                """
                SELECT user_id, group_id
                FROM user_requests
                WHERE movie_title ILIKE %s AND notified = FALSE;
                """,
                (f"%{movie_title}%",),
                fetch=True,
            )
        except Exception:
            # SQLite fallback
            users = Database.execute(
                """
                SELECT user_id, group_id
                FROM user_requests
                WHERE movie_title LIKE %s AND notified = 0;
                """,
                (f"%{movie_title}%",),
                fetch=True,
            )

        notified_count = 0
        for user_id, group_id in users or []:
            try:
                if file_id:
                    await context.bot.send_document(chat_id=user_id, document=file_id)
                else:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"🎉 Your requested movie '{movie_title}' is now available!",
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("Watch Now", url=movie_url)]]
                        ),
                    )
                # Mark as notified
                try:
                    Database.execute(
                        "UPDATE user_requests SET notified = TRUE WHERE user_id = %s AND movie_title ILIKE %s;",
                        (user_id, f"%{movie_title}%"),
                    )
                except Exception:
                    Database.execute(
                        "UPDATE user_requests SET notified = 1 WHERE user_id = %s AND movie_title LIKE %s;",
                        (user_id, f"%{movie_title}%"),
                    )
                notified_count += 1
            except Forbidden:
                logger.warning(f"User {user_id} blocked the bot")
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")

        logger.info(f"Notified {notified_count} users")
        return notified_count


# ========= Keyboards =========
class Keyboards:
    @staticmethod
    def main_menu():
        keyboard = [
            ["🔍 Search Movies", "🙋 Request Movie"],
            ["📊 My Stats", "⭐ Rate Us", "❓ Help"],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    @staticmethod
    def movie_options(movie_title, url, quality=None, size=None):
        buttons = [[InlineKeyboardButton("🎬 Watch Now", url=url)]]
        if quality:
            buttons.append([InlineKeyboardButton(f"Quality: {quality}", callback_data="info_quality")])
        if size:
            buttons.append([InlineKeyboardButton(f"Size: {size}", callback_data="info_size")])
        buttons.append([InlineKeyboardButton("📥 Download", callback_data=f"download_{movie_title}")])
        return InlineKeyboardMarkup(buttons)

    @staticmethod
    def rating_options():
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⭐", callback_data="rate_1"),
                    InlineKeyboardButton("⭐⭐", callback_data="rate_2"),
                    InlineKeyboardButton("⭐⭐⭐", callback_data="rate_3"),
                    InlineKeyboardButton("⭐⭐⭐⭐", callback_data="rate_4"),
                    InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="rate_5"),
                ]
            ]
        )


# ========= Helpers =========
def store_user_request(user_id, username, first_name, movie_title, group_id=None, message_id=None):
    try:
        Database.execute(
            """
            INSERT INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, movie_title) DO UPDATE SET requested_at = CURRENT_TIMESTAMP;
            """,
            (user_id, username, first_name, movie_title, group_id, message_id),
        )
        return True
    except Exception:
        # SQLite fallback (no upsert on unique pair easily): try insert, ignore if exists
        try:
            Database.execute(
                """
                INSERT OR IGNORE INTO user_requests (user_id, username, first_name, movie_title, group_id, message_id)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (user_id, username, first_name, movie_title, group_id, message_id),
            )
            # If existed, update timestamp
            Database.execute(
                """
                UPDATE user_requests
                SET requested_at = CURRENT_TIMESTAMP
                WHERE user_id = %s AND movie_title = %s;
                """,
                (user_id, movie_title),
            )
            return True
        except Exception as e:
            logger.error(f"store_user_request error: {e}")
            return False


# ========= Bot Handlers =========
class BotHandlers:
    def __init__(self):
        self.ai = AIAssistant()

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user = update.effective_user
            UserManager.track_activity(user.id, user.username, user.first_name, "start")
            welcome = (
                "🎬 Welcome to MovieFinder Bot! 🎬\n\n"
                "I can help you find and request movies and series.\n\n"
                "Features:\n"
                "• Search our database\n"
                "• Request movies we don't have yet\n"
                "• Get notified when your request is available\n"
                "• Multi-language support\n\n"
                "Use the buttons below to get started!"
            )
            await update.message.reply_text(welcome, reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
        except Exception as e:
            logger.error(f"/start error: {e}")
            return States.MAIN_MENU

    async def search_movies(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not update.message or not update.message.text:
                return States.MAIN_MENU

            user_message = update.message.text.strip()
            user = update.effective_user

            # Rate limit (Redis or memory)
            allowed = True
            if redis_conn:
                key = f"rate:{user.id}"
                current = redis_conn.get(key)
                if current and int(current) >= Config.REQUEST_LIMIT:
                    allowed = False
                else:
                    if current:
                        redis_conn.incr(key)
                    else:
                        redis_conn.setex(key, Config.REQUEST_WINDOW, 1)
            else:
                allowed = memory_limiter.allow(str(user.id))

            if not allowed:
                await update.message.reply_text("🚫 You've reached your hourly request limit. Please try again later.")
                return States.MAIN_MENU

            UserManager.track_activity(user.id, user.username, user.first_name, "search")

            if len(user_message) < 3:
                await update.message.reply_text("Please enter at least 3 characters to search.")
                return States.SEARCHING

            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

            results = await MovieSearch.search_movie(user_message)

            if results:
                first = results[0]
                response = f"🎉 Found {len(results)} result(s)!\n\nTop result: {first['title']}"
                if first.get("quality"):
                    response += f"\nQuality: {first['quality']}"
                if first.get("size"):
                    response += f" | Size: {first['size']}"

                if first.get("file_id"):
                    await update.message.reply_document(document=first["file_id"], caption=response)
                else:
                    await update.message.reply_text(
                        response,
                        reply_markup=Keyboards.movie_options(
                            first["title"],
                            first["url"],
                            first.get("quality"),
                            first.get("size"),
                        ),
                    )
                # Send remaining
                for r in results[1:]:
                    if r.get("file_id"):
                        await context.bot.send_document(
                            chat_id=update.effective_chat.id, document=r["file_id"], caption=r["title"]
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=r["title"],
                            reply_markup=Keyboards.movie_options(
                                r["title"], r["url"], r.get("quality"), r.get("size")
                            ),
                        )
            else:
                # Offer request
                store_user_request(
                    user.id,
                    user.username,
                    user.first_name,
                    user_message,
                    update.effective_chat.id if update.effective_chat and update.effective_chat.type != "private" else None,
                    update.message.message_id,
                )
                keyboard = [[InlineKeyboardButton("✅ Yes, Request It", callback_data=f"request_{user_message}")]]
                await update.message.reply_text(
                    f"😔 Sorry, '{user_message}' is not in our collection yet. Would you like to request it?",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )

            await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
            return States.MAIN_MENU
        except Exception as e:
            logger.error(f"search_movies error: {e}")
            if update.message:
                await update.message.reply_text("Sorry, something went wrong. Please try again.")
            return States.MAIN_MENU


async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu selections"""
    text = (update.message.text or "").strip()
    user = update.effective_user

    if text == "🔍 Search Movies":
        await update.message.reply_text("🔍 Enter movie name to search:")
        return States.SEARCHING

    if text == "🙋 Request Movie":
        await update.message.reply_text("📝 Enter the movie name you want to request:")
        return States.REQUESTING

    if text == "📊 My Stats":
        try:
            rows = Database.execute(
                "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = %s;",
                (user.id,),
                fetch=True,
            )
            if rows:
                s, r, last = rows[0]
                last_s = last.strftime("%Y-%m-%d %H:%M") if hasattr(last, "strftime") else str(last)
                msg = f"📊 Your Stats:\n\n🔍 Searches: {s}\n🙋 Requests: {r}\n🕐 Last Active: {last_s}"
            else:
                msg = "📊 Your Stats:\n\nNo activity recorded yet."
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"My Stats error: {e}")
            await update.message.reply_text("❌ Error retrieving your stats.")
        return States.MAIN_MENU

    if text == "⭐ Rate Us":
        await update.message.reply_text("Please rate our service:", reply_markup=Keyboards.rating_options())
        return States.MAIN_MENU

    if text == "❓ Help":
        help_text = (
            "🤖 How to use MovieFinder Bot:\n\n"
            "🔍 Search Movies - Find movies in our database\n"
            "🙋 Request Movie - Request movies we don't have\n"
            "📊 My Stats - View your usage statistics\n"
            "⭐ Rate Us - Rate our service\n\n"
            "Commands:\n"
            "/start - Start the bot\n"
            "/stats - View your statistics\n"
            "/feedback - Share feedback\n"
            "/help - Show this help message"
        )
        await update.message.reply_text(help_text)
        return States.MAIN_MENU

    # Fallback to menu
    await update.message.reply_text("Choose an option:", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU


async def request_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle movie requests"""
    movie_title = (update.message.text or "").strip()
    user = update.effective_user

    if len(movie_title) < 2:
        await update.message.reply_text("Please enter a valid movie name (at least 2 characters).")
        return States.REQUESTING

    success = store_user_request(user.id, user.username, user.first_name, movie_title)

    if success:
        try:
            await NotificationSystem.notify_admin(context, user, movie_title)
        except Exception as e:
            logger.error(f"Notify admin error: {e}")
        await update.message.reply_text(
            f"✅ Your request for '{movie_title}' has been recorded! We'll notify you when it's available."
        )
    else:
        await update.message.reply_text("❌ Sorry, there was an error processing your request. Please try again.")

    await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU


async def process_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user feedback"""
    feedback_text = (update.message.text or "").strip()
    user = update.effective_user
    try:
        Database.execute(
            "INSERT INTO feedback (user_id, message) VALUES (%s, %s);",
            (user.id, feedback_text),
        )
        await update.message.reply_text("✅ Thank you for your feedback!")
    except Exception as e:
        logger.error(f"Feedback store error: {e}")
        await update.message.reply_text("❌ Error saving your feedback.")
    await update.message.reply_text("What would you like to do next?", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("rate_"):
        rating = int(data.split("_")[1])
        user = update.effective_user
        try:
            Database.execute(
                "INSERT INTO feedback (user_id, message, rating) VALUES (%s, %s, %s);",
                (user.id, f"User rating: {rating} stars", rating),
            )
            await query.edit_message_text(f"✅ Thank you for your {rating} star rating!")
        except Exception as e:
            logger.error(f"Rating store error: {e}")
            await query.edit_message_text("❌ Error saving your rating.")
        return

    if data.startswith("request_"):
        movie_title = data.replace("request_", "", 1)
        user = update.effective_user
        success = store_user_request(user.id, user.username, user.first_name, movie_title)
        if success:
            await query.edit_message_text(f"✅ Your request for '{movie_title}' has been recorded!")
        else:
            await query.edit_message_text("❌ Error processing your request.")
        return


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled.", reply_markup=Keyboards.main_menu())
    return States.MAIN_MENU


async def user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        rows = Database.execute(
            "SELECT search_count, request_count, last_active FROM user_stats WHERE user_id = %s;",
            (user.id,),
            fetch=True,
        )
        if rows:
            s, r, last = rows[0]
            last_s = last.strftime("%Y-%m-%d %H:%M") if hasattr(last, "strftime") else str(last)
            msg = f"📊 Your Stats:\n\n🔍 Searches: {s}\n🙋 Requests: {r}\n🕐 Last Active: {last_s}"
        else:
            msg = "📊 Your Stats:\n\nNo activity recorded yet."
        await update.message.reply_text(msg)
    except Exception as e:
        logger.error(f"/stats error: {e}")
        await update.message.reply_text("❌ Error retrieving your stats.")


async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💬 Please share your feedback or suggestions:")
    return States.FEEDBACK


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 How to use MovieFinder Bot:\n\n"
        "🔍 Search Movies - Find movies in our database\n"
        "🙋 Request Movie - Request movies we don't have\n"
        "📊 My Stats - View your usage statistics\n"
        "⭐ Rate Us - Rate our service\n\n"
        "Commands:\n"
        "/start - Start the bot\n"
        "/stats - View your statistics\n"
        "/feedback - Share feedback\n"
        "/help - Show this help message\n\n"
        "Need help? Contact support."
    )
    await update.message.reply_text(help_text)


async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if Config.ADMIN_USER_ID == 0 or user.id != Config.ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is for administrators only.")
        return
    await update.message.reply_text("📝 Admin movie addition feature will be implemented here.")


def build_application() -> Application:
    if not Config.TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    app = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()

    handlers = BotHandlers()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", handlers.start)],
        states={
            States.MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu)],
            States.SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.search_movies)],
            States.REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, request_movie)],
            States.FEEDBACK: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_feedback)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(CommandHandler("addmovie", add_movie))
    app.add_handler(CommandHandler("stats", user_stats))
    app.add_handler(CommandHandler("feedback", feedback))
    app.add_handler(CommandHandler("help", help_command))

    return app


def main():
    # DB init (robust: PG with SSL, fallback to SQLite)
    Database.init()

    # Build and run bot
    application = build_application()
    logger.info("Bot starting polling...")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
