# -*- coding: utf-8 -*-
import traceback
import os
import secrets
import pytz
import re
import json
import threading
import asyncio
import logging  # Logging import zaroori hai
import random
import requests
import signal
import sys
from PIL import Image, ImageFilter
from trending_manager import trending_worker_loop
from telegram import KeyboardButton, WebAppInfo
from telegram import MenuButtonWebApp, WebAppInfo
import aiohttp
# import anthropic  # Agar zaroorat ho toh uncomment karein
from flask import jsonify
from flask_cors import CORS
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, quote
from collections import defaultdict
from telegram.error import RetryAfter, TelegramError
from typing import Optional
from psycopg2 import pool
from io import BytesIO

# Naya Lock banaya Auto-Batch ke liye
auto_batch_lock = asyncio.Lock()

# ==================== 1. LOGGING SETUP (SABSE PEHLE YEH AAYEGA) ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Change from INFO to DEBUG if needed
)
logger = logging.getLogger(__name__)

# ==================== 2. AB IMDB CHECK KAREIN (AB YE SAFE HAI) ====================
try:
    from imdb import Cinemagoer
    ia = Cinemagoer()
except ImportError:
    # Ab logger define ho chuka hai, toh yeh error nahi dega
    logger.warning("imdb (cinemagoer) module not found. Run: pip install cinemagoer")
    ia = None

# ==================== 3. BAAKI IMPORTS ====================
# Third-party imports
from bs4 import BeautifulSoup
import telegram
import psycopg2
from flask import Flask, request, session, g
import google.generativeai as genai
from googleapiclient.discovery import build
from fuzzywuzzy import process, fuzz
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

# Local imports
import admin_views as admin_views_module

# Try to import db_utils
try:
    import db_utils
    FIXED_DATABASE_URL = getattr(db_utils, "FIXED_DATABASE_URL", None)
except Exception:
    FIXED_DATABASE_URL = None

def get_safe_font(text, style=None):
    """
    Normal text ko Premium Fonts mein convert karta hai.
    """
    if not text:
        return ""
    
    # 1. Bold Italic (𝑲𝒂𝒍𝒌𝒊 𝟐𝟖𝟗𝟖 𝑨𝑫)
    def to_bold_italic(s):
        result = ""
        for char in s:
            if 'a' <= char <= 'z': result += chr(0x1D482 + ord(char) - ord('a'))
            elif 'A' <= char <= 'Z': result += chr(0x1D468 + ord(char) - ord('A'))
            elif '0' <= char <= '9': result += chr(0x1D7CE + ord(char) - ord('0'))
            else: result += char
        return result

    # 2. Double Struck (𝕂𝕒𝕝𝕜𝕚 𝟚𝟠𝟡𝟠 𝔸𝔻)
    def to_double_struck(s):
        result = ""
        for char in s:
            if 'a' <= char <= 'z': result += chr(0x1D552 + ord(char) - ord('a'))
            elif 'A' <= char <= 'Z': result += chr(0x1D538 + ord(char) - ord('A'))
            elif '0' <= char <= '9': result += chr(0x1D7D8 + ord(char) - ord('0'))
            else: result += char
        return result

    # Randomly inme se ek uthayega taaki wo chutiya squared font na aaye
    return random.choice([to_bold_italic, to_double_struck])(text)

# ==================== GLOBAL VARIABLES ====================
BATCH_18_SESSION = {'active': False, 'admin_id': None, 'files': []}

background_tasks = set()

DEFAULT_POSTER = os.environ.get(
    "DEFAULT_POSTER",
    "https://i.imgur.com/6XK4F6K.png"  # fallback placeholder
)
# ==================== CONVERSATION STATES (YEH MISSING HAI) ====================
WAITING_FOR_NAME, CONFIRMATION = range(2)
SEARCHING, REQUESTING, MAIN_MENU, REQUESTING_FROM_BUTTON = range(2, 6)
# ================= CONFIGURATION =================
FORUM_GROUP_ID = -1003696437312  # Apne Group ki ID

# Yahan Topic ID aur uske Keywords set karo
# Bot in shabdon ko Genre ya Description me dhundega
TOPIC_MAPPING = {
    # Format:  Topic_ID:  ['keyword1', 'keyword2', 'keyword3']
    
    20: ['south', 'telugu', 'tamil', 'kannada', 'malayalam', 'allu arjun'], # South Topic (ID: 12)
    32: ['hollywood', 'english', 'marvel', 'dc', 'disney'],                 # Hollywood Topic (ID: 34)
    16: ['bollywood', 'hindi', 'khan', 'kapoor'],                           # Bollywood Topic (ID: 56)
    18: ['series', 'season', 'episode', 'netflix', 'amazon'],               # Web Series Topic (ID: 78)
    22: ['anime', 'cartoon', 'animation'],                                  # Anime Topic (ID: 90)
    
    # Default Topic (Agar kuch match na ho to yahan jayega)
    1: ['default'] 
}
# =================================================

def get_auto_topic_id(genre, description):
    """
    Ye function movie ke data ko padhkar sahi Topic ID batata hai.
    """
    text_to_check = (str(genre) + " " + str(description)).lower()
    
    for topic_id, keywords in TOPIC_MAPPING.items():
        for word in keywords:
            if word in text_to_check:
                return topic_id
    
    return 100 # Agar kuch samajh na aaye to Default Topic ID

async def post_to_topic_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Forum topic par post karo + DB mein save karo (Restore ke liye)
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    # --- 1. MOVIE SEARCH ---
    movie_search_name = " ".join(context.args).strip() if context.args else ""

    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("❌ Database connection failed.")
        return

    cursor = conn.cursor()

    query = """
        SELECT id, title, year, rating, genre, 
               poster_url, description, category 
        FROM movies
    """

    if movie_search_name:
        cursor.execute(
            query + " WHERE title ILIKE %s LIMIT 1",
            (f"%{movie_search_name}%",)
        )
    elif BATCH_SESSION.get('active'):
        cursor.execute(
            query + " WHERE id = %s",
            (BATCH_SESSION['movie_id'],)
        )
    else:
        await update.message.reply_text(
            "❌ Naam batao!\nExample: `/post Pushpa`",
            parse_mode='Markdown'
        )
        cursor.close()
        close_db_connection(conn)
        return

    movie_data = cursor.fetchone()
    cursor.close()
    close_db_connection(conn)

    if not movie_data:
        await update.message.reply_text("❌ Movie nahi mili database mein.")
        return

    # --- 2. DATA UNPACK ---
    movie_id, title, year, rating, genre, poster_url, description, category = movie_data

    # --- 3. TOPIC SELECTION ---
    topic_id  = 100
    cat_lower = str(category or "").lower()

    for tid, keywords in TOPIC_MAPPING.items():
        if cat_lower in [k.lower() for k in keywords]:
            topic_id = tid
            break

    if topic_id == 100:
        if "south"      in cat_lower: topic_id = 20
        elif "hollywood" in cat_lower: topic_id = 32
        elif "bollywood" in cat_lower: topic_id = 16
        elif "anime"     in cat_lower: topic_id = 22
        elif "series"    in cat_lower: topic_id = 18

    # --- 4. MISSING DATA HANDLE ---
    final_photo = (
        poster_url
        if poster_url and poster_url != 'N/A'
        else DEFAULT_POSTER
    )
    short_desc = (
        (description[:150] + "...")
        if description
        else "Plot details unavailable."
    )

    # --- 5. CAPTION ---
    caption = (
        f"🎬 **{title} ({year})**\n\n"
        f"⭐️ **Rating:** {rating}/10\n"
        f"🎭 **Genre:** {genre}\n"
        f"🏷 **Category:** {category}\n\n"
        f"📜 **Story:** {short_desc}\n\n"
        f"👇 **Click Below to Download** 👇"
    )

    # --- 6. DEEP LINKS (NEW SECURE LINKS) ---
    secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"

    # Keyboard data (Restore ke liye save hoga)
    keyboard_data = {
        "inline_keyboard": [
            [
                {"text": "📥 Download Now", "url": secure_url},
                {"text": "📥 Download Now", "url": secure_url}
            ],
            [
                {"text": "⚡ Download Now", "url": secure_url}
            ]
        ]
    }

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📥 Download Now", url=secure_url),
            InlineKeyboardButton("📥 Download Now", url=secure_url)
        ],
        [
            InlineKeyboardButton("⚡ Download Now", url=secure_url)
        ]
    ])

    # --- 7. POST SEND (Anti-Block Mode) ---
    try:
        # Pehle image download karne ki koshish karo
        downloaded_poster = await get_poster_bytes(final_photo)
        
        # Agar download fail ho jaye, tabhi URL use karo (Fallback)
        photo_to_send = downloaded_poster if downloaded_poster else final_photo

        sent = await context.bot.send_photo(
            chat_id            = FORUM_GROUP_ID,
            message_thread_id  = topic_id,
            photo              = photo_to_send,
            caption            = caption,
            parse_mode         = 'Markdown',
            reply_markup       = keyboard
        )

        # --- 8. DB SAVE (Restore ke liye) ---
        try:
            bot_info = await context.bot.get_me()
            save_post_to_db(
                movie_id      = movie_id,
                channel_id    = FORUM_GROUP_ID,
                message_id    = sent.message_id,
                bot_username  = bot_info.username,
                caption       = caption,
                media_file_id = final_photo,
                media_type    = "photo",
                keyboard_data = keyboard_data,
                topic_id      = topic_id,
                content_type  = (
                    "adult"  if "adult"    in cat_lower else
                    "series" if "series"   in cat_lower else
                    "anime"  if "anime"    in cat_lower else
                    "movies"
                )
            )
            save_status = "💾 DB mein save hua ✅"
        except Exception as save_err:
            logger.warning(f"Post DB save failed (non-critical): {save_err}")
            save_status = "⚠️ DB save nahi hua"

        await update.message.reply_text(
            f"✅ **{title}** posted in Topic `{topic_id}`\n"
            f"{save_status}",
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Post failed: {e}")
        await update.message.reply_text(f"❌ Post Error: {e}")

# ==================== ENVIRONMENT VARIABLES ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL = os.environ.get('DATABASE_URL')
    # 👇👇👇 START COPY HERE 👇👇👇
db_pool = None
try:
    # Pool create kar rahe hain taki baar baar connection na banana pade
    pool_url = FIXED_DATABASE_URL or DATABASE_URL
    if pool_url:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, # Min 1, Max 20 connections
            dsn=pool_url
        )
        logger.info("✅ Database Connection Pool Created!")
except Exception as e:
    logger.error(f"❌ Error creating pool: {e}")
# 👆👆👆 END COPY HERE 👆👆👆
BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
BLOG_ID = os.environ.get('BLOG_ID')
UPDATE_SECRET_CODE = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
_admin_id = os.environ.get('ADMIN_USER_ID', '0')
ADMIN_USER_ID = int(_admin_id) if _admin_id.isdigit() else 0
GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
ADMIN_CHANNEL_ID = os.environ.get('ADMIN_CHANNEL_ID')

# ==================== DYNAMIC FSUB SYSTEM ====================
ACTIVE_FSUB = {
    'id': os.environ.get('REQUIRED_CHANNEL_ID', '-1002555232489'),
    'url': 'https://t.me/MovieHdmoviehub' 
}
BACKUP_FSUB_LIST = [
    {'id': '-1003586110697', 'url': 'https://t.me/FlimfyBoxmovies'}, 
    {'id': '-1002222222222', 'url': 'https://t.me/BackupChannel2'}  
]
# =============================================================

# 👇👇 YAHAN YE EK LINE PASTE KAR DO 👇👇
FILMFYBOX_CHANNEL_URL = ACTIVE_FSUB['url']

REQUIRED_GROUP_ID = os.environ.get('REQUIRED_GROUP_ID', '-1003460387180')
FILMFYBOX_GROUP_URL = 'https://t.me/FlimfyBox'
REQUEST_CHANNEL_ID = os.environ.get('REQUEST_CHANNEL_ID', '-1003078990647')
DUMP_CHANNEL_ID = os.environ.get('DUMP_CHANNEL_ID', '-1002683355160')
FORCE_JOIN_ENABLED = True

# ✅ NEW ENVIRONMENT VARIABLES FOR MULTI-CHANNEL & AI
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")  # ✅ NEW: Claude API Key
STORAGE_CHANNELS = os.environ.get("STORAGE_CHANNELS", "")  # ✅ NEW: Backup Channels List

# Verified users cache (Taaki baar baar API call na ho)
verified_users = {}
VERIFICATION_CACHE_TIME = 3600  # 1 Hour

# --- Random GIF IDs for Search Failure ---
SEARCH_ERROR_GIFS = [
    'https://media.giphy.com/media/26hkhKd2Cp5WMWU1O/giphy.gif',
    'https://media.giphy.com/media/3o7aTskHEUdgCQAXde/giphy.gif',
    'https://media.giphy.com/media/l2JhkHg5y5tW3wO3u/giphy.gif',
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

# ✅ NEW GLOBAL VARIABLES FOR BATCH SESSION
BATCH_SESSION = {'active': False, 'movie_id': None, 'movie_title': None, 'file_count': 0, 'admin_id': None}
SUPER_BATCH_SESSION = {'active': False, 'admin_id': None, 'files': []}

# Validate required environment variables
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable is not set")
    raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable is not set")
    raise ValueError("DATABASE_URL is not set.")


# 👇👇👇 START COPY HERE (Line 290 ke aas-paas paste karein) 👇👇👇
import functools

async def run_async(func, *args, **kwargs):
    """
    Ye function blocking code (jaise Database/Fuzzy search) ko
    background thread me chalata hai taaki bot hang na ho.
    """
    func_partial = functools.partial(func, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(None, func_partial)
# 👆👆👆 END COPY HERE 👆👆👆

# ==================== UTILITY FUNCTIONS ====================

def extract_season_name(extra_info):
    """File ke extra_info se 'Season 1', 'Season 2' nikalta hai"""
    if not extra_info: 
        return "Extra Files"
    
    import re
    # S01, S1, Season 1, etc. ko dhoondhne ka regex
    match = re.search(r'(?i)(s\d{1,2}|season\s*\d+)', extra_info)
    if match:
        s = match.group().upper()
        # Number nikal lo (e.g., '01' se '1')
        num = re.search(r'\d+', s).group()
        return f"Season {int(num)}"
    return "Extra Files"
    
    import re
    # S01, S1, Season 1, etc. ko dhoondhne ka regex
    match = re.search(r'(?i)(s\d{1,2}|season\s*\d+)', extra_info)
    if match:
        s = match.group().upper()
        # Number nikal lo (e.g., '01' se '1')
        num = re.search(r'\d+', s).group()
        return f"Season {int(num)}"
    return "Extra Files"

async def get_poster_bytes(url):
    """
    Amazon/IMDb se fake browser (User-Agent) ban kar image download karta hai,
    taaki 'Region Block' wala error na aaye.
    """
    if not url or url == 'N/A':
        return None
        
    try:
        # Fake browser details taaki Amazon block na kare
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'Referer': 'https://www.imdb.com/'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    image_data = await response.read()
                    return BytesIO(image_data) # Image ko bytes me convert kar diya
        return None
    except Exception as e:
        logger.error(f"Error downloading poster: {e}")
        return None

def preprocess_query(query):
    """Clean and normalize user query"""
    query = re.sub(r'[^\w\s-]', '', query)
    return query

def clean_telegram_text(text):
    """Removes emojis and converts fancy fonts to normal text"""
    if not text: return ""
    fancy = {'ᴀ':'a','ʙ':'b','ᴄ':'c','ᴅ':'d','ᴇ':'e','ғ':'f','ɢ':'g','ʜ':'h','ɪ':'i','ᴊ':'j','ᴋ':'k','ʟ':'l','ᴍ':'m','ɴ':'n','ᴏ':'o','ᴘ':'p','ǫ':'q','ʀ':'r','s':'s','ᴛ':'t','ᴜ':'u','ᴠ':'v','ᴡ':'w','x':'x','ʏ':'y','ᴢ':'z'}
    for k, v in fancy.items(): 
        text = text.replace(k, v)
    
    import unicodedata
    text = unicodedata.normalize('NFKC', text)
    
    import re
    text = re.sub(r'[^\w\s\.\-\'\[\]\(\)@:]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    text = re.sub(r'^[\.\-\s]+', '', text)
    
    # 👇 NAYA FIX: "Name:", "Title:", "File Name:" jaise words ko shuruat se hata dega
    text = re.sub(r'(?i)^(name|title|file\s*name|movie)\s*:\s*', '', text).strip()
    
    return text
async def make_landscape_poster(url_or_bytes):
    """
    Portrait poster ko Mobile+PC friendly (Square 1:1) format me convert karta hai.
    Heavily Blurred background aur Center me Sharp image.
    """
    try:
        from PIL import Image, ImageFilter 
        
        image_data = None
        if isinstance(url_or_bytes, str) and url_or_bytes.startswith('http'):
            async with aiohttp.ClientSession() as session:
                headers = {'User-Agent': 'Mozilla/5.0'}
                async with session.get(url_or_bytes, headers=headers) as resp:
                    if resp.status == 200:
                        image_data = await resp.read()
        elif isinstance(url_or_bytes, bytes):
            image_data = url_or_bytes
        elif hasattr(url_or_bytes, 'getvalue'): 
            image_data = url_or_bytes.getvalue()
            
        if not image_data:
            return url_or_bytes

        # ---------------- IMAGE MAGIC (MOBILE FRIENDLY) ----------------
        img = Image.open(BytesIO(image_data)).convert("RGB")
        
        # TARGET SIZE: Square format (Phone aur Desktop dono pe best lagta hai)
        # 800x800 resolution rakhte hain.
        target_w, target_h = 800, 800
        
        # 1. Background Blur (Zoomed in and heavily blurred)
        bg_img = img.resize((target_w, int(img.height * (target_w / img.width))), Image.Resampling.LANCZOS)
        if bg_img.height > target_h:
            top = (bg_img.height - target_h) // 2
            bg_img = bg_img.crop((0, top, target_w, top + target_h))
        else:
            bg_img = bg_img.resize((target_w, target_h), Image.Resampling.LANCZOS)
            
        bg_img = bg_img.filter(ImageFilter.GaussianBlur(radius=40)) # Extra strong blur background ke liye
        
        # 2. Foreground Paste (Portrait poster ko centre me chhota karke lagana)
        # Foreground height ko thoda chota karenge (like 90% of target) taaki breathing room mile
        fg_h = int(target_h * 0.95)
        fg_w = int(img.width * (fg_h / img.height))
        fg_img = img.resize((fg_w, fg_h), Image.Resampling.LANCZOS)
        
        paste_x = (target_w - fg_w) // 2
        paste_y = (target_h - fg_h) // 2
        bg_img.paste(fg_img, (paste_x, paste_y))
        
        # 3. Output as Bytes
        output = BytesIO()
        output.name = "cinematic_poster.jpg"
        bg_img.save(output, format='JPEG', quality=95) 
        output.seek(0)
        return output
        
    except Exception as e:
        logger.error(f"❌ Cinematic Conversion Error: {e}")
        return url_or_bytes


async def check_rate_limit(user_id):
    """Check if user is rate limited"""
    now = datetime.now()
    last_request = user_last_request[user_id]

    if now - last_request < timedelta(seconds=2):
        return False

    user_last_request[user_id] = now
    return True

async def upload_image_to_telegraph(bot, file_id):
    """Downloads photo from Telegram and uploads to Telegra.ph"""
    try:
        # File download karo
        file = await bot.get_file(file_id)
        byte_array = await file.download_as_bytearray()
        
        # Telegraph par upload karo
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field('file', byte_array, filename='poster.jpg', content_type='image/jpeg')
            
            async with session.post('https://telegra.ph/upload', data=data) as resp:
                res = await resp.json()
                if isinstance(res, list) and 'src' in res[0]:
                    return f"https://telegra.ph{res[0]['src']}"
        return None
    except Exception as e:
        logger.error(f"❌ Telegraph upload failed: {e}")
        return None

# 👇 NAYA HELPER FUNCTION: Yeh aapki saari keys .env se nikal lega
def get_gemini_keys():
    keys = []
    # Purani standard key check karein
    std_key = os.environ.get("GEMINI_API_KEY")
    if std_key: keys.append(std_key)
    
    # Nayi numbered keys check karein (1 se 5 tak)
    for i in range(1, 6):
        k = os.environ.get(f"GEMINI_API_KEY_{i}")
        if k and k not in keys:
            keys.append(k)
    return keys


# 👇 UPDATED FUNCTION 1: Name Extraction (With Multi-Key Rotation)
async def get_movie_name_from_caption(caption_text, image_bytes=None):
    """
    🎯 FULLY AI-POWERED EXTRACTION (MULTIMODAL WITH AUTO-KEY ROTATION)
    """
    if not caption_text or len(caption_text.strip()) < 2:
        return {"title": "UNKNOWN", "year": "", "language": "", "extra_info": "", "category": ""}
    
    first_line = clean_telegram_text(caption_text.split('\n')[0].strip())
    logger.info(f"📝 Processing caption: {first_line[:100]}...")

    gemini_keys = get_gemini_keys()

    if gemini_keys:
        prompt = f"""Extract movie/series info from this filename. Return ONLY JSON.
Filename: "{first_line}"
Rules:
- title: Clean name without S01, E01, group names, quality, etc.
- year: 4-digit year if present
- language: Audio languages mentioned
- extra_info: Season/episode info (e.g., "S01 E01-12 COMBINED")
- category: 'Web Series' if season/episode found, else 'Movies'

Example:
Input: "A Gatherer's Adventure In Isekai S01 [E01-12] COMBiNED 720p AMZN WEB-DL HEVC Multi DDP2.0 MSub KEИ !! Shubham.mkv"
Output: {{"title": "A Gatherer's Adventure In Isekai", "year": "", "language": "Multi Audio", "extra_info": "S01 E01-12 COMBINED", "category": "Web Series"}}
JSON:"""

        contents = [prompt]
        if image_bytes:
            contents.append({"mime_type": "image/jpeg", "data": image_bytes})

        # 🚀 KEY ROTATION LOOP
        for key in gemini_keys:
            try:
                genai.configure(api_key=key)
                model = genai.GenerativeModel('gemini-2.0-flash')
                response = await run_async(model.generate_content, contents)
                
                if response and response.text:
                    text = response.text.strip()
                    text = re.sub(r'```json|```', '', text)
                    json_match = re.search(r'\{.*\}', text, re.DOTALL)
                    if json_match:
                        data = json.loads(json_match.group())
                        if data.get("title") and len(data["title"]) > 2:
                            logger.info(f"✅ Gemini Success (Key used: {key[:5]}...): {data['title']}")
                            return data
                break # Agar response mila par JSON galat hai, toh aage wali key waste mat karo
                
            except Exception as e:
                error_msg = str(e).lower()
                # Agar Quota/Limit ka error aaya toh agli key try karo
                if "429" in error_msg or "quota" in error_msg or "exhausted" in error_msg:
                    logger.warning(f"⚠️ Key {key[:5]}... limit reached. Shifting to next key...")
                    continue
                else:
                    logger.error(f"❌ Gemini Error on key {key[:5]}...: {e}")
                    break

    # FALLBACK: Improved version
    logger.info("⚠️ Keys exhausted or failed. Using fallback extraction...")
    return await fallback_extraction(first_line)


# 👇 UPDATED FUNCTION 2: Alias Generation (With Multi-Key Rotation)
def generate_aliases_gemini(movie_title, year="", category=""):
    """
    🎯 AI se 50 search aliases generate karta hai (WITH AUTO-KEY ROTATION)
    """
    logger.info(f"🚀 Generating aliases for: '{movie_title}' ({year}) [{category}]")
    
    if not movie_title or movie_title == "UNKNOWN":
        return []
    
    gemini_keys = get_gemini_keys()
    if not gemini_keys:
        logger.error("❌ No GEMINI_API_KEY found!")
        return generate_basic_aliases(movie_title, year)

    prompt = f"""Generate 50 search aliases for the movie/show: "{movie_title}"
Year: {year if year else "N/A"}
Category: {category if category else "N/A"}

Include these types of variations:
1. Common misspellings (typos people make)
2. With and without year
3. Hindi transliterations if applicable
4. Short forms and abbreviations
5. With "movie", "film", "download" keywords
6. Without spaces, with hyphens
7. Regional language spellings

IMPORTANT: Return ONLY comma-separated aliases, nothing else.
Example format: alias1, alias2, alias3, alias4"""

    safety_settings = {
        genai.types.HarmCategory.HARM_CATEGORY_HARASSMENT: genai.types.HarmBlockThreshold.BLOCK_NONE,
        genai.types.HarmCategory.HARM_CATEGORY_HATE_SPEECH: genai.types.HarmBlockThreshold.BLOCK_NONE,
        genai.types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: genai.types.HarmBlockThreshold.BLOCK_NONE,
        genai.types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: genai.types.HarmBlockThreshold.BLOCK_NONE,
    }

    # 🚀 KEY ROTATION LOOP
    for key in gemini_keys:
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel('gemini-2.0-flash')
            response = model.generate_content(prompt, safety_settings=safety_settings)
            
            if not response or not response.parts:
                logger.warning("Gemini response was empty or blocked. Trying basic.")
                return generate_basic_aliases(movie_title, year)
            
            ai_text = response.text.strip()
            aliases = []
            
            # 👇 FIX 1: Ab bot comma (,) aur New Line (\n) dono ko split kar lega
            raw_items = re.split(r',|\n', ai_text)
            
            for item in raw_items:
                alias = item.strip().lower()
                # Numbers, bullets (*, -) sab hata dega
                alias = re.sub(r'^[\d\.\-\*\)]+\s*', '', alias).strip('"\'').strip()
                if alias and len(alias) >= 2 and len(alias) <= 100:
                    aliases.append(alias)
            
            aliases = list(dict.fromkeys(aliases))[:50]
            
            # 👇 FIX 2: Agar AI ne ajeeb format diya aur 0 alias bache, toh Basic aliases (Fallback) use kar lo
            if not aliases:
                logger.warning("AI returned bad format. Using fallback aliases.")
                return generate_basic_aliases(movie_title, year)
                
            logger.info(f"✅ Generated {len(aliases)} aliases (Key used: {key[:5]}...)")
            return aliases

        except Exception as e:
            error_msg = str(e).lower()
            if "429" in error_msg or "quota" in error_msg or "exhausted" in error_msg:
                logger.warning(f"⚠️ Key {key[:5]}... limit reached. Shifting to next key...")
                continue
            else:
                logger.warning(f"❌ Alias Gemini error on key {key[:5]}...: {e}")
                break

    return generate_basic_aliases(movie_title, year)

def generate_basic_aliases(title, year=""):
    """
    Fallback function to generate simple search aliases without AI.
    """
    aliases = set()
    title_lower = title.lower().strip()
    aliases.add(title_lower)
    aliases.add(title_lower.replace(" ", ""))
    if year:
        aliases.add(f"{title_lower} {year}")
        aliases.add(f"{title_lower}{year}")
    # Remove leading 'the' variations
    if title_lower.startswith("the "):
        base = title_lower[4:]
        aliases.add(base)
        if year:
            aliases.add(f"{base} {year}")
            aliases.add(f"{base}{year}")
    return list(aliases)

def normalize_episodes(text):
    # 1. E12 22 -> E12-22
    text = re.sub(r'(?i)\b(e|ep|episode)\s*(\d{1,3})\s+(\d{1,3})\b(?!\s*p)', r'\1\2-\3', text)
    
    # 2. E12 e22 / E12 ep22 -> E12-22
    text = re.sub(r'(?i)\b(e|ep|episode)\s*(\d{1,3})\s+(?:e|ep|episode)\s*(\d{1,3})\b', r'\1\2-\3', text)
    
    # 3. E12 to 22 / E12 to22 -> E12-22
    text = re.sub(r'(?i)\b(e|ep|episode)\s*(\d{1,3})\s*to\s*(?:e|ep|episode)?\s*(\d{1,3})\b', r'\1\2-\3', text)
    
    return text
    
async def fallback_extraction(caption_text):
    """
    SMART FALLBACK: Improved regex-based extraction for both movies and web series.
    """
    try:
        text = clean_telegram_text(caption_text.strip())
        original = text

        # 1. Remove obvious group prefixes and promotional words
        text = re.sub(r'(?i)^(join\s+)?@\w+\s*', '', text)  # JOIN @channel ko udayega
        text = re.sub(r'(?i)^join\s+', '', text)            # Sirf JOIN likha ho toh udayega
        text = re.sub(r'^\{[^}]+\}\s*', '', text)           # {@Royal_Backup2} ko udayega
        text = re.sub(r'^@\w+\s+', '', text)                # @MRKUPDATES4U6 ko udayega
        text = re.sub(r'^\[[^\]]+\]\s*', '', text)          # [Group] ko udayega
        
        # 2. Detect if it's a web series (contains season/episode indicators)
        season_pattern = re.compile(r'\b(S\d{1,2}|Season\s*\d+|S\d{1,2}E\d{1,3}|\[?E\d{1,3}\s*(?:[-~_]|to)\s*(?:e|ep)?\d{1,3}\]?|EP\s*\d{1,3}(?:\s*(?:[-~_]|to)\s*(?:e|ep)?\d{1,3})?|Episode\s*\d+|Part\s*\d+|P\d+)\b', re.IGNORECASE)
        season_match = season_pattern.search(text)
        if season_match:
            # Use existing web series logic (kept from original)
            return await _extract_web_series(text, original)

        # 3. MOVIE EXTRACTION
        # Try to find year
        year_match = re.search(r'[\(\[]?(19|20)\d{2}[\)\]]?', text)
        year = year_match.group() if year_match else ""
        # Clean year to just digits
        year_clean = re.sub(r'[^0-9]', '', year) if year else ""

        # Determine split point
        split_pos = None
        if year_match:
            split_pos = year_match.start()
        else:
            # Look for first quality/resolution tag
            quality_patterns = [
                r'\b\d{3,4}p\b',                     # 480p, 720p, 1080p
                r'\b(HDRip|WEB-DL|BluRay|DVDRip|BRRip|HDTV|WEBRip|DS4K)\b',
                r'\.(mkv|mp4|avi|m4v)$'              # file extension at end
            ]
            for pat in quality_patterns:
                q_match = re.search(pat, text, re.IGNORECASE)
                if q_match:
                    split_pos = q_match.start()
                    break

        # Extract title
        if split_pos is not None:
            title_part = text[:split_pos].strip()
        else:
            title_part = text

        # Clean title
        title = title_part
        # Replace separators with space
        title = re.sub(r'[._\-]+', ' ', title)
        # Remove any remaining brackets and their content (often group names)
        title = re.sub(r'[\[\(].*?[\]\)]', '', title)
        # Remove URLs, mentions, hashtags
        title = re.sub(r'https?://\S+', '', title)
        title = re.sub(r'@\w+', '', title)
        title = re.sub(r'#\w+', '', title)
        # Collapse multiple spaces and strip
        title = re.sub(r'\s+', ' ', title).strip()
        # Remove trailing noise words (common tech tags)
        junk_words = [
            'hindi', 'english', 'tamil', 'telugu', 'malayalam', 'kannada', 'bengali',
            'dubbed', 'multi', 'audio', 'ddp', 'web', 'dl', 'bluray', 'amzn', 'hevc',
            'x264', 'x265', 'mkv', 'mp4', 'avi', '480p', '720p', '1080p', '2160p',
            'hdrip', 'webdl', 'webrip', 'hdtv', 'ds4k', 'uncut', 'extended', 'directors',
            'cut', 'edition', 'repack', 'proper', 'internal', 'nf', 'hulu', 'hotstar',
            'sony', 'zee5', 'mubi', 'esub', 'sub', 'aac', 'ac3', 'dd5', 'dd2', 'ddp5',
            'ddp2', 'xvid', 'divx', 'remux', 'bdrip', 'brrip', 'dvdrip', 'dvdr', 'pal',
            'ntsc', 'region', 'free', 'watch', 'online', 'download', 'movies', 'series',
            'show', 'south', 'movie', 'org', 'dual', 'truehd', 'atmos', 'dts', 'mp3',
            'flac', 'opus', 'aac2', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'xvid', 'hd', 'full', 'half', 'brrip', 'bdrip', 'web', 'dl', 'hdr'
        ]
        words = title.split()
        if words:
            # Remove trailing junk words
            while words and words[-1].lower() in junk_words:
                words.pop()
            # Also remove leading junk? (rare, but possible)
            while words and words[0].lower() in junk_words:
                words.pop(0)
            title = ' '.join(words)

        # If title is too short after cleaning, fallback to original first line without extension
        if len(title) < 3:
            title = original.split('.')[0].strip()
            title = re.sub(r'[\[\(].*?[\]\)]', '', title)

        # 4. Language extraction (same as original)
        languages = []
        lang_map = {
            'japanese|日本語': 'Japanese', 'english': 'English',
            'hindi|हिन्दी': 'Hindi', 'tamil|தமிழ்': 'Tamil',
            'telugu|తెలుగు': 'Telugu', 'malayalam': 'Malayalam',
            'korean': 'Korean', 'dual.*audio': 'Dual Audio',
            'multi.*audio': 'Multi Audio'
        }
        for pattern, name in lang_map.items():
            if re.search(pattern, text, re.IGNORECASE):
                languages.append(name)
        language = ', '.join(dict.fromkeys(languages)) if languages else ""

        # 5. Extra info (for movies, we might capture edition like UNCUT, EXTENDED)
        extra_info = ""
        edition_match = re.search(r'\b(UNCUT|EXTENDED|DIRECTOR\'?S?\s*CUT|THEATRICAL|UNRATED|REMASTERED)\b', text, re.IGNORECASE)
        if edition_match:
            extra_info = edition_match.group(0).upper()

        # 6. Category
        category = "Movies"

        logger.info(f"✅ Movie Fallback: '{title}' | Year: {year_clean} | Lang: {language} | Extra: {extra_info} | Cat: {category}")

        return {
            "title": title,
            "year": year_clean,
            "language": language,
            "extra_info": extra_info,
            "category": category
        }

    except Exception as e:
        logger.error(f"❌ Fallback error: {e}")
        return {"title": "UNKNOWN", "year": "", "language": "", "extra_info": "", "category": ""}


async def _extract_web_series(text, original):
    try:
        # 1. Episode formats ko normalize karo (to22, ep22, space etc.)
        text = normalize_episodes(text)
        
        # 2. Remove language indicators line if present
        text = re.sub(r'🔊.*?(?:\n|$)', '', text, flags=re.DOTALL)

        # 2. Find season/episode/part position to split title
        split_pos = None
        season_patterns = [
            r'\bPart\s*\d+\b', r'\bP\d+\b',
            r'\bS\d{1,2}\b', r'\bSeason\s*\d+\b',
            r'\bS\d{1,2}E\d{1,3}\b', r'\[?E\d{1,3}[-~_]\d{1,3}\]?',
            r'\bEP\s*\d{1,3}(?:[-~_]\d{1,3})?\b', r'\bEpisode\s*\d+\b'
        ]
        for pattern in season_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                split_pos = match.start()
                break

        # 3. Extract title
        if split_pos is not None:
            title = text[:split_pos].strip()
        else:
            title = text

        # 4. Clean title (similar to movie cleaning)
        title = re.sub(r'[A-ZА-Я]{2,}\s*!+\s*\w+$', '', title, flags=re.IGNORECASE)
        title = re.sub(r'\[.*?\]', '', title)
        title = re.sub(r'\(.*?\)', '', title)
        title = re.sub(r'by\s+\w+$', '', title, flags=re.IGNORECASE)
        title = re.sub(r'https?://\S+', '', title)
        title = re.sub(r'@\w+', '', title)
        title = re.sub(r'#\w+', '', title)
        title = re.sub(r'[_\.\-]+', ' ', title)
        title = re.sub(r'\s+', ' ', title).strip()

        # Remove trailing junk words (common in web series too)
        junk = ['hindi', 'english', 'tamil', 'telugu', 'dubbed', 'multi', 'audio',
                'ddp', 'web', 'dl', 'bluray', 'amzn', 'hevc', 'x264', 'x265', 'mkv']
        words = title.split()
        while words and words[-1].lower() in junk:
            words.pop()
        title = ' '.join(words)

        # 5. Extract metadata
        year_match = re.search(r'[\(\[]?(19|20)\d{2}[\)\]]?', text)
        year = re.search(r'(19|20)\d{2}', year_match.group()) if year_match else ""
        year = year.group() if year else ""

        # Languages
        languages = []
        lang_map = {
            'japanese|日本語': 'Japanese', 'english': 'English',
            'hindi|हिन्दी': 'Hindi', 'tamil|தமிழ்': 'Tamil',
            'telugu|తెలుగు': 'Telugu', 'malayalam': 'Malayalam',
            'korean': 'Korean', 'dual.*audio': 'Dual Audio',
            'multi.*audio': 'Multi Audio'
        }
        for pattern, name in lang_map.items():
            if re.search(pattern, text, re.IGNORECASE):
                languages.append(name)
        language = ', '.join(dict.fromkeys(languages)) if languages else ""

        # Extra info (season/episodes/parts)
        extra_parts = []
        
        # Pehle Part dhoondo (e.g., P1, Part 1)
        p_match = re.search(r'(?i)\b(Part\s*\d+|P\d+)\b', text)
        if p_match:
            extra_parts.append(p_match.group().upper())
            
        s_match = re.search(r'(?i)(s\d{1,2}|season\s*\d+)', text)
        if s_match:
            extra_parts.append(s_match.group().upper())
            
        e_match = re.search(r'(?i)(\[?(?:ep|e|episode)\s*\d{1,3}\s*(?:[-~_]|to)\s*(?:e|ep)?\s*\d{1,3}\]?|\b(?:ep|e|episode)\s*\d{1,3}\b)', text)
        if e_match:
            ep = re.sub(r'[\[\]]', '', e_match.group()).upper()
            extra_parts.append(ep)
            
        if re.search(r'(?i)(combined|complete|batch)', text):
            extra_parts.append('COMBINED')
            
        extra_info = ' '.join(extra_parts)

        # Category
        category = "Web Series"

        # Final check
        if not title or len(title) < 2:
            title = original.split('.')[0].strip()
            title = re.sub(r'[\[\(].*?[\]\)]', '', title)

        logger.info(f"✅ Web Series Fallback: '{title}' | Year: {year} | Lang: {language} | Extra: {extra_info} | Cat: {category}")
        return {
            "title": title,
            "year": year,
            "language": language,
            "extra_info": extra_info,
            "category": category
        }
    except Exception as e:
        logger.error(f"❌ Web series fallback error: {e}")
        return {"title": "UNKNOWN", "year": "", "language": "", "extra_info": "", "category": ""}
# ==================== MEMBERSHIP CHECK LOGIC ====================
async def is_user_member(context, user_id: int, force_fresh: bool = False):
    """Check if user is member of channel and group (Smart Auto-Switch Logic)"""
    global ACTIVE_FSUB, BACKUP_FSUB_LIST
    
    if not FORCE_JOIN_ENABLED:
        return {'is_member': True, 'channel': True, 'group': True, 'error': None}
    
    current_time = datetime.now()
    if not force_fresh and user_id in verified_users:
        last_checked, cached = verified_users[user_id]
        if (current_time - last_checked).total_seconds() < VERIFICATION_CACHE_TIME:
            return cached
    
    result = {'is_member': False, 'channel': False, 'group': False, 'error': None}
    VALID_STATUSES = ['member', 'administrator', 'creator']
    
    # --- 1. SMART CHANNEL CHECK (WITH AUTO-SWITCH) ---
    try:
        channel_member = await context.bot.get_chat_member(chat_id=ACTIVE_FSUB['id'], user_id=user_id)
        if channel_member.status in VALID_STATUSES:
            result['channel'] = True
            
    except telegram.error.Forbidden as e:
        # 🚨 ERROR 1: Bot ko channel se nikal diya gaya hai!
        logger.error(f"🚨 Bot banned from channel! Switching FSub...")
        if BACKUP_FSUB_LIST:
            next_backup = BACKUP_FSUB_LIST.pop(0)
            ACTIVE_FSUB['id'] = next_backup['id']
            ACTIVE_FSUB['url'] = next_backup['url']
            
            # Admin ko SOS Alert bhejo!
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_USER_ID, 
                    text=f"🚨 **URGENT ALARM!** 🚨\n\nTumhara Main Channel ban ho gaya hai ya bot ko admin se hata diya gaya hai!\n\n✅ Maine automatically FSub ko naye channel par shift kar diya hai: {ACTIVE_FSUB['url']}", 
                    parse_mode='Markdown'
                )
            except: pass
            
            # Naye channel ke sath wapas check karo
            return await is_user_member(context, user_id, force_fresh)
        else:
            result['channel'] = True # Agar saare backup khatam, toh FSub bypass kar do taaki bot chalta rahe
            
    except telegram.error.BadRequest as e:
        if "chat not found" in str(e).lower():
            # 🚨 ERROR 2: Channel Telegram ne uda diya (Delete ho gaya)
            logger.error(f"🚨 Channel Deleted! Switching FSub...")
            if BACKUP_FSUB_LIST:
                next_backup = BACKUP_FSUB_LIST.pop(0)
                ACTIVE_FSUB['id'] = next_backup['id']
                ACTIVE_FSUB['url'] = next_backup['url']
                
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_USER_ID, 
                        text=f"🚨 **URGENT ALARM!** 🚨\n\nMain Channel Telegram dwara Delete/Ban kar diya gaya hai!\n\n✅ Maine traffic backup par shift kar diya hai: {ACTIVE_FSUB['url']}", 
                        parse_mode='Markdown'
                    )
                except: pass
                
                return await is_user_member(context, user_id, force_fresh)
            else:
                result['channel'] = True
        else:
            # Koi chhota mota network error, channel active hai
            result['channel'] = False 
            
    except Exception as e:
        # Temporary glitch (ignore and allow/deny gracefully without switching)
        logger.error(f"Temporary Channel Check Error: {e}")
        result['channel'] = False 

    # --- 2. GROUP CHECK ---
    try:
        group_member = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        if group_member.status in VALID_STATUSES:
            result['group'] = True
    except Exception as e:
        logger.error(f"Group Check Error: {e}")
        result['group'] = False

    result['is_member'] = result['channel'] and result['group']
    verified_users[user_id] = (current_time, result)
    
    return result

def get_join_keyboard():
    """Join buttons keyboard"""
    global ACTIVE_FSUB
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 Join Channel", url=ACTIVE_FSUB['url']),
            InlineKeyboardButton("💬 Join Group", url=FILMFYBOX_GROUP_URL)
        ],
        [InlineKeyboardButton("✅ Joined Both - Verify", callback_data="verify")]
    ])

def get_join_message(channel_status, group_status):
    """Generate message based on what is missing"""
    if not channel_status and not group_status:
        missing = "Channel and Group both"
    elif not channel_status:
        missing = "Channel"
    else:
        missing = "Group"
    
    return (
        f"📂 **Your File is Ready!**\n\n"
        f"🚫 **But Access Denied**\n\n"
        f"You haven't joined {missing}!\n\n"
        f"📢 Channel: {'✅' if channel_status else '❌'}\n"
        f"💬 Group: {'✅' if group_status else '❌'}\n\n"
        f"Join both, then click **Verify** button 👇"
    )

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
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

async def send_multi_bot_message(target_user_id, text_message, parse_mode='HTML'):
    """Teeno bots se message bhejkar try karega, jo chal jaye wahi sahi."""
    # Apne .env wale tokens yahan laayein
    tokens = [
        os.environ.get("TELEGRAM_BOT_TOKEN"),
        os.environ.get("BOT_TOKEN_2"),
        os.environ.get("BOT_TOKEN_3")
    ]
    tokens = [t for t in tokens if t] # Khali tokens hata do
    
    for token in tokens:
        try:
            # Temporary bot instance banayega aur message bhejega
            temp_bot = telegram.Bot(token=token)
            await temp_bot.send_message(chat_id=target_user_id, text=text_message, parse_mode=parse_mode)
            return True # Success ho gaya, function khatam
        except telegram.error.Forbidden:
            continue # User ne ye bot block kiya hai, agle bot par jao
        except Exception as e:
            logger.error(f"Multi-bot send error: {e}")
            continue
            
    return False # Teeno bots se fail ho gaya

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
        close_db_connection(conn)

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
            close_db_connection(conn)
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
        close_db_connection(conn)
        return cnt
    except Exception as e:
        logger.error(f"Error counting burst requests for user {user_id}: {e}")
        try:
            close_db_connection(conn)
        except:
            pass
        return 0

# ==================== DATABASE-BACKED AUTO-DELETE FUNCTIONS ====================

async def add_messages_to_db_queue(context, chat_id, message_ids, delay):
    """Messages ko DB me save karta hai taaki restart hone par bhi yaad rahe"""
    try:
        bot_info = await context.bot.get_me()
        bot_username = bot_info.username
        
        # Exact time calculate karo kab delete karna hai
        delete_time = datetime.now() + timedelta(seconds=delay)
        
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                for msg_id in message_ids:
                    cur.execute(
                        "INSERT INTO auto_delete_queue (bot_username, chat_id, message_id, delete_at) VALUES (%s, %s, %s, %s)",
                        (bot_username, chat_id, msg_id, delete_time)
                    )
                conn.commit()
                cur.close()
            except Exception as e:
                logger.error(f"Error saving to delete queue: {e}")
            finally:
                close_db_connection(conn)
    except Exception as e:
        logger.error(f"Failed to get bot info for delete queue: {e}")

async def delete_messages_after_delay(context, chat_id, message_ids, delay=60):
    """Old function ab sidha DB me save karega (No sleep)"""
    await add_messages_to_db_queue(context, chat_id, message_ids, delay)

def track_message_for_deletion(context, chat_id, message_id, delay=60):
    """Synchronous code se DB me entry dalne ke liye helper"""
    if not message_id: return
    
    # Task create karein taaki bot hang na ho
    task = asyncio.create_task(add_messages_to_db_queue(context, chat_id, [message_id], delay))
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

# ==================== DATABASE FUNCTIONS ====================

def setup_database():
    """Setup database tables and indexes (UPDATED to match usage in code)"""
    try:
        conn_str = FIXED_DATABASE_URL or DATABASE_URL
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()

        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        # Movies table (now matches the rest of your code)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL DEFAULT '',
                file_id TEXT,
                is_unreleased BOOLEAN DEFAULT FALSE,

                imdb_id TEXT,
                poster_url TEXT,
                year INTEGER DEFAULT 0,
                genre TEXT,
                rating TEXT,

                description TEXT,
                category TEXT
            )
        """)

        # 👇👇👇 NAYA TABLE: Anti-Bot Temporary Links ke liye 👇👇👇
        cur.execute("""
            CREATE TABLE IF NOT EXISTS temp_links (
                token VARCHAR(50) PRIMARY KEY,
                movie_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 👇👇👇 NAYA TABLE: Auto-Delete Queue ke liye 👇👇👇
        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_delete_queue (
                id SERIAL PRIMARY KEY,
                bot_username TEXT NOT NULL,
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                delete_at TIMESTAMP NOT NULL
            )
        """)
        # Faster search ke liye index (Taki DB slow na ho)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_delete_at ON auto_delete_queue (bot_username, delete_at);")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_files (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                quality TEXT NOT NULL,
                url TEXT,
                file_id TEXT,
                file_size TEXT,
                backup_map JSONB DEFAULT '{}'::jsonb,
                UNIQUE(movie_id, quality)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_info (
                id SERIAL PRIMARY KEY,
                last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
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
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS movie_aliases (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER REFERENCES movies(id) ON DELETE CASCADE,
                alias TEXT NOT NULL,
                UNIQUE(movie_id, alias)
            )
        """)

        # Used in update_buttons_command + some admin flows
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channel_posts (
                id SERIAL PRIMARY KEY,
                movie_id INTEGER,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                bot_username TEXT,
                posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, message_id)
            )
        """)

        # Used in list_all_users (your code queries user_activity)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_activity (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                chat_id BIGINT,
                chat_type TEXT,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id)
            )
        """)

        # Unique constraint for requests
        cur.execute("""
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'user_requests_unique_constraint') THEN
                ALTER TABLE user_requests
                ADD CONSTRAINT user_requests_unique_constraint UNIQUE (user_id, movie_title);
            END IF;
            END $$;
        """)

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_title ON movies (title);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_title_trgm ON movies USING gin (title gin_trgm_ops);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies (imdb_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_requests_movie_title ON user_requests (movie_title);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_requests_user_id ON user_requests (user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movie_aliases_alias ON movie_aliases (alias);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movie_files_movie_id ON movie_files (movie_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_channel_posts_movie_id ON channel_posts (movie_id);")

        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("✅ Database setup completed successfully")

    except Exception as e:
        logger.error(f"❌ Error setting up database: {e}", exc_info=True)
        logger.info("Continuing without database setup...")


def migrate_add_imdb_columns():
    """One-time migration to add missing columns safely (including cast)"""
    conn = get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS imdb_id TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS poster_url TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS year INTEGER DEFAULT 0;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS genre TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS rating TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS description TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS category TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS language TEXT;")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS extra_info TEXT;")
        # Important: quote column name with double quotes in SQL
        cur.execute('ALTER TABLE movies ADD COLUMN IF NOT EXISTS "cast" TEXT;')
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_imdb_id ON movies (imdb_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movies_year ON movies (year);")
        conn.commit()
        cur.close()
        close_db_connection(conn)
        return True
    except Exception as e:
        logger.error(f"Migration error: {e}")
        close_db_connection(conn)
        return False

def migrate_content_type_for_restore():
    """Channel posts mein content_type column add karo"""
    conn = get_db_connection()
    if not conn:
        return
    try:
        cur = conn.cursor()
        # Ye column batayega ki post kis type ki hai
        cur.execute("""
            ALTER TABLE channel_posts 
            ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT 'movies'
        """)
        # content_type ke values honge:
        # 'movies'  -> Normal Movies
        # 'adult'   -> 18+ Content  
        # 'series'  -> Web Series
        # 'anime'   -> Anime
        conn.commit()
        cur.close()
        close_db_connection(conn)
        print("✅ content_type column added!")
    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
            close_db_connection(conn)
def fix_channel_posts_constraint():
    """UNIQUE constraint add karne wala function"""
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'channel_posts_unique_idx') THEN
                    ALTER TABLE channel_posts ADD CONSTRAINT channel_posts_unique_idx UNIQUE (channel_id, message_id);
                END IF;
            END $$;
        """)
        conn.commit()
        cur.close()
        logger.info("✅ Database UNIQUE Constraint fixed!")
    except Exception as e:
        logger.error(f"❌ DB Constraint Fix Error: {e}")
    finally:
        close_db_connection(conn)

def fix_movies_title_constraint():
    """Movies table mein title ko UNIQUE banane ke liye"""
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        # Title column ko unique banayenge taaki ON CONFLICT kaam kare
        cur.execute("ALTER TABLE movies ADD CONSTRAINT movies_title_unique UNIQUE (title);")
        conn.commit()
        cur.close()
        logger.info("✅ Movies table UNIQUE constraint added!")
    except Exception as e:
        logger.error(f"❌ Movies Constraint Error: {e}")
        if conn: conn.rollback()
    finally:
        close_db_connection(conn)
        
def fix_movies_unique_constraint():
    """Movies table mein title ko UNIQUE banata hai taaki bot crash na ho"""
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        # Title par UNIQUE constraint add kar rahe hain
        cur.execute("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'movies_title_key') THEN
                    ALTER TABLE movies ADD CONSTRAINT movies_title_key UNIQUE (title);
                END IF;
            END $$;
        """)
        conn.commit()
        cur.close()
        logger.info("✅ Movies table UNIQUE constraint fixed!")
    except Exception as e:
        logger.error(f"❌ Movies DB Fix Error: {e}")
    finally:
        close_db_connection(conn)

# 👇 Line 1225 ke baad yahan paste karein
def migrate_channel_posts_v2():
    """Ye function channel_posts table mein missing columns add karega"""
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        # Ek ek karke saare missing columns check aur add karega
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS caption TEXT;")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS media_file_id TEXT;")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS media_type TEXT DEFAULT 'photo';")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS keyboard_data TEXT;")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS topic_id INTEGER;")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS content_type TEXT DEFAULT 'movies';")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS is_restored BOOLEAN DEFAULT FALSE;")
        cur.execute("ALTER TABLE channel_posts ADD COLUMN IF NOT EXISTS restored_at TIMESTAMP;")
        
        conn.commit()
        cur.close()
        logger.info("✅ channel_posts table migrated to V2 successfully!")
    except Exception as e:
        logger.error(f"❌ Migration V2 Error: {e}")
    finally:
        close_db_connection(conn)

def save_post_to_db(
    movie_id,
    channel_id,
    message_id,
    bot_username,
    caption,
    media_file_id=None,
    media_type="photo",
    keyboard_data=None,
    topic_id=None,
    content_type="movies"    # ✅ NAYA: Default movies
):
    """
    Post ka full data save karo.
    content_type = 'movies' / 'adult' / 'series' / 'anime'
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO channel_posts 
                (movie_id, channel_id, message_id, bot_username,
                 caption, media_file_id, media_type, 
                 keyboard_data, topic_id, content_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (channel_id, message_id) DO UPDATE SET
                caption       = EXCLUDED.caption,
                media_file_id = EXCLUDED.media_file_id,
                media_type    = EXCLUDED.media_type,
                keyboard_data = EXCLUDED.keyboard_data,
                topic_id      = EXCLUDED.topic_id,
                content_type  = EXCLUDED.content_type
        """, (
            movie_id, channel_id, message_id, bot_username,
            caption, media_file_id, media_type,
            json.dumps(keyboard_data) if keyboard_data else None,
            topic_id,
            content_type    # ✅ Save hoga
        ))
        conn.commit()
        cur.close()
        close_db_connection(conn)
        return True
    except Exception as e:
        logger.error(f"Save post error: {e}")
        if conn:
            conn.rollback()
            close_db_connection(conn)
        return False


# 👇👇👇 START COPY HERE (New Function) 👇👇👇
def get_db_connection():
    """Pool se connection lene wala naya function"""
    if not db_pool:
        logger.error("Database pool is not ready.")
        return None
    try:
        conn = db_pool.getconn()
        return conn
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        return None

def close_db_connection(conn):
    """Connection ko wapas pool me dalne ke liye helper"""
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass
# 👆👆👆 END COPY HERE 👆👆👆

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
        last_sync_time = last_sync_result[0] if last_sync_result else None

        cur.execute("SELECT title FROM movies;")
        existing_movies = {row[0] for row in cur.fetchall()}

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
        if conn: close_db_connection(conn)


def get_movies_from_db(user_query, limit=10):
    """Search for MULTIPLE movies in database with fuzzy matching"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()

        logger.info(f"Searching for: '{user_query}'")

        # ✅ Updated to include new columns
        cur.execute(
            """SELECT id, title, url, file_id, imdb_id, poster_url, year, genre 
               FROM movies WHERE LOWER(title) LIKE LOWER(%s) ORDER BY title LIMIT %s""",
            (f'%{user_query}%', limit)
        )
        exact_matches = cur.fetchall()

        if exact_matches:
            logger.info(f"Found {len(exact_matches)} exact matches")
            cur.close()
            close_db_connection(conn)
            return exact_matches

        cur.execute("""
            SELECT DISTINCT m.id, m.title, m.url, m.file_id, m.imdb_id, m.poster_url, m.year, m.genre
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
            close_db_connection(conn)
            return alias_matches

        cur.execute("SELECT id, title, url, file_id, imdb_id, poster_url, year, genre FROM movies")
        all_movies = cur.fetchall()

        if not all_movies:
            cur.close()
            close_db_connection(conn)
            return []

        movie_titles = [movie[1] for movie in all_movies]
        movie_dict = {movie[1]: movie for movie in all_movies}

        matches = process.extract(user_query, movie_titles, scorer=fuzz.token_sort_ratio, limit=limit)

        filtered_movies = [movie_dict[title] for title, score, index in matches if score >= 65]

        logger.info(f"Found {len(filtered_movies)} fuzzy matches")

        cur.close()
        close_db_connection(conn)
        return filtered_movies[:limit]

    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def get_movies_fast_sql(query: str, limit: int = 5):
    """
    Smart SQL Search: Fast like SQL + Smart like FuzzyWuzzy.
    Handles typos using PostgreSQL 'pg_trgm' (Similarity).
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return []

        cur = conn.cursor()
        
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        
        # ✅ Updated to include new columns
        sql = """
            SELECT m.id, m.title, m.url, m.file_id, m.imdb_id, m.poster_url, m.year, m.genre,
                   SIMILARITY(m.title, %s) as sim_score
            FROM movies m
            WHERE SIMILARITY(m.title, %s) > 0.3
            ORDER BY sim_score DESC
            LIMIT %s
        """
        
        cur.execute(sql, (query, query, limit))
        results = cur.fetchall()
        
        # Format results (remove score from tuple)
        final_results = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]) for r in results]
        
        cur.close()
        return final_results

    except Exception as e:
        logger.error(f"Smart SQL Search Error: {e}")
        return []
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def get_movie_by_imdb_id(imdb_id: str):
    """Get movie from database by IMDb ID"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return None

        cur = conn.cursor()
        cur.execute(
            """SELECT id, title, url, file_id, imdb_id, poster_url, year, genre 
               FROM movies WHERE imdb_id = %s LIMIT 1""",
            (imdb_id,)
        )
        result = cur.fetchone()
        cur.close()
        close_db_connection(conn)
        return result

    except Exception as e:
        logger.error(f"Error fetching movie by IMDb ID: {e}")
        return None
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass


def update_movie_metadata(
    movie_id: int,
    imdb_id: str = None,
    poster_url: str = None,
    year: int = None,
    genre: str = None,
    rating: str = None,
    description: str = None,
    category: str = None
):
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cur = conn.cursor()
        updates, values = [], []

        def add(field, val):
            updates.append(f"{field} = %s")
            values.append(val)

        if imdb_id: add("imdb_id", imdb_id)
        if poster_url: add("poster_url", poster_url)
        if year is not None: add("year", year)
        if genre: add("genre", genre)
        if rating: add("rating", rating)
        if description: add("description", description)
        if category: add("category", category)

        if not updates:
            return False

        values.append(movie_id)
        cur.execute(f"UPDATE movies SET {', '.join(updates)} WHERE id = %s", values)
        conn.commit()
        cur.close()
        return True

    except Exception as e:
        logger.error(f"Error updating movie metadata: {e}", exc_info=True)
        try:
            if conn: conn.rollback()
        except Exception:
            pass
        return False
    finally:
        if conn:
            close_db_connection(conn)


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
        close_db_connection(conn)
        return True
    except Exception as e:
        logger.error(f"Error storing user request: {e}")
        try:
            conn.rollback()
            close_db_connection(conn)
        except:
            pass
        return False


# ==================== METADATA FUNCTIONS ====================

def is_valid_imdb_id(imdb_id: str) -> bool:
    """Validate IMDb ID format (tt1234567 or tt12345678)"""
    if not imdb_id:
        return False
    return bool(re.match(r'^tt\d{7,8}$', imdb_id.strip()))

def auto_fetch_and_update_metadata(movie_id: int, movie_title: str):
    """Automatically fetch and update metadata for a movie"""
    try:
        metadata = fetch_movie_metadata(movie_title)
        if metadata:
            title, year, poster_url, genre, imdb_id, rating = metadata  # 6 values unpack
            update_movie_metadata(
                movie_id=movie_id,
                imdb_id=imdb_id if imdb_id else None,
                poster_url=poster_url if poster_url else None,
                year=year if year else None,
                genre=genre if genre else None,
                rating=rating if rating and rating != 'N/A' else None  # Rating add करें
            )
            return True
        return False
    except Exception as e:
        logger.error(f"Error in auto_fetch_and_update_metadata: {e}")
        return False

# ============================================================================
# 🔍 GOOGLE SEARCH METADATA FETCHER (Premium Edition)
# ============================================================================

async def fetch_metadata_from_google(query: str, search_year: str = ""):
    API_KEY = os.environ.get("GOOGLE_API_KEY")
    CX_ID = os.environ.get("GOOGLE_CX_ID")
    
    if not API_KEY or not CX_ID:
        return None
    
    search_query = f"{query} {search_year} poster plot".strip()
    
    try:
        encoded = quote(search_query)
        
        base_url = "https://www.googleapis.com/customsearch/v1"
        
        # ---------- IMAGE SEARCH ----------
        img_url = f"{base_url}?key={API_KEY}&cx={CX_ID}&q={encoded}&num=5&searchType=image"
        response = await run_async(requests.get, img_url, timeout=10)
        data = response.json()
        
        items = data.get("items", [])
        
        # ---------- FALLBACK TO TEXT ----------
        if not items:
            txt_url = f"{base_url}?key={API_KEY}&cx={CX_ID}&q={encoded}&num=5"
            response = await run_async(requests.get, txt_url, timeout=10)
            data = response.json()
            items = data.get("items", [])
        
        if not items:
            return None
        
        # ---------- PICK BEST RESULT ----------
        best_item = items[0]
        
        title = clean_title(best_item.get("title", query))
        snippet = best_item.get("snippet", "")
        
        # ---------- IMAGE EXTRACTION ----------
        image_url = None
        pagemap = best_item.get("pagemap", {})
        
        if "cse_image" in pagemap:
            image_url = pagemap["cse_image"][0].get("src")
        elif "cse_thumbnail" in pagemap:
            image_url = pagemap["cse_thumbnail"][0].get("src")
        
        # fallback: direct link image
        if not image_url:
            link = best_item.get("link", "")
            if link.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                image_url = link
        
        # ---------- EXTRA CLEANUPS ----------
        plot = snippet[:300] if snippet else "Premium content available."
        
        # better genre detection (thoda smart banaya 😏)
        q_lower = query.lower()
        if any(x in q_lower for x in ['bhabhi', 'unrated', 'adult', 'hot']):
            genre = "Adult"
        elif any(x in q_lower for x in ['crime', 'murder', 'thriller']):
            genre = "Crime/Thriller"
        else:
            genre = "Drama"
        
        return {
            "title": title,
            "poster": image_url or DEFAULT_POSTER,
            "plot": plot,
            "year": search_year or "2024-2026",
            "genre": genre,
            "category": "Web Series"
        }
        
    except Exception as e:
        logger.error(f"Google Search Error: {e}")
        return None

# ============================================================================
# 🔧 HELPER FUNCTIONS
# ============================================================================

def clean_google_title(raw_title: str) -> str:
    """Google title se junk hatao"""
    # Common patterns remove karo
    junk_patterns = [
        r' - IMDb$', r' - Wikipedia$', r' - Rotten Tomatoes$',
        r' \| Netflix$', r' - Prime Video$', r' \| .*?Official',
        r'Watch ', r' Online', r' Full Movie', r' Download'
    ]
    
    title = raw_title
    for pattern in junk_patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)
    
    # Year hatao agar title me hai
    title = re.sub(r'\s*\(\d{4}\)\s*', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title


def clean_plot(raw_snippet: str) -> str:
    """Google snippet ko clean plot banao"""
    # Ellipsis hatao
    plot = raw_snippet.replace('...', ' ')
    
    # URLs hatao
    plot = re.sub(r'https?://\S+', '', plot)
    
    # Extra spaces clean karo
    plot = re.sub(r'\s+', ' ', plot).strip()
    
    # Limit karo
    if len(plot) > 300:
        plot = plot[:297] + "..."
    
    return plot if plot else "Premium content available on FlimfyBox."


async def extract_imdb_poster(imdb_url: str) -> Optional[str]:
    """IMDb page se poster nikalo (Fallback)"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0'
        }
        response = await run_async(requests.get, imdb_url, headers=headers, timeout=8)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Meta tag se poster
        meta_img = soup.find('meta', property='og:image')
        if meta_img:
            return meta_img.get('content')
        
        # JSON-LD se poster
        script = soup.find('script', type='application/ld+json')
        if script:
            import json
            data = json.loads(script.string)
            if 'image' in data:
                return data['image']
                
    except Exception as e:
        logger.warning(f"IMDb poster extraction failed: {e}")
    
    return None


def extract_tmdb_poster_from_url(tmdb_url: str) -> Optional[str]:
    """TMDB URL se poster ID nikalo"""
    try:
        # Pattern: themoviedb.org/movie/12345-movie-name
        match = re.search(r'/(movie|tv)/(\d+)', tmdb_url)
        if match:
            media_type, tmdb_id = match.groups()
            # TMDB poster URL construct karo
            return f"https://image.tmdb.org/t/p/w500/{tmdb_id}.jpg"  # Simplified
    except:
        pass
    return None

def fetch_cast_from_imdb(imdb_id: str, limit: int = 5) -> str:
    """Fetch cast list from TMDB using IMDb ID, return comma-separated string."""
    try:
        api_key = "9fa44f5e9fbd41415df930ce5b81c4d7"
        find_url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={api_key}&external_source=imdb_id"
        resp = requests.get(find_url, timeout=10).json()
        tmdb_results = resp.get('movie_results', [])
        if not tmdb_results:
            tmdb_results = resp.get('tv_results', [])
        if not tmdb_results:
            return ""
        tmdb_id = tmdb_results[0]['id']
        media_type = 'movie' if resp.get('movie_results') else 'tv'
        credits_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/credits?api_key={api_key}"
        credits = requests.get(credits_url, timeout=10).json()
        cast = credits.get('cast', [])[:limit]
        if cast:
            return ', '.join([c['name'] for c in cast])
    except Exception as e:
        logger.error(f"Failed to fetch cast for {imdb_id}: {e}")
    return ""

# ==================== NEW METADATA HELPER FUNCTIONS ====================

def get_tmdb_backdrop(query, search_year=""):
    """TMDB API se HD Original Poster (Vertical with Text) nikalta hai"""
    api_key = "9fa44f5e9fbd41415df930ce5b81c4d7" 
    try:
        url = f"https://api.themoviedb.org/3/search/multi?api_key={api_key}&query={quote(query)}"
        resp = requests.get(url, timeout=10).json()
        
        if resp.get('results'):
            for item in resp['results']:
                item_year = str(item.get('release_date', item.get('first_air_date', '')))[:4]
                if search_year and str(search_year) != item_year:
                    continue
                
                # 🛑 NAYA: Ab pehle Original Poster (Jisme Text hota hai) dhundega
                if item.get('poster_path'):
                    return f"https://image.tmdb.org/t/p/original{item['poster_path']}"
                elif item.get('backdrop_path'):
                    return f"https://image.tmdb.org/t/p/original{item['backdrop_path']}"
            
            first = resp['results'][0]
            if first.get('poster_path'):
                return f"https://image.tmdb.org/t/p/original{first['poster_path']}"
            elif first.get('backdrop_path'):
                return f"https://image.tmdb.org/t/p/original{first['backdrop_path']}"
    except Exception as e:
        logger.error(f"TMDB Error: {e}")
    return None

def fetch_movie_metadata(query: str, search_year: str = "", search_lang: str = "", adult_mode: bool = False):
    """
    IMDb से डेटा और TMDb से सिर्फ Lamba (Portrait) पोस्टर निकालने वाला इंजन
    adult_mode=True होने पर TMDb सर्च में include_adult=true भेजेगा और OMDb को बायपास करेगा।
    """
    omdb_api_key = os.environ.get("OMDB_API_KEY")
    tmdb_api_key = "9fa44f5e9fbd41415df930ce5b81c4d7"

    search_query = query.strip()
    is_imdb_id = bool(re.match(r'^tt\d{7,8}$', search_query))

    # ----- एडल्ट मोड: OMDb का उपयोग न करें (क्योंकि उसमें एडल्ट डेटा नहीं) -----
    if adult_mode:
        # सीधे TMDb का उपयोग करें
        try:
            tmdb_search = f"https://api.themoviedb.org/3/search/multi?api_key={tmdb_api_key}&query={quote(search_query)}&include_adult=true"
            if search_year and search_year.strip().isdigit():
                tmdb_search += f"&year={search_year.strip()}"
            t_resp = requests.get(tmdb_search, timeout=10).json()
            if not t_resp.get('results'):
                return None

            best_match = t_resp['results'][0]
            # वर्ष मैच करने की कोशिश
            if search_year and str(search_year).strip().isdigit():
                for item in t_resp['results']:
                    item_year = str(item.get('release_date', item.get('first_air_date', '')))[:4]
                    if str(search_year).strip() == item_year:
                        best_match = item
                        break

            title = best_match.get('title') or best_match.get('name') or search_query
            year_str = str(best_match.get('release_date', best_match.get('first_air_date', '')))[:4]
            year = int(year_str) if year_str.isdigit() else 0
            plot = best_match.get('overview', 'No story available.')
            rating = str(round(best_match.get('vote_average', 0), 1)) if best_match.get('vote_average') else 'N/A'
            category = "Adult"  # जबरदस्ती Adult
            genre = "Romance, Drama"  # डिफ़ॉल्ट, TMDb जॉनर बाद में ला सकते हैं

            path = best_match.get('poster_path')
            poster_url = f"https://image.tmdb.org/t/p/original{path}" if path else None

            imdb_id = None
            try:
                tmdb_id = best_match.get('id')
                media_type = best_match.get('media_type', 'movie')
                ext_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids?api_key={tmdb_api_key}"
                imdb_id = requests.get(ext_url, timeout=5).json().get('imdb_id')
            except:
                pass

            return title, year, poster_url, genre, imdb_id, rating, plot, category
        except Exception as e:
            logger.error(f"Adult TMDb Fetch Error: {e}")
            return None

    # ----- नॉर्मल मोड (मूल कोड) -----
    if not omdb_api_key:
        logger.error("❌ OMDB_API_KEY missing in .env")
        return None

    try:
        if is_imdb_id:
            url = f"https://www.omdbapi.com/?i={search_query}&apikey={omdb_api_key}&plot=full"
        else:
            url = f"https://www.omdbapi.com/?t={quote(search_query)}&apikey={omdb_api_key}&plot=full"
            if search_year and str(search_year).strip().isdigit():
                url += f"&y={str(search_year).strip()}"

        resp = requests.get(url, timeout=10).json()

        if resp.get("Response") != "True":
            # OMDb फेल होने पर TMDb का उपयोग करें
            tmdb_search = f"https://api.themoviedb.org/3/search/multi?api_key={tmdb_api_key}&query={quote(search_query)}"
            if search_year and str(search_year).strip().isdigit():
                tmdb_search += f"&year={search_year.strip()}"
            t_resp = requests.get(tmdb_search, timeout=10).json()
            if not t_resp.get('results'):
                return None

            best_match = t_resp['results'][0]
            if search_year and str(search_year).strip().isdigit():
                for item in t_resp['results']:
                    item_year = str(item.get('release_date', item.get('first_air_date', '')))[:4]
                    if str(search_year).strip() == item_year:
                        best_match = item
                        break

            title = best_match.get('title') or best_match.get('name') or search_query
            year_str = str(best_match.get('release_date', best_match.get('first_air_date', '')))[:4]
            year = int(year_str) if year_str.isdigit() else 0
            plot = best_match.get('overview', 'No story available.')
            rating = str(round(best_match.get('vote_average', 0), 1)) if best_match.get('vote_average') else 'N/A'
            category = "Movies" if best_match.get('media_type') == 'movie' else "Web Series"
            genre = "Action, Drama"
            path = best_match.get('poster_path')
            poster_url = f"https://image.tmdb.org/t/p/original{path}" if path else None

            imdb_id = None
            try:
                tmdb_id = best_match.get('id')
                media_type = best_match.get('media_type', 'movie')
                ext_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids?api_key={tmdb_api_key}"
                imdb_id = requests.get(ext_url, timeout=5).json().get('imdb_id')
            except:
                pass

            return title, year, poster_url, genre, imdb_id, rating, plot, category

        # OMDb से डेटा सफल
        title = resp.get('Title')
        year = int(resp.get('Year', '0').split('–')[0]) if resp.get('Year') else 0
        genre = resp.get('Genre', 'Action, Drama')
        rating = resp.get('imdbRating', 'N/A')
        plot = resp.get('Plot', 'No story available.')
        imdb_id = resp.get('imdbID')
        country = resp.get('Country', '')
        lang = resp.get('Language', '').lower()

        category = "Movies"
        g_low = genre.lower()
        if "animation" in g_low or "anime" in g_low:
            category = "Anime"
        elif "series" in g_low or "episode" in g_low:
            category = "Web Series"
        elif "india" in country.lower():
            if any(x in lang for x in ['telugu', 'tamil', 'kannada', 'malayalam']):
                category = "South"
            else:
                category = "Bollywood"
        else:
            category = "Hollywood"

        # TMDb से HD पोस्टर लाना
        poster_url = resp.get('Poster')
        if imdb_id and imdb_id != 'N/A':
            tmdb_find = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={tmdb_api_key}&external_source=imdb_id"
            t_resp = requests.get(tmdb_find, timeout=10).json()
            results = t_resp.get('movie_results', []) + t_resp.get('tv_results', [])
            if results:
                path = results[0].get('poster_path')
                if path:
                    poster_url = f"https://image.tmdb.org/t/p/original{path}"
        else:
            tmdb_search = f"https://api.themoviedb.org/3/search/multi?api_key={tmdb_api_key}&query={quote(title)}"
            t_resp = requests.get(tmdb_search, timeout=10).json()
            if t_resp.get('results'):
                for item in t_resp['results']:
                    item_year = str(item.get('release_date', item.get('first_air_date', '')))[:4]
                    if str(year) == item_year and item.get('poster_path'):
                        poster_url = f"https://image.tmdb.org/t/p/original{item['poster_path']}"
                        break
                else:
                    path = t_resp['results'][0].get('poster_path')
                    if path:
                        poster_url = f"https://image.tmdb.org/t/p/original{path}"

        return title, year, poster_url, genre, imdb_id, rating, plot, category

    except Exception as e:
        logger.error(f"Metadata Fetch Error: {e}")
        return None

# ==================== AI INTENT ANALYSIS ====================
# 👇👇👇 START COPY HERE 👇👇👇
async def analyze_intent(message_text):
    """
    Bina AI (Gemini) ke message analyze karna.
    Isse API limit waste nahi hogi!
    """
    try:
        text_lower = message_text.lower().strip()
        
        # 1. Agar message bahut lamba hai ya usme Link hai, toh reject kar do
        if len(text_lower) > 60 or "http" in text_lower or "t.me" in text_lower:
            return {"is_request": False, "content_title": None}

        # 2. Agar chota message hai, toh usko direct Movie ka naam maan lo
        # Faltu words hatane ki koshish (Optional)
        words_to_remove = ["please", "plz", "bhai", "movie", "series", "chahiye", "give", "me"]
        clean_name = text_lower
        for word in words_to_remove:
            clean_name = clean_name.replace(word, "").strip()

        if len(clean_name) < 2:
            return {"is_request": False, "content_title": None}

        return {"is_request": True, "content_title": message_text.strip()}

    except Exception as e:
        logger.error(f"Error in intent analysis: {e}")
        return {"is_request": True, "content_title": message_text.strip()}
# 👆👆👆 END COPY HERE 👆👆👆

# ==================== NOTIFICATION FUNCTIONS ====================
async def send_admin_notification(context, user, movie_title, group_info=None):
    """Send notification to admin channel about a new request with Lifetime Buttons"""
    if not REQUEST_CHANNEL_ID: return

    try:
        safe_movie_title = movie_title.replace('<', '&lt;').replace('>', '&gt;')
        safe_username = user.username if user.username else 'N/A'
        safe_first_name = (user.first_name or 'Unknown').replace('<', '&lt;').replace('>', '&gt;')

        message = f"🎬 New Movie Request! 🎬\n\n"
        message += f"Movie: <b>{safe_movie_title}</b>\n"
        message += f"User: {safe_first_name} (ID: <code>{user.id}</code>)\n"
        if user.username: message += f"Username: @{safe_username}\n"
        message += f"From: {'Group: '+str(group_info) if group_info else 'Private Message'}\n"
        message += f"Time: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}"

        # ⚡ LIFETIME BUTTONS LOGIC
        # Telegram me button data limit 64 bytes hoti hai, isliye title chota kiya hai
        short_title = safe_movie_title[:15].replace('_', ' ') 
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Movie Add Kar Di Gai Hai", callback_data=f"reqA_{user.id}_{short_title}")],
            [InlineKeyboardButton("❌ Nahi Mili", callback_data=f"reqN_{user.id}_{short_title}")]
        ])

        await context.bot.send_message(
            chat_id=REQUEST_CHANNEL_ID,
            text=message,
            parse_mode='HTML',
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Error sending admin notification: {e}")

async def notify_users_for_movie(context: ContextTypes.DEFAULT_TYPE, movie_title, movie_url_or_file_id):
    logger.info(f"Attempting to notify users for movie: {movie_title}")
    conn = None
    cur = None
    notified_count = 0

    caption_text = (
        f"🎬 <b>{movie_title}</b>\n\n"
        "➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 <b>Please drop the movie name, and I'll find it for you as soon as possible. 🎬✨👇</b>\n"
        "➖➖➖➖➖➖➖➖➖➖\n"
        "🔹 <b>Support group:</b> https://t.me/+2hFeRL4DYfBjZDQ1\n"
    )
    join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url=FILMFYBOX_CHANNEL_URL)]])

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
                # Optional heads-up text
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"🎉 Hey {first_name or username or 'there'}! Your requested movie '{movie_title}' is now available!"
                    )
                except Exception:
                    pass

                warning_msg = None
                try:
                    warning_msg = await context.bot.copy_message(
                        chat_id=user_id,
                        from_chat_id=int(DUMP_CHANNEL_ID),
                        message_id=1773
                    )
                except Exception:
                    warning_msg = None

                sent_msg = None

                val = str(movie_url_or_file_id or "").strip()

                # Telegram file_id heuristics (your existing logic)
                is_file_id = any(val.startswith(prefix) for prefix in ["BQAC", "BAAC", "CAAC", "AQAC"])

                if is_file_id:
                    # try video then document
                    try:
                        sent_msg = await context.bot.send_video(
                            chat_id=user_id, video=val, caption=caption_text,
                            parse_mode='HTML', reply_markup=join_keyboard
                        )
                    except telegram.error.BadRequest:
                        sent_msg = await context.bot.send_document(
                            chat_id=user_id, document=val, caption=caption_text,
                            parse_mode='HTML', reply_markup=join_keyboard
                        )

                elif val.startswith("https://t.me/c/"):
                    parts = val.split('/')
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

                elif val.startswith("http"):
                    sent_msg = await context.bot.send_message(
                        chat_id=user_id,
                        text=f"{caption_text}\n\n<b>Link:</b> {val}",
                        parse_mode='HTML',
                        disable_web_page_preview=True,
                        reply_markup=join_keyboard
                    )

                else:
                    # last fallback: try send as document
                    sent_msg = await context.bot.send_document(
                        chat_id=user_id,
                        document=val,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )

                # Auto delete both after 60 seconds
                ids = []
                if sent_msg:
                    ids.append(sent_msg.message_id)
                if warning_msg:
                    ids.append(warning_msg.message_id)
                if ids:
                    asyncio.create_task(delete_messages_after_delay(context, user_id, ids, 60))

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
                logger.error(f"Error notifying user {user_id}: {e}", exc_info=True)
                continue

        return notified_count

    except Exception as e:
        logger.error(f"Error in notify_users_for_movie: {e}", exc_info=True)
        return 0
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            close_db_connection(conn)

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
        if conn: close_db_connection(conn)

# ==================== NEW GENRE FUNCTIONS ====================

def get_all_genres_from_db():
    """Fetch all unique genres from database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT genre FROM movies WHERE genre IS NOT NULL AND genre != ''")
        results = cur.fetchall()
        
        # Parse comma-separated genres and flatten
        all_genres = []
        for row in results:
            genre_str = row[0]
            if genre_str:
                # Split by comma and strip spaces
                genres = [g.strip() for g in genre_str.split(',')]
                all_genres.extend(genres)
        
        # Remove duplicates and return sorted list
        unique_genres = sorted(set(all_genres))
        cur.close()
        close_db_connection(conn)
        return unique_genres
        
    except Exception as e:
        logger.error(f"Error fetching genres: {e}")
        return []
    finally:
        if conn:
            close_db_connection(conn)


def create_genre_selection_keyboard():
    """Create inline keyboard with genre selection buttons"""
    genres = get_all_genres_from_db()
    
    if not genres:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ No Genres Found", callback_data="cancel_genre")]])
    
    keyboard = []
    row = []
    
    for idx, genre in enumerate(genres):
        row.append(InlineKeyboardButton(
            f"📂 {genre}",
            callback_data=f"genre_{genre}"
        ))
        
        # 2 buttons per row
        if (idx + 1) % 2 == 0:
            keyboard.append(row)
            row = []
    
    # Add remaining buttons
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_genre")])
    return InlineKeyboardMarkup(keyboard)


def get_movies_by_genre(genre: str, limit: int = 10):
    """Fetch movies filtered by genre"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        # Use ILIKE for case-insensitive search within genre string
        cur.execute("""
            SELECT id, title, url, file_id, poster_url, year 
            FROM movies 
            WHERE genre ILIKE %s
            ORDER BY year DESC NULLS LAST
            LIMIT %s
        """, (f'%{genre}%', limit))
        
        results = cur.fetchall()
        cur.close()
        close_db_connection(conn)
        return results
        
    except Exception as e:
        logger.error(f"Error fetching movies by genre: {e}")
        return []
    finally:
        if conn:
            close_db_connection(conn)


async def show_genre_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 'Browse by Genre' button click"""
    if update.message:
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # FSub check
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            msg = await update.message.reply_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            track_message_for_deletion(context, chat_id, msg.message_id, 120)
            return
        
        # Show genre selection
        keyboard = create_genre_selection_keyboard()
        msg = await update.message.reply_text(
            "📂 **Select a genre to browse movies:**",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        track_message_for_deletion(context, chat_id, msg.message_id, 180)


async def handle_genre_selection(update: Update, context:  ContextTypes.DEFAULT_TYPE):
    """Handle genre selection callback"""
    query = update.callback_query
    await query.answer()
    
    data = query. data
    
    if data == "cancel_genre":
        await query.edit_message_text("❌ Genre browsing cancelled.")
        return
    
    if data.startswith("genre_"):
        genre = data.replace("genre_", "")
        
        # Fetch movies for this genre
        movies = get_movies_by_genre(genre, limit=15)
        
        if not movies:
            await query.edit_message_text(
                f"😕 No movies found for genre: **{genre}**\n\n"
                "Try another genre or use 🔍 Search.",
                parse_mode='Markdown'
            )
            return
        
        # Create movie selection keyboard
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = genre
        
        keyboard = create_movie_selection_keyboard(movies, page=0)  # ✅ Now handles 6-tuple
        
        await query.edit_message_text(
            f"🎬 **Found {len(movies)} movies in '{genre}' genre**\n\n"
            "👇 Select a movie:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
# ==================== KEYBOARD MARKUPS ====================
def get_main_keyboard():
    keyboard = [
        ['🔍 Search Movies'],
        ['📂 Browse by Genre', '🙋 Request Movie'],
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

def get_movie_options_keyboard(movie_title, url, movie_id=None, file_info=None):
    keyboard = []

    # Scan info only if movie_id is available
    if movie_id is not None:
        keyboard.append([InlineKeyboardButton("ℹ️ SCAN INFO : AUDIO & SUBS", callback_data=f"scan_{movie_id}")])

    if url:
        keyboard.append([InlineKeyboardButton("🎬 Watch Now", url=url)])

    keyboard.append([InlineKeyboardButton("📥 Download", callback_data=f"download_{movie_title[:50]}")])
    keyboard.append([InlineKeyboardButton("➡️ Join Channel", url=FILMFYBOX_CHANNEL_URL)])

    return InlineKeyboardMarkup(keyboard)

def create_movie_selection_keyboard(movies, page=0, movies_per_page=5):
    start_idx = page * movies_per_page
    end_idx = start_idx + movies_per_page
    current_movies = movies[start_idx:end_idx]

    keyboard = []

    for movie in current_movies:
        # FIX: check 8-tuple before 6-tuple
        if len(movie) >= 8:
            movie_id, title, url, file_id, imdb_id, poster_url, year, genre = movie[:8]
        elif len(movie) >= 6:
            movie_id, title, url, file_id, poster_url, year = movie[:6]
        else:
            movie_id, title = movie[0], movie[1]

        button_text = title if len(title) <= 40 else title[:37] + "..."
        keyboard.append([InlineKeyboardButton(f"🎬 {button_text}", callback_data=f"movie_{movie_id}")])

    total_pages = (len(movies) + movies_per_page - 1) // movies_per_page
    nav_buttons = []

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
        # NAYI QUERY: languages aur extra_info add kiya hai
        cur.execute("""
            SELECT quality, url, file_id, file_size, languages, extra_info
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
            close_db_connection(conn)

# create_quality_selection_keyboard function ko isse replace karein ya modify karein:

def create_quality_selection_keyboard(movie_id, view="main", page=1, total_pages=1, current_files=None, season_view=False):
    """नया UI: फाइल्स के लिए बटन्स, फिल्टर्स और पेजिनेशन"""
    keyboard = []
    
    if view == "main":

        # 2. अगर सीजन के अंदर हैं, तो बैक बटन दिखाओ
        if season_view:
            keyboard.append([InlineKeyboardButton("🔙 Back to Seasons", callback_data=f"back_to_seasons_{movie_id}")])

        # 3. Send All बटन
        keyboard.append([InlineKeyboardButton("🚀 SEND ALL", callback_data=f"sendall_{movie_id}")])
        
        # 4. Filters
        keyboard.append([
            InlineKeyboardButton("QUALITY", callback_data=f"v_qual_{movie_id}"),
            InlineKeyboardButton("LANGUAGE", callback_data=f"v_lang_{movie_id}"),
            InlineKeyboardButton("SEASON", callback_data=f"v_seas_{movie_id}")
        ])
        
        # 5. Pagination
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("◀️ PREV" if page > 1 else "PAGE", callback_data=f"vpage_{movie_id}_{page-1}" if page > 1 else "ignore"))
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="ignore"))
        nav_buttons.append(InlineKeyboardButton("NEXT ▶️" if page < total_pages else "NEXT >", callback_data=f"vpage_{movie_id}_{page+1}" if page < total_pages else "ignore"))
        keyboard.append(nav_buttons)

    # ... (बाकी व्यूज जैसे language, quality, season पहले जैसे ही रहेंगे)
    elif view == "language":
                keyboard.append([InlineKeyboardButton("MALAYALAM", callback_data=f"fl_lang_{movie_id}_Malayalam"), InlineKeyboardButton("TAMIL", callback_data=f"fl_lang_{movie_id}_Tamil")])
                keyboard.append([InlineKeyboardButton("ENGLISH", callback_data=f"fl_lang_{movie_id}_English"), InlineKeyboardButton("HINDI", callback_data=f"fl_lang_{movie_id}_Hindi")])
                keyboard.append([InlineKeyboardButton("TELUGU", callback_data=f"fl_lang_{movie_id}_Telugu"), InlineKeyboardButton("KANNADA", callback_data=f"fl_lang_{movie_id}_Kannada")])
                # ✅ NAYA: Gujarati, Marathi aur Punjabi add ho gaye
                keyboard.append([InlineKeyboardButton("GUJARATI", callback_data=f"fl_lang_{movie_id}_Gujarati"), InlineKeyboardButton("MARATHI", callback_data=f"fl_lang_{movie_id}_Marathi")])
                keyboard.append([InlineKeyboardButton("PUNJABI", callback_data=f"fl_lang_{movie_id}_Punjabi")])
                keyboard.append([InlineKeyboardButton("🔄 CLEAR FILTER", callback_data=f"fl_clear_{movie_id}_all")])
                keyboard.append([InlineKeyboardButton("<< BACK TO FILES >>", callback_data=f"v_main_{movie_id}")])

    elif view == "quality":
                keyboard.append([InlineKeyboardButton("360P", callback_data=f"fl_qual_{movie_id}_360p"), InlineKeyboardButton("480P", callback_data=f"fl_qual_{movie_id}_480p")])
                keyboard.append([InlineKeyboardButton("720P", callback_data=f"fl_qual_{movie_id}_720p"), InlineKeyboardButton("1080P", callback_data=f"fl_qual_{movie_id}_1080p")])
                # ✅ NAYA: 1440P aur 2160P (Premium Quality) add ho gaye
                keyboard.append([InlineKeyboardButton("1440P", callback_data=f"fl_qual_{movie_id}_1440p"), InlineKeyboardButton("2160P", callback_data=f"fl_qual_{movie_id}_2160p")])
                keyboard.append([InlineKeyboardButton("4K", callback_data=f"fl_qual_{movie_id}_4K")])
                keyboard.append([InlineKeyboardButton("🔄 CLEAR FILTER", callback_data=f"fl_clear_{movie_id}_all")])
                keyboard.append([InlineKeyboardButton("<< BACK TO FILES >>", callback_data=f"v_main_{movie_id}")])

    elif view == "season":
        # ये डमी है, असली सीजन्स डायनामिकली बनते हैं
        keyboard.append([InlineKeyboardButton("🔄 CLEAR FILTER", callback_data=f"fl_clear_{movie_id}_all")])
        keyboard.append([InlineKeyboardButton("<< BACK TO FILES >>", callback_data=f"v_main_{movie_id}")])

    return InlineKeyboardMarkup(keyboard)

# ==================== HELPER FUNCTION ====================
async def send_movie_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int, title: str, url: Optional[str] = None, file_id: Optional[str] = None, send_warning: bool = True, pre_fetched_meta: dict = None):
    """Sends the movie file/link to the user with THUMBNAIL PROTECTION - OPTIMIZED & FIXED"""
    chat_id = update.effective_chat.id

    # --- 1. Fetch movie details (Genre, Year, Language) ---
    genre = ""
    year = ""
    lang_display = ""
    extra_display = "" # NAYA: Info (Ep) dikhane ke liye

    # ✅ OPTIMIZATION: Agar data pehle se diya gaya hai, to DB connect mat karo
    if pre_fetched_meta:
        db_genre = pre_fetched_meta.get('genre')
        db_year = pre_fetched_meta.get('year')
        db_lang = pre_fetched_meta.get('language')
        
        if db_genre and db_genre != 'Unknown': genre = f"🎭 <b>Genre:</b> {db_genre}\n"
        if db_year and db_year > 0: year = f"📅 <b>Year:</b> {db_year}\n"
        if db_lang and db_lang.strip(): lang_display = f"🔊 <b>Language:</b> {db_lang}\n"
    
    # Agar data nahi diya gaya, tabhi DB open karo
    else:
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT genre, year, language FROM movies WHERE id = %s", (movie_id,))
                result = cur.fetchone()
                if result:
                    db_genre, db_year, db_lang = result
                    if db_genre and db_genre != 'Unknown': genre = f"🎭 <b>Genre:</b> {db_genre}\n"
                    if db_year and db_year > 0: year = f"📅 <b>Year:</b> {db_year}\n"
                    if db_lang and db_lang.strip(): lang_display = f"🔊 <b>Language:</b> {db_lang}\n"
                cur.close()
            except Exception as e:
                logger.error(f"Error fetching movie info: {e}")
            finally:
                close_db_connection(conn)

    # 👇 NAYA CODE: Yahan hum us ek specific file ka episode 'movie_files' table se nikalenge! 👇
    if url or file_id:
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                if file_id:
                    cur.execute("SELECT extra_info FROM movie_files WHERE file_id = %s LIMIT 1", (file_id,))
                else:
                    cur.execute("SELECT extra_info FROM movie_files WHERE url = %s LIMIT 1", (url,))
                res = cur.fetchone()
                if res and res[0] and res[0].strip():
                    extra_display = f"📌 <b>Episode:</b> {res[0]}\n"
                cur.close()
            except Exception:
                pass
            finally:
                close_db_connection(conn)
    # 👆 ---------------------------------------------------- 👆

    # 1. Multi-Quality Check (Agar direct link/file nahi hai)
    if not url and not file_id:
        all_qualities = get_all_movie_qualities(movie_id)
        if all_qualities:
            context.user_data['selected_movie_data'] = {'id': movie_id, 'title': title, 'qualities': all_qualities}
            context.user_data['active_filter'] = None
            context.user_data.pop('selected_season', None)
            
            limit = 10
            total_pages = (len(all_qualities) + limit - 1) // limit if all_qualities else 1
            current_files = all_qualities[0:limit]
            
            # 👇 YAHAN SE FIX SHURU HOTA HAI (HTML INLINE LINKS KE LIYE) 👇
            bot_username = context.bot.username
            text = f"📁 <b>{title}</b>\n\n👇 <b>Your Requested Files Are Here</b>\n\n"
            
            import re # Text clean karne ke liye taaki links break na hon
            
            for idx, f_data in enumerate(current_files, start=1):
                q_name = str(f_data[0])
                
                # Kachra saaf kar rahe hain taaki deep links perfect banein
                q_name = re.sub(r'\[([^\]]+)\]\(https?://[^\)]+\)', r'\1', q_name)
                q_name = re.sub(r'\(https?://[^\)]+\)', '', q_name)
                q_name = re.sub(r'https?://[^\s]+', '', q_name)
                q_name = re.sub(r'(?i)t\.me/[^\s]+', '', q_name)
                q_name = re.sub(r'@[a-zA-Z0-9_]+', '', q_name)
                
                f_size = f_data[3] if len(f_data)>3 else "Unknown"
                
                e_info = str(f_data[5]) if len(f_data)>5 else ""
                e_info = re.sub(r'\[([^\]]+)\]\(https?://[^\)]+\)', r'\1', e_info)
                e_info = re.sub(r'\(https?://[^\)]+\)', '', e_info)
                e_info = re.sub(r'https?://[^\s]+', '', e_info)
                e_info = re.sub(r'(?i)t\.me/[^\s]+', '', e_info)
                e_info = re.sub(r'@[a-zA-Z0-9_]+', '', e_info)
                
                ep_tag = f"[{e_info.strip()}] " if e_info.strip() else ""
                
                # ✅ NAYA: HTML wala Neela (Inline) link
                text += f"<b>{idx}.</b> <b><a href='https://t.me/{bot_username}?start=file_{movie_id}_{idx-1}'>{f_size} | {title} {ep_tag}{q_name.strip()}</a></b>\n\n"
                
            keyboard = create_quality_selection_keyboard(movie_id, view="main", page=1, total_pages=total_pages, current_files=current_files)
            
            # ✅ NAYA: parse_mode='HTML' kar diya aur link preview off kar diya
            msg = await context.bot.send_message(
                chat_id=chat_id, 
                text=text, 
                reply_markup=keyboard, 
                parse_mode='HTML', 
                disable_web_page_preview=True
            )
            # 👆 FIX KHATAM 👆
            
            track_message_for_deletion(context, chat_id, msg.message_id, 60)
            return

    try:
        warning_msg = None
        if send_warning:
            try:
                warning_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=-1002683355160,
                    message_id=1773
                )
            except Exception as e:
                logger.error(f"Warning file send failed: {e}")
        
        # --- CAPTION UPDATE WITH EXTRA INFO ---
        caption_text = (
            f"🎬 <b>{title}</b>\n"
            f"{extra_display}"
            f"{year}"        
            f"{genre}"       
            f"{lang_display}"  
            f"\n🔗 <b>JOIN »</b> <a href='{FILMFYBOX_CHANNEL_URL}'>FilmfyBox</a>\n\n"
            f"🔹 <b>Please drop the movie name, and I'll find it for you as soon as possible. 🎬✨👇</b>\n"
            f"🔹 <b><a href='https://t.me/+2hFeRL4DYfBjZDQ1'>FlimfyBox Chat</a></b>"
        )
        
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url=FILMFYBOX_CHANNEL_URL)]])

        sent_msg = None
        if url and ("t.me/c/" in url or "t.me/" in url) and "http" in url:
            try:
                clean_url = url.strip()
                parts = clean_url.rstrip('/').split('/')
                msg_id = int(parts[-1])
                
                if "t.me/c/" in clean_url:
                    from_chat_id = int("-100" + parts[-2])
                else:
                    from_chat_id = f"@{parts[-2]}"

                sent_msg = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=msg_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except Exception as e:
                logger.error(f"Copy link failed: {e}")

        if not sent_msg and file_id:
            clean_file_id = str(file_id).strip()
            try:
                sent_msg = await context.bot.send_video(
                    chat_id=chat_id,
                    video=clean_file_id,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_markup=join_keyboard
                )
            except telegram.error.BadRequest:
                try:
                    sent_msg = await context.bot.send_document(
                        chat_id=chat_id,
                        document=clean_file_id,
                        caption=caption_text,
                        parse_mode='HTML',
                        reply_markup=join_keyboard
                    )
                except Exception as e:
                    logger.error(f"Send Document failed: {e}")

        if not sent_msg and url and "http" in url and "t.me" not in url:
             sent_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"🎬 <b>{title}</b>\n\n🔗 <b>Watch/Download:</b> {url}",
                parse_mode='HTML',
                reply_markup=join_keyboard
            )

        messages_to_delete = []
        if sent_msg:
            messages_to_delete.append(sent_msg.message_id)
        if warning_msg:
            messages_to_delete.append(warning_msg.message_id)

        if messages_to_delete:
            track_message_for_deletion(context, chat_id, messages_to_delete[0], 60) 
            if len(messages_to_delete) > 1:
                track_message_for_deletion(context, chat_id, messages_to_delete[1], 60)
        elif not sent_msg:
            await context.bot.send_message(chat_id=chat_id, text="❌ Error: File not found or Bot needs Admin rights in Source Channel.")

    except Exception as e:
        logger.error(f"Critical Error in send_movie: {e}")
        try: await context.bot.send_message(chat_id=chat_id, text="❌ System Error.")
        except: pass

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
                if conn: close_db_connection(conn)

        movies_found = []
        if exact_movie:
            movies_found = [exact_movie] # Exact match found, skip fuzzy search
        else:
            # Agar exact nahi mila to hi Fuzzy Search karein (Slower process)
            # Assuming get_movies_from_db is your existing function
            movies_found = await run_async(get_movies_from_db, query_text, limit=1)

        # 2. Result Handle karein
        if not movies_found:
            try: await status_msg.delete() 
            except: pass
            
            safe_query = quote(query_text)
            web_app_url = f"https://flimfybox-bot-yht0.onrender.com/webapp?req={safe_query}"
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🌐 Open Request Portal", web_app=WebAppInfo(url=web_app_url))]
            ])
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"😕 Sorry, <b>'{query_text}'</b> not found.\n\nस्पेलिंग चेक करने और Request भेजने के लिए नीचे क्लिक करें 👇",
                reply_markup=keyboard,
                parse_mode='HTML'
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

# ==================== CLEAN LOADING FUNCTION (FIXED) ====================
async def deliver_movie_on_start(update: Update, context: ContextTypes.DEFAULT_TYPE, movie_id: int):
    """
    Fetches and sends a movie with a clean 'Loading' animation.
    No technical details shown to the user.
    """
    chat_id = update.effective_chat.id
    
    # 1. Loading Effect
    status_msg = None
    try:
        status_msg = await context.bot.send_message(chat_id, "⏳ <b>Please wait...</b>", parse_mode='HTML')
        
        # Backup Auto-delete
        track_message_for_deletion(context, chat_id, status_msg.message_id, 60)
    except:
        pass

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            # User ko technical error mat dikhao, bas chupchap delete kar do
            if status_msg: 
                try: 
                    await status_msg.delete() 
                except: 
                    pass
            return

        cur = conn.cursor()
        cur.execute("SELECT title, url, file_id FROM movies WHERE id = %s", (movie_id,))
        movie_data = cur.fetchone()
        cur.close()
        close_db_connection(conn)

        # 2. Movie milne ke baad turant Loading Msg delete karo
        if status_msg:
            try: 
                await status_msg.delete()
            except: 
                pass

        if movie_data:
            title, url, file_id = movie_data
            # Movie bhejo
            await send_movie_to_user(update, context, movie_id, title, url, file_id)
        else:
            # Agar movie nahi mili
            fail_msg = await context.bot.send_message(chat_id, "❌ <b>Movie not found or deleted.</b>", parse_mode='HTML')
            track_message_for_deletion(context, chat_id, fail_msg.message_id, 10)

    except Exception as e:
        logger.error(f"Error in deliver_movie: {e}")
        if status_msg:
            try: 
                await status_msg.delete()
            except: 
                pass
        if movie_data:
            title, url, file_id = movie_data
            await send_movie_to_user(update, context, movie_id, title, url, file_id)
        else:
            await context.bot.send_message(
                chat_id=chat_id, 
                text="❌ Movie not found. It may have been removed from our database."
            )

    except Exception as e:
        logger.error(f"CRITICAL ERROR in deliver_movie: {e}", exc_info=True)
        error_msg = "❌ Failed to retrieve movie. Please try again or use search."
        if status_msg:
            try:
                await status_msg.edit_text(error_msg)
            except:
                pass
        else:
            await context.bot.send_message(chat_id=chat_id, text=error_msg)
            
    finally:
        if conn:
            try:
                close_db_connection(conn)
            except:
                pass

# Add this at the top level
from asyncio import Lock
from collections import defaultdict

user_processing_locks = defaultdict(Lock)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # ✅ FIX 1: Message ko safe tarike se nikalein (Button aur Text dono ke liye)
    message = update.effective_message 

    # === FSub Check (Smart Logic) ===
    force_check = True if context.args else False
    
    check = await is_user_member(context, user_id, force_fresh=force_check)
    
    if not check['is_member']:
        # Agar deep link (args) hain to unhe save kar lo
        if context.args:
            context.user_data['pending_start_args'] = context.args

        # ✅ FIX 2: send_message use karein (reply_text fail ho sakta hai button par)
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=get_join_message(check['channel'], check['group']),
            reply_markup=get_join_keyboard(),
            parse_mode='Markdown'
        )
        track_message_for_deletion(context, chat_id, msg.message_id, 120)
        return
    # ==================

    logger.info(f"START called by user {user_id} with args: {context.args}")

    # Purani states clear karein
    context.user_data.clear()
    if hasattr(context, 'conversation') and context.conversation:
        context.conversation = None

    # === DEEP LINK PROCESSING ===
    if context.args and len(context.args) > 0:
        payload = context.args[0]
        
        # Check lock (taaki user spam na kare)
        if user_processing_locks[user_id].locked():
            await context.bot.send_message(
                chat_id=chat_id, 
                text="⏳ Please wait! Your previous request is still processing..."
            )
            return

        async with user_processing_locks[user_id]:
            
            # 🔐 NAYA: ANTI-BOT TEMPORARY LINK SYSTEM (BURN ON READ)
            if payload.startswith("tmp_"):
                conn = get_db_connection()
                if not conn:
                    await context.bot.send_message(chat_id, "❌ System Error.")
                    return

                try:
                    cur = conn.cursor()
                    cur.execute("SELECT movie_id, created_at FROM temp_links WHERE token = %s", (payload,))
                    res = cur.fetchone()
                    
                    # Token TURANT delete kar do (Single Use)
                    cur.execute("DELETE FROM temp_links WHERE token = %s", (payload,))
                    conn.commit()
                    cur.close()
                    
                    if not res:
                        msg = await context.bot.send_message(chat_id, "❌ <b>Link Expired ya Invalid hai!</b>\nKripya app par jaakar dobara click karein.", parse_mode='HTML')
                        track_message_for_deletion(context, chat_id, msg.message_id, 15)
                        return
                    
                    movie_id, created_at = res
                    time_diff = (datetime.now() - created_at).total_seconds()
                    
                    if time_diff > 60:
                        msg = await context.bot.send_message(chat_id, "❌ <b>Link Expired!</b>\nYeh link sirf 60 seconds ke liye valid tha.", parse_mode='HTML')
                        track_message_for_deletion(context, chat_id, msg.message_id, 15)
                        return
                    
                    # Sab sahi hai, movie bhej do!
                    await deliver_movie_on_start(update, context, movie_id)
                    logger.info(f"✅ Secure token {payload} used successfully for movie {movie_id}")
                    return

                except Exception as e:
                    logger.error(f"Temp Link Error: {e}")
                    await context.bot.send_message(chat_id, "❌ Processing error.")
                    return
                finally:
                    close_db_connection(conn)

                    
            # --- CASE NAYA: DIRECT FILE CLICK FROM TEXT LINK ---
            if payload.startswith("file_"):
                try:
                    parts = payload.split('_')
                    movie_id = int(parts[1])
                    file_index = int(parts[2])
                    
                    status_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ <b>Fetching file...</b>", parse_mode='HTML')
                    
                    qualities = get_all_movie_qualities(movie_id)
                    if qualities and len(qualities) > file_index:
                        file_data = qualities[file_index]
                        url = file_data[1]
                        file_id = file_data[2]
                        
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                        res = cur.fetchone()
                        cur.close()
                        close_db_connection(conn)
                        title = res[0] if res else "Requested File"
                        
                        await send_movie_to_user(update, context, movie_id, title, url, file_id, send_warning=False)
                        
                        try: await status_msg.delete() 
                        except: pass
                    else:
                        await status_msg.edit_text("❌ File not found or expired.")
                    return
                except Exception as e:
                    logger.error(f"File click error: {e}")
                    await context.bot.send_message(chat_id=chat_id, text="❌ Invalid File Link")
                    return
                    
            # --- CASE 1: DIRECT MOVIE ID (movie_123) ---
            
            # --- CASE NAYA: DIRECT FILE CLICK FROM TEXT LINK ---
            if payload.startswith("file_"):
                try:
                    parts = payload.split('_')
                    movie_id = int(parts[1])
                    file_index = int(parts[2])
                    
                    status_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ **Fetching file...**", parse_mode='Markdown')
                    
                    # File ka data nikalo
                    qualities = get_all_movie_qualities(movie_id)
                    if qualities and len(qualities) > file_index:
                        file_data = qualities[file_index]
                        url = file_data[1]
                        file_id = file_data[2]
                        
                        # Movie ka naam nikalo
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                        res = cur.fetchone()
                        cur.close()
                        close_db_connection(conn)
                        title = res[0] if res else "Requested File"
                        
                        # Tera premium thumbnail wala function!
                        await send_movie_to_user(update, context, movie_id, title, url, file_id, send_warning=False)
                        
                        try: await status_msg.delete() 
                        except: pass
                    else:
                        await status_msg.edit_text("❌ File not found or expired.")
                    return
                except Exception as e:
                    logger.error(f"File click error: {e}")
                    await context.bot.send_message(chat_id=chat_id, text="❌ Invalid File Link")
                    return
            
            if payload.startswith("movie_"):
                try:
                    movie_id = int(payload.split('_')[1])
                    
                    # ✅ FIX 3: send_message use karein
                    status_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🎬 Deep link detected!\nMovie ID: {movie_id}\nFetching... Please wait ⏳"
                    )
                    
                    try:
                        await deliver_movie_on_start(update, context, movie_id)
                        
                        # Success hone par status msg delete karein
                        try: await status_msg.delete() 
                        except: pass
                        
                        logger.info(f"✅ Deep link SUCCESS for user {user_id}, movie {movie_id}")
                        
                    except Exception as e:
                        logger.error(f"❌ Deep link FAILED: {e}")
                        await status_msg.edit_text(f"❌ Error fetching movie: {e}")
                    
                    return # Movie mil gayi, Welcome msg mat dikhao

                except Exception as e:
                    logger.error(f"Invalid movie link: {e}")
                    await context.bot.send_message(chat_id=chat_id, text="❌ Invalid Link Format")
                    return

            # --- CASE 2: AUTO SEARCH (q_kalki) ---
            # ✅ RESTORED: Ye logic maine wapas add kar di hai
            elif payload.startswith("q_"):
                try:
                    query_text = payload[2:].replace("_", " ").strip()
                    
                    # ✅ FIX 4: send_message use karein
                    status_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🔎 Deep link search detected!\nQuery: '{query_text}'\nSearching... Please wait ⏳"
                    )
                    
                    try:
                        # Background search function call karein
                        await background_search_and_send(update, context, query_text, status_msg)
                        logger.info(f"✅ Deep link SEARCH SUCCESS for user {user_id}, query: {query_text}")
                        
                    except Exception as e:
                        logger.error(f"❌ Deep link SEARCH FAILED: {e}")
                        error_text = f"❌ Search failed for '{query_text}'.\nTry searching manually."
                        try: await status_msg.edit_text(error_text)
                        except: await context.bot.send_message(chat_id=chat_id, text=error_text)
                    
                    return # Search ho gaya, Welcome msg mat dikhao
                    
                except Exception as e:
                    logger.error(f"Deep link search error: {e}")
                    await context.bot.send_message(chat_id=chat_id, text="❌ Error processing search link.")
                    return

    # --- NORMAL WELCOME MESSAGE (WITH GIF & DYNAMIC GREETING) ---
    user_name = update.effective_user.first_name
    
    # 🌟 NAYA: Bot ka actual naam aur username nikalo
    bot_info = await context.bot.get_me()
    bot_name = bot_info.first_name  # Ye har bot ka apna alag naam uthayega!
    
    # 1. Dynamic Greeting Logic
    try:
        import pytz
        tz = pytz.timezone('Asia/Kolkata')
        hour = datetime.now(tz).hour
    except ImportError:
        hour = datetime.now().hour # Fallback agar pytz na ho
        
    if 5 <= hour < 12: greeting = "Good Morning ☀️"
    elif 12 <= hour < 17: greeting = "Good Afternoon 🌤️"
    elif 17 <= hour < 21: greeting = "Good Evening 🌆"
    else: greeting = "Good Night 🌙"

    # 2. Premium Caption (Dynamic Bot Name ke sath)
    caption_text = (
        f"<b>🚩 JAI SHRI RAM 🚩</b>\n\n"
        f"Hey <b>{user_name}</b>, {greeting}\n\n"
        f"🤖 Main hoon <b>{bot_name}</b>, the most powerful Auto Filter Bot with premium features.\n\n"
        f"<b>⚡️ My Capabilities:</b>\n"
        f"• Fastest auto-filtering\n"
        f"• 24/7 uptime\n"
        f"• Premium file processing\n\n"
        f"Tap the buttons below to know more! 👇"
    )

    # 3. Inline Buttons
    inline_buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔰 ADD ME TO YOUR GROUP 🔰", url=f"https://t.me/{bot_info.username}?startgroup=true")],
        [InlineKeyboardButton("HELP 📢", callback_data="start_help"), InlineKeyboardButton("ABOUT 📖", callback_data="start_about")],
        [InlineKeyboardButton("DONATION 💰", callback_data="start_donate")]
    ])

    try:
        # Web App button set karna
        web_app_url = "https://flimfybox-bot-yht0.onrender.com/webapp"
        await context.bot.set_chat_menu_button(
            chat_id=chat_id,
            menu_button=MenuButtonWebApp(text="🎬 Web Version", web_app=WebAppInfo(url=web_app_url))
        )
        
        # Bottom Keyboard ('Search', 'Request') lane ke liye ek chhota silent message
        # Bottom keyboard bhej kar turant delete kar do (chat clean rahegi)
        menu_msg = await context.bot.send_message(chat_id=chat_id, text="🔄 Loading Menu...", reply_markup=get_main_keyboard())
        try:
            await menu_msg.delete()
        except: 
            pass

        # GIF from Dump Channel + Naya Caption & Buttons
        msg = await context.bot.copy_message(
            chat_id=chat_id,
            from_chat_id=int(DUMP_CHANNEL_ID),
            message_id=6057, # Tumhari GIF ki Message ID
            caption=caption_text,
            parse_mode='HTML',
            reply_markup=inline_buttons
        )
        track_message_for_deletion(context, chat_id, msg.message_id, delay=300)
        
    except Exception as e:
        logger.error(f"Start Menu Error: {e}")
        # Agar copy_message fail ho (bot dump channel me admin na ho)
        msg = await context.bot.send_message(chat_id=chat_id, text=caption_text, parse_mode='HTML', reply_markup=inline_buttons)
        track_message_for_deletion(context, chat_id, msg.message_id, delay=300)
        
    return
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu options"""
    try:
        query = update.message.text

        if query == '🔍 Search Movies':
            msg = await update.message.reply_text("Great! Tell me the name of the movie you want to search for.")
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return SEARCHING

        elif query == '🙋 Request Movie':
            msg = await update.message.reply_text("Okay, you've chosen to request a new movie. Please tell me the name of the movie you want me to add.")
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
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
                if conn: close_db_connection(conn)

            return MAIN_MENU

        elif query == '❓ Help':
            help_text = """
🤖 How to use FlimfyBox Bot:

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
        # Agar ye button click se aya hai (cancel/back)
        if update.callback_query:
            query = update.callback_query
            await query.answer()
            # Yahan hum kuch return nahi kar rahe, bas message bhej rahe hain
            return

        # Agar message text nahi hai
        if not update.message or not update.message.text:
            return 

        query = update.message.text.strip()
        
        # Safety check
        if query in ['🔍 Search Movies', '📊 My Stats', '❓ Help']:
             return await main_menu_or_search(update, context)

        # 👇 NAYA FIX: Search query se Season/Episode tags hata do taaki main show mil jaye 👇
        import re
        clean_query = re.sub(r'(?i)\b(s\d{1,2}|season\s*\d+|ep\s?\d+|e\d{1,2})\b.*', '', query).strip()
        search_term = clean_query if (clean_query and len(clean_query) > 1) else query

        # 1. Search DB (Ab bot 'The Great' dhoondhega, 'The Great S03' nahi)
        movies = await run_async(get_movies_from_db, search_term, limit=10)
        
        # 2. Not Found
        if not movies:
            if SEARCH_ERROR_GIFS:
                try:
                    gif = random.choice(SEARCH_ERROR_GIFS)
                    msg_gif = await update.message.reply_animation(animation=gif)
                    track_message_for_deletion(context, update.effective_chat.id, msg_gif.message_id, 60)
                except:
                    pass

            not_found_text = (
                "माफ़ करें, मुझे कोई मिलती-जुलती फ़िल्म नहीं मिली\n\n"
                "<b><a href='https://www.google.com/'>𝗚𝗼𝗼𝗴𝗹𝗲</a></b> ☜ सर्च करें..!!\n\n"
                "मूवी की स्पेलिंग गूगल पर सर्च करके, कॉपी करे, उसके बाद यहां टाइप करें।✔️\n\n"
                "बस मूवी का नाम + वर्ष:::: लिखें, उसके आगे पीछे कुछ भी ना लिखे..।♻️\n\n"
                "✐ᝰ𝗘𝘅𝗮𝗺𝗽𝗹𝗲\n\n"
                "सही है.!‼️    \n"
                "─────────────────────\n"
                "𝑲𝒈𝒇 𝟐✔️ | 𝑲𝒈𝒇 𝟐 𝑴𝒐𝒗𝒊𝒆 ❌\n"
                "─────────────────────\n"
                "𝑨𝒔𝒖𝒓 𝑺𝟎𝟏 𝑬𝟎𝟑✔️ | 𝑨𝒔𝒖𝒓 𝑺𝒆𝒂𝒔𝒐𝒏𝟑❌\n"
                "─────────────────────\n\n"
                "👇 <b>सही स्पेलिंग ढूँढने और Request करने के लिए नीचे क्लिक करें:</b>"
            )

            # 🌐 NAYA JUGAD: Web App URL jisme user ki galat spelling (query) attach hogi
            safe_query = quote(query)
            web_app_url = f"https://flimfybox-bot-yht0.onrender.com/webapp?req={safe_query}"

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🌐 Open Request Portal", web_app=WebAppInfo(url=web_app_url))]
            ])
            
            msg = await update.message.reply_text(
                text=not_found_text,
                reply_markup=keyboard,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            # Auto Delete Not Found Msg
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return # <--- YAHAN SE MAIN_MENU HATA DIYA HAI

        # 3. Found
        context.user_data['search_results'] = movies
        context.user_data['search_query'] = query

        keyboard = create_movie_selection_keyboard(movies, page=0)
        
        msg = await update.message.reply_text(
            f"🎬 **Found {len(movies)} results for '{query}'**\n\n"
            "👇 Select your movie below:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
        return # <--- YAHAN SE BHI MAIN_MENU HATA DIYA HAI

    except Exception as e:
        logger.error(f"Error in search_movies: {e}")
        # await update.message.reply_text("An error occurred during search.") <--- ERROR MSG HATA DIYA TAKI USER DISTURB NA HO
        return

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
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
            return REQUESTING

        intent = await analyze_intent(user_message)
        if not intent["is_request"]:
            msg = await update.message.reply_text("यह एक मूवी/सीरीज़ का नाम नहीं लग रहा है। कृपया सही नाम भेजें।")
            track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
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
                track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)
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
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat.id
    data = query.data

    # ✅ NAYA: Video wala Pages Button Popup
    if data == "ignore":
        await query.answer("THIS IS PAGES BUTTON 🔴", show_alert=False)
        return

    if data.startswith("fl_") or data.startswith("v_"):
        parts = query.data.split('_')
        view_type = parts[1] if parts[0] == "v" else "main" 
        
        # ✅ NAYA: Video wale cool popups!
        if view_type in ["lang", "qual", "seas"]:
             await query.answer("Share & Support Us ❤️", show_alert=False)

    # ==================== NAYA: SINGLE FILE SEND ====================
    if data.startswith("send_single_"):
        # Telegram File IDs mein underscores (_) ho sakte hain, isliye safai se nikalenge
        parts = data.split('_')
        movie_id = int(parts[-1]) # Aakhri hissa hamesha movie_id hota hai
        file_id_to_send = data.replace("send_single_", "").replace(f"_{movie_id}", "")
        
        # Memory se movie ka naam nikal lo
        movie_data = context.user_data.get('selected_movie_data')
        title = movie_data['title'] if movie_data else "Requested Movie"

        try:
            # 🚀 NAYA: Ab simple text ki jagah tera Premium function use hoga!
            await send_movie_to_user(
                update=update, 
                context=context, 
                movie_id=movie_id, 
                title=title, 
                url=None, 
                file_id=file_id_to_send, 
                send_warning=False # Har single file ke sath auto-delete warning baar-baar na bheje
            )
            await query.answer("✅ File Sent!", show_alert=False)
        except Exception as e:
            await query.answer("❌ Error sending file.", show_alert=True)
            logger.error(f"Single file send error: {e}")
        return

    elif data.startswith("back_to_seasons_"):
        movie_id = int(data.split('_')[3])
        context.user_data.pop('active_filter', None)
        context.user_data.pop('selected_season', None)
        movie_data = context.user_data.get('selected_movie_data')
        if not movie_data:
            await query.answer("❌ Session expired.", show_alert=True)
            return
        title = movie_data['title']
        qualities = movie_data['qualities']
        seasons = set()
        for f in qualities:
            extra = f[5] if len(f) > 5 else ""
            if extra:
                s_name = extract_season_name(extra)
                if s_name != "Extra Files": seasons.add(s_name)
        
        keyboard = []
        keyboard.append([InlineKeyboardButton("🎬 Movie", callback_data=f"showseason_{movie_id}_Extra Files")])
        for s in sorted(list(seasons)):
            keyboard.append([InlineKeyboardButton(f"📁 {s}", callback_data=f"showseason_{movie_id}_{s}")])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_selection")])
        
        await query.edit_message_text(f"📺 **{title}**\n\n👇 **Select Option:**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    
    
    # === START MENU BUTTONS LOGIC ===
    if data.startswith("start_"):
        await query.answer()

        if data == "start_help":
            text = "<b>🛠 HELP MENU</b>\n\nMujhe apne group me add karke Admin bana do. Main automatically files filter karna shuru kar dunga!"
            back_btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="start_back")]])
            # Purani GIF delete karke naya message bhejenge (sabse safe tareeka)
            try: await query.message.delete()
            except: pass
            msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', reply_markup=back_btn)
            track_message_for_deletion(context, chat_id, msg.message_id, 120)
            return

        elif data == "start_about":
            text = f"<b>📖 ABOUT ME</b>\n\n• <b>Developer:</b> @{ADMIN_USERNAME}\n• <b>Language:</b> Python 3\n• <b>Library:</b> python-telegram-bot"
            back_btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="start_back")]])
            try: await query.message.delete()
            except: pass
            msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', reply_markup=back_btn)
            track_message_for_deletion(context, chat_id, msg.message_id, 120)
            return
            
        elif data == "start_donate":
            # Yahan se tumhara purana start_donate wala code shuru hoga...
            await query.answer()
            user = update.effective_user
            amount = 10  # Tumhara VIP amount
            upi_id = os.environ.get("UPI_ID", "default_id@ybl")
            
            try:
                import qrcode
                from io import BytesIO
                from urllib.parse import quote
                
                # QR Code Generate karna
                note = f"TG-{user.id}"
                upi_url = f"upi://pay?pa={upi_id}&pn=VIP+Subscription&am={amount}&tn={note}&cu=INR"
                
                qr = qrcode.QRCode(version=1, box_size=10, border=4)
                qr.add_data(upi_url)
                qr.make(fit=True)
                img = qr.make_image(fill_color="black", back_color="white")
                bio = BytesIO()
                img.save(bio, format='PNG')
                bio.seek(0)
                
                text = (
                    f"💎 <b>VIP DONATION - ₹{amount}</b>\n\n"
                    f"📱 <b>Scan QR Code</b> from any UPI app (GPay/PhonePe/Paytm)\n"
                    f"💳 <b>UPI ID:</b> <code>{upi_id}</code>\n\n"
                    f"✅ Payment ke baad:\n"
                    f"1️⃣ <b>Screenshot</b> bhejo yahan\n"
                    f"2️⃣ Phir <b>UTR Number</b> type karke bhejo\n\n"
                    f"📸 <i>Intezaar hai aapke screenshot ka...</i>"
                )
                
                # Bot ko batana ki user ab screenshot bhejega
                context.user_data['payment_step'] = 'screenshot'
                
                # Purana menu delete karke QR bhejna
                await query.message.delete()
                await context.bot.send_photo(
                    chat_id=query.message.chat_id,
                    photo=bio,
                    caption=text,
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="start_back")]])
                )
            except Exception as e:
                # Agar qrcode install nahi hai toh normal text bhejega
                text = f"<b>💰 DONATION</b>\n\nAgar aapko mera kaam pasand aaya, toh aap UPI pe support kar sakte hain: <code>{upi_id}</code>"
                back_btn = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 BACK", callback_data="start_back")]])
                await query.edit_message_caption(caption=text, parse_mode='HTML', reply_markup=back_btn)
            return

    if data == "start_back":
        await query.answer()
        chat_id = query.message.chat_id
        
        # Pehle wala message delete karo (chahe wo Help ho, About ho ya QR code ho)
        try:
            await query.message.delete()
        except: pass

        # Wapas Start Menu Banane ka logic
        user_name = update.effective_user.first_name
        bot_info = await context.bot.get_me()
        bot_name = bot_info.first_name
        
        try:
            import pytz
            tz = pytz.timezone('Asia/Kolkata')
            hour = datetime.now(tz).hour
        except ImportError:
            hour = datetime.now().hour
            
        if 5 <= hour < 12: greeting = "Good Morning ☀️"
        elif 12 <= hour < 17: greeting = "Good Afternoon 🌤️"
        elif 17 <= hour < 21: greeting = "Good Evening 🌆"
        else: greeting = "Good Night 🌙"

        caption_text = (
            f"<b>🚩 JAI SHRI RAM 🚩</b>\n\n"
            f"Hey <b>{user_name}</b>, {greeting}\n\n"
            f"🤖 Main hoon <b>{bot_name}</b>, the most powerful Auto Filter Bot with premium features.\n\n"
            f"<b>⚡️ My Capabilities:</b>\n"
            f"• Fastest auto-filtering\n"
            f"• 24/7 uptime\n"
            f"• Premium file processing\n\n"
            f"Tap the buttons below to know more! 👇"
        )

        inline_buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔰 ADD ME TO YOUR GROUP 🔰", url=f"https://t.me/{bot_info.username}?startgroup=true")],
            [InlineKeyboardButton("HELP 📢", callback_data="start_help"), InlineKeyboardButton("ABOUT 📖", callback_data="start_about")],
            [InlineKeyboardButton("DONATION 💰", callback_data="start_donate")]
        ])

        # Original GIF wapas send karo
        await context.bot.copy_message(
            chat_id=chat_id,
            from_chat_id=int(os.environ.get('DUMP_CHANNEL_ID', '-1002683355160')),
            message_id=6057, 
            caption=caption_text,
            parse_mode='HTML',
            reply_markup=inline_buttons
        )
        return
        
    # === ADMIN REQUEST BUTTONS (Add/Not Found) ===
    if data.startswith("reqA_") or data.startswith("reqN_"):
        await query.answer("🔄 Sending message to user...", show_alert=False)
        parts = data.split('_', 2)
        action = parts[0]  # Yahan '_' hat jata hai, sirf 'reqA' ya 'reqN' bachta hai
        target_user_id = int(parts[1])
        movie_title = parts[2]

        # User ka naam DB se nikalo taaki message personal lage
        conn = get_db_connection()
        first_name = "User"
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT first_name FROM user_requests WHERE user_id = %s LIMIT 1", (target_user_id,))
                res = cur.fetchone()
                if res: first_name = res[0] or "User"
            except: pass
            finally: close_db_connection(conn)

        # ✅ FIXED: "reqA_" ki jagah "reqA" use karna hai
        if action == "reqA":
            user_msg = (
                f"🎉 <b>Good News!</b> 👋\n\n"
                f"Hello <b>{first_name}!</b> आपकी Requested Movie अब उपलब्ध है।\n\n"
                f"🎬 File: <b>{movie_title}</b>\n\n"
                f"इसे पाने के लिए अभी बॉट में मूवी का नाम टाइप करें और एन्जॉय करें! 😊\n\n"
                f"━━━━━━━━━━━━━━━━━━━\nRegards, <b>@ownermahi</b>"
            )
            btn_status = "✅ User Notified: Added"
        else:
            user_msg = (
                f"😔 <b>Update!</b> 👋\n\n"
                f"Hello <b>{first_name}!</b> आपकी Requested File (<b>{movie_title}</b>) अभी हमें कहीं नहीं मिल पाई है।\n\n"
                f"जैसे ही यह अवेलेबल होगी, हम आपको जरूर बताएंगे।\n\n"
                f"━━━━━━━━━━━━━━━━━━━\nRegards, <b>@ownermahi</b>"
            )
            btn_status = "❌ User Notified: Not Found"

        # ✅ FIXED: Yahan user ko message send karna hai, taaki request block sahi se band ho jaye!
        success = await send_multi_bot_message(target_user_id, user_msg)
        
        if success:
            # Button hata do aur Admin ko updated status dikhao
            await query.edit_message_text(f"{query.message.text}\n\n{btn_status} 📩", parse_mode='HTML')
        else:
            await query.answer("❌ Failed! User ne sabhi bots block kar diye hain.", show_alert=True)
            
        return # Yahan is block ka kaam khatam!

    

    # =======================================================
    # 🖼️ NEW: ASK POSTER LOGIC (Semi-Auto Post)
    # =======================================================
    if data.startswith("askposter_"):
        if update.effective_user.id != ADMIN_USER_ID:
            await query.answer("❌ Admin only!", show_alert=True)
            return

        movie_id = int(data.split("_")[1])
        
        # Bot ko yaad dilao ki ab agli photo is movie ke liye aayegi
        context.user_data['waiting_for_poster'] = movie_id
        
        await query.answer()
        await query.message.reply_text(
            "🖼️ **Please send the Landscape Poster (Image) for this movie now.**\n\n"
            "*(सिर्फ़ फोटो भेजें, कोई कैप्शन लिखने की ज़रूरत नहीं है)*",
            parse_mode='Markdown'
        )
        return
        
    # Iske niche aapke baki ke callback conditions waise hi rahenge (autopost_, cancel_genre aadi...)
    
    # =======================================================
    # 🤖 NEW: AUTO POST LOGIC (Premium Cinematic & Random Styles)
    # =======================================================
    if query.data.startswith("autopost_"):
        await query.answer("⏳ Premium Post Generate ho rahi hai...")
        movie_id = int(query.data.split("_")[1])
        
        # --- 1. DATABASE SE DATA NIKALNA ---
        conn = get_db_connection()
        cur = conn.cursor()
        
        # क्वालिटी निकालें
        cur.execute("SELECT quality FROM movie_files WHERE movie_id = %s", (movie_id,))
        rows = cur.fetchall()
        
        # मूवी की डिटेल्स निकालें (🚀 NAYA: Ab poster_url bhi nikalega)
        cur.execute("SELECT title, genre, language, poster_url FROM movies WHERE id = %s", (movie_id,))
        m_data = cur.fetchone()
        cur.close()
        
        # Connection close (Tera custom function)
        try: close_db_connection(conn) 
        except: db_pool.putconn(conn) 

        if not m_data:
            await query.edit_message_text("❌ Error: Movie DB mein nahi mili!")
            return

        # क्वालिटी फॉर्मेटिंग
        res_list = []
        for r in rows:
            if r and r[0]:
                match = re.search(r'(\d{3,4}p)', str(r[0]))
                if match: res_list.append(match.group(1))
        res_list = sorted(list(set(res_list)), key=lambda x: int(x.replace('p','')), reverse=True)
        dynamic_res = " | ".join(res_list) if res_list else "1080p | 720p | 480p"

        m_title = m_data[0] if m_data[0] else "Unknown Movie"
        m_genre = m_data[1] if m_data[1] else "Action, Drama"
        m_lang = m_data[2] if m_data[2] else "Hindi + English"
        m_poster = m_data[3] if len(m_data) > 3 and m_data[3] else None

        # --- 2. POSTER PROCESSING (Cinematic Square Effect) ---
        # 🚀 NAYA FIX: Pehle TMDB ka link uthao. Agar TMDB poster nahi hai, tabhi Thumbnail use karo.
        raw_photo = m_poster if (m_poster and m_poster != 'N/A' and m_poster.startswith('http')) else None
        
        if not raw_photo:
            thumb_id = context.bot_data.get(f"auto_thumb_{movie_id}")
            if isinstance(thumb_id, str) and not thumb_id.startswith("http"):
                try:
                    tg_file = await context.bot.get_file(thumb_id)
                    raw_photo = bytes(await tg_file.download_as_bytearray())
                except Exception as e:
                    logger.error(f"Autopost thumb download error: {e}")
                    raw_photo = None
        
        # Yahan hum naya blurred poster banayenge
        if raw_photo:
            photo_to_send = await make_landscape_poster(raw_photo)
        else:
            # Default poster agar kuch na mile
            photo_to_send = "https://i.imgur.com/6XK4F6K.png"

        # --- 3. 🎲 RANDOM PREMIUM STYLES 🎲 ---
        safe_title = m_title.replace('<', '').replace('>', '')
        unicode_title = get_safe_font(safe_title)
        
        # 👈 Ab sirf 2 styles bache hain (Box wala hata diya)
        style_choice = random.choice([1, 2])

        if style_choice == 1:
            channel_caption = (
                f"🎬 <b>{safe_title}</b>\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"✨ <b>Genre:</b> {m_genre}\n"
                f"🔊 <b>Language:</b> {m_lang}\n"
                f"💿 <b>Quality:</b> V2 HQ-HDTC {dynamic_res}\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
                f"👇 <b>Download Below</b> 👇"
            )
        else:
            channel_caption = (
                f"🔥 <b>{unicode_title}</b>\n"
                f" ├ ✨ Genre: {m_genre}\n"
                f" ├ 🔊 Language: {m_lang}\n"
                f" └ 💿 Quality: V2 HQ-HDTC {dynamic_res}\n"
                f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
                f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
                f"👇 <b>Download Below</b> 👇"
            )

        # --- 4. SECURE LINK & BUTTONS ---
        secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"
        channel_link = os.environ.get('FILMFYBOX_CHANNEL_URL', 'https://t.me/your_channel')

        post_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Download Now", url=secure_url), InlineKeyboardButton("Download Now", url=secure_url)],
            [InlineKeyboardButton("⚡ Download Now", url=secure_url)],
            [InlineKeyboardButton("📢 Join Channel", url=channel_link)]
        ])

        # --- 5. BROADCASTING TO CHANNELS ---
        channels_str = os.environ.get('BROADCAST_CHANNELS', '')
        target_channels = [ch.strip() for ch in channels_str.split(',') if ch.strip()]

        if not target_channels:
            await query.edit_message_text(f"{query.message.text}\n\n❌ Error: No BROADCAST_CHANNELS found in env.")
            return

        sent_count = 0
        last_error = ""
        telegram_photo_id = None 

        for chat_id_str in target_channels:
            try:
                chat_id = int(chat_id_str)
                
                # Fast posting ke liye Telegram File ID use karo
                if telegram_photo_id:
                    sent_msg = await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=telegram_photo_id,
                        caption=channel_caption,
                        parse_mode='HTML',
                        reply_markup=post_keyboard
                    )
                else:
                    # BytesIO pointer ko start par reset karna zaroori hai
                    if hasattr(photo_to_send, 'seek'):
                        photo_to_send.seek(0)
                        
                    sent_msg = await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=photo_to_send,
                        caption=channel_caption,
                        parse_mode='HTML',
                        reply_markup=post_keyboard
                    )
                    # Agle channel ke liye File ID save kar lo
                    if sent_msg and sent_msg.photo:
                        telegram_photo_id = sent_msg.photo[-1].file_id

                # DB mein save karne wala tera purana logic
                if sent_msg:
                    try:
                        save_post_to_db(movie_id, chat_id, sent_msg.message_id, "bot3", channel_caption, telegram_photo_id, "photo", post_keyboard.to_dict(), None, "movies")
                    except Exception as db_err:
                        logger.error(f"Save to DB Error: {db_err}")
                        
                sent_count += 1
                await asyncio.sleep(1) # Flood se bachne ke liye chhota delay
                
            except Exception as e:
                logger.error(f"Auto-post failed for {chat_id_str}: {e}")
                last_error = str(e)

        # --- 6. SUCCESS MESSAGE ---
        result_msg = f"✅ <b>Auto-Posted (VIP Square Poster) to {sent_count} channels!</b>"
        if sent_count == 0 and last_error: 
            result_msg += f"\n❌ <b>Failed Reason:</b> <code>{last_error}</code>"

        await query.edit_message_text(result_msg, parse_mode='HTML')
        
        # Memory saaf karo
        context.bot_data.pop(f"auto_thumb_{movie_id}", None)
        return
    
    # === NEW: GENRE CALLBACK HANDLER ===
    if data.startswith(("genre_", "cancel_genre")):
        await handle_genre_selection(update, context)
        return
    
    # === SEND ALL FILES LOGIC (HIGHLY OPTIMIZED) ===
    if query.data.startswith("sendall_"):
        movie_id = int(query.data.split("_")[1])
        chat_id = update.effective_chat.id

        # ✅ FAST FETCH: Ek hi bar mein sab nikal lo
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT title, genre, year, language FROM movies WHERE id = %s", (movie_id,))
        res = cur.fetchone()
        cur.close()
        close_db_connection(conn)

        if res:
            title, db_genre, db_year, db_lang = res
            pre_fetched_meta = {'genre': db_genre, 'year': db_year, 'language': db_lang}
        else:
            title = "Movie"
            pre_fetched_meta = {}

        qualities = get_all_movie_qualities(movie_id)
        
        # NAYA: Filter apply karo taaki Send All sirf filter ki hui files bheje
        active_filter = context.user_data.get('active_filter')
        if active_filter:
            f_type = active_filter['type']
            f_val = active_filter['value'].lower()
            temp_list = []
            for q in qualities:
                q_name = str(q[0]).lower()
                lang_name = str(q[4]).lower() if len(q) > 4 else ""
                extra = str(q[5]).lower() if len(q) > 5 else ""
                if f_type == "lang" and f_val in lang_name: temp_list.append(q)
                elif f_type == "qual" and f_val in q_name: temp_list.append(q)
                elif f_type == "seas" and f_val in extract_season_name(extra).lower(): temp_list.append(q)
            qualities = temp_list

        if not qualities:
            await query.answer("❌ No files found!", show_alert=True)
            return

        await query.answer(f"🚀 Sending {len(qualities)} files...")
        status_msg = await query.message.reply_text(f"🚀 **Sending {len(qualities)} files...**", parse_mode='Markdown')
        
        # 1. LOOP: FILES BHEJO
        count = 0
        # 👇 NAYA BULLETPROOF CODE 👇
        for file_data in qualities:
            url = file_data[1]
            file_id = file_data[2]
            
            try:
                await send_movie_to_user(
                    update, context, movie_id, title, url, file_id, 
                    send_warning=False,
                    pre_fetched_meta=pre_fetched_meta
                )
                await asyncio.sleep(1.2) 
                count += 1
            except Exception as e:
                logger.error(f"Send All Error: {e}")

        # 2. END: WARNING FILE BHEJO (Ek hi baar)
        try:
            warning_msg = await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=-1002683355160, # Apka Channel ID
                message_id=1773              # Warning File Message ID
            )
            track_message_for_deletion(context, chat_id, warning_msg.message_id, 60)
        except Exception as e:
            logger.error(f"Failed to send final warning file: {e}")

        await status_msg.edit_text(f"✅ **Sent {count} Files!**", parse_mode='Markdown')
        track_message_for_deletion(context, chat_id, status_msg.message_id, 30)
        return
    
    # === NEW: SCAN INFO POPUP ===
    if data.startswith("scan_"):
        m_id = int(data.split("_")[1])
        
        # Database se details nikalo
        conn = get_db_connection()
        cur = conn.cursor()
        # Maan lo tumhare DB mein 'language' aur 'subtitle' column hain, ya tum file name se guess karoge
        cur.execute("SELECT title, year, genre FROM movies WHERE id = %s", (m_id,))
        res = cur.fetchone()
        cur.close()
        close_db_connection(conn)

        if res:
            title, year, genre = res
            # Ye wo text hai jo Popup mein dikhega
            popup_text = (
                f"📂 File Info:\n"
                f"🎬 Movie: {title}\n"
                f"📅 Year: {year}\n"
                f"🎭 Genre: {genre}\n"
                f"🔊 Audio: Hindi, English (Dual)\n" # Ise DB se dynamic bana sakte ho
                f"📝 Subs: English, Hindi"
            )
            # show_alert=True ka matlab hai Screen par bada popup aayega!
            await query.answer(popup_text, show_alert=True)
        else:
            await query.answer("❌ Info not found", show_alert=True)
        return
    
    # ===================================
    
    # 👇👇👇 YE NAYA CODE ADD KARO 👇👇👇
    if query.data.startswith("clearfiles_"):
        if update.effective_user.id != ADMIN_USER_ID:
            await query.answer("❌ Sirf Admin ke liye!", show_alert=True)
            return

        movie_id = int(query.data.split("_")[1])
        
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM movie_files WHERE movie_id = %s", (movie_id,))
                deleted_count = cur.rowcount # Kitni delete hui
                conn.commit()
                cur.close()
                close_db_connection(conn)
                
                # 👇 NAYA: BATCH_SESSION ke counter ko bhi zero (0) kar do
                if BATCH_SESSION.get('movie_id') == movie_id:
                    BATCH_SESSION['file_count'] = 0
                
                await query.answer(f"✅ {deleted_count} purani files delete ho gayi!", show_alert=True)
                await query.edit_message_text(
                    f"🗑️ **Deleted {deleted_count} old files.**\n\n"
                    f"✅ **Clean Slate!** Ab nayi files upload karo.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"Delete Error: {e}")
                await query.answer("❌ Error deleting files", show_alert=True)
        return
    
    
    # === CANCEL BATCH LOGIC ===
    if query.data == "cancel_batch":
        if update.effective_user.id != ADMIN_USER_ID:
            await query.answer("❌ Sirf Admin ke liye!", show_alert=True)
            return

        # Session ko off kar do taaki aur files save na hon
        BATCH_SESSION.update({
            'active': False, 'movie_id': None, 'movie_title': None,
            'file_count': 0, 'admin_id': None, 'year': '', 'category': ''
        })

        await query.answer("🛑 Batch Stopped!", show_alert=True)
        await query.edit_message_text(
            "❌ **Batch Stopped & Cancelled.**\n\n"
            "Aap chaho to manually sahi naam dekar naya batch start kar sakte ho:\n"
            "`/batch Sahi Movie Name`",
            parse_mode='Markdown'
        )
        return
        
    # === CANCEL 18+ BATCH LOGIC ===
    if query.data == "cancel_batch18":
        if update.effective_user.id != ADMIN_USER_ID:
            await query.answer("❌ Sirf Admin ke liye!", show_alert=True)
            return

        BATCH_18_SESSION.update({
            'active': False, 'movie_id': None, 'movie_title': None,
            'file_count': 0, 'admin_id': None, 'year': '', 'category': ''
        })

        await query.answer("🛑 18+ Batch Stopped!", show_alert=True)
        await query.edit_message_text(
            "❌ **18+ Batch Stopped & Cancelled.**\n\n"
            "Aap chaho to manually naya batch start kar sakte ho.",
            parse_mode='Markdown'
        )
        return
    
    # === 1. VERIFY BUTTON LOGIC (UPDATED) ===
    if data == "verify":
        await query.answer("🔍 Checking membership...", show_alert=False) # Alert False rakha taki user disturb na ho
        
        # Force Fresh Check
        check = await is_user_member(context, user_id, force_fresh=True)
        
        if check['is_member']:
            # ✅ SCENARIO 1: Agar koi Deep Link pending tha (e.g. start=movie_123)
            if 'pending_start_args' in context.user_data:
                saved_args = context.user_data.pop('pending_start_args')
                
                # "Verified" wala msg delete kar do taaki clean lage
                try: await query.message.delete()
                except: pass
                
                # Start function ko manually call karo saved args ke saath
                context.args = saved_args
                await start(update, context)
                return

            # ✅ SCENARIO 2: Agar koi Text Search pending tha (e.g. "Kalki")
            elif 'pending_search_query' in context.user_data:
                saved_query = context.user_data.pop('pending_search_query')
                
                # "Verified" wala msg delete kar do
                try: await query.message.delete()
                except: pass
                
                # Search Movies ko call karne ke liye update object ko modify karein
                # Hum current query message ko use karenge par text replace kar denge
                update.message = query.message 
                update.message.text = saved_query
                
                # User ko feedback do ki search shuru ho gaya
                await search_movies(update, context)
                return

            # ✅ SCENARIO 3: Agar koi pending request nahi thi (Normal Verify)
            else:
                await query.edit_message_text(
                    "✅ **Verified Successfully!**\n\n"
                    "You can now use the bot! 🎬\n"
                    "Click /start or search any movie.",
                    parse_mode='Markdown'
                )
                track_message_for_deletion(context, chat_id, query.message.message_id, 10)
        else:
            # Agar abhi bhi join nahi kiya
            try:
                await query.edit_message_text(
                    get_join_message(check['channel'], check['group']),
                    reply_markup=get_join_keyboard(),
                    parse_mode='Markdown'
                )
            except telegram.error.BadRequest:
                await query.answer("❌ You haven't joined yet!", show_alert=True)
        return
    # ==============================

    # === 2. OTHER BUTTONS PROTECTION (Optional but Recommended) ===
    # Agar user 'download', 'movie', 'request' dabaye to bhi check karo
    if data.startswith(("movie_", "download_", "quality_", "request_")):
        check = await is_user_member(context, user_id) # Cache use karega
        if not check['is_member']:
            await query.answer("❌ Please join channels first!", show_alert=True)
            await query.edit_message_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            return
    # ==============================================================

    try:
        # ==================== MOVIE SELECTION ====================
        if query.data.startswith("movie_"):
            movie_id = int(query.data.replace("movie_", ""))

            conn = get_db_connection()
            cur = conn.cursor()
            # 🚀 FIX: Yahan 'category' bhi nikal rahe hain taaki pata chale Web Series hai ya nahi
            cur.execute("SELECT id, title, category FROM movies WHERE id = %s", (movie_id,))
            movie = cur.fetchone()
            cur.close()
            close_db_connection(conn)

            if not movie:
                await query.edit_message_text("❌ Movie not found in database.")
                return

            movie_id, title, category = movie
            qualities = get_all_movie_qualities(movie_id)

            if not qualities:
                await query.answer("❌ No files found!", show_alert=True)
                return

            # Data context mein save karo aage ke liye
            context.user_data['selected_movie_data'] = {
                'id': movie_id,
                'title': title,
                'category': category,
                'qualities': qualities
            }


            # Agar normal Movie hai (ya Series ka season logic fail hua), toh direct qualities dikhao
            bot_username = context.bot.username
            file_list_text = f"📁 <b>{title}</b>\n\n👇 <b>Your Requested Files Are Here</b>\n\n"
            
            for idx, file_data in enumerate(qualities[:10], start=1):
                quality = file_data[0]
                file_size = file_data[3] if len(file_data) > 3 else "Unknown Size"
                extra_info = file_data[5] if len(file_data) > 5 else ""
                
                ep_tag = f"[{extra_info}] " if extra_info else ""
                # ✅ CLEAN HTML LINK: Naruto bot jaisa neela text!
                file_list_text += f"<b>{idx}.</b> <b><a href='https://t.me/{bot_username}?start=file_{movie_id}_{idx-1}'>{file_size} | {title} {ep_tag}{quality}</a></b>\n\n"

            selection_text = file_list_text
            
            # Pagination calculate karo pehli baar ke liye
            limit = 10
            total_pages = (len(qualities) + limit - 1) // limit if qualities else 1
            
            # CLEAR PREVIOUS FILTERS
            context.user_data['active_filter'] = None
            
            # ✅ NAYA: Function ko call karo taaki 1, 2, 3 wale buttons aa jayein!
            current_files = qualities[:limit]
            keyboard_markup = create_quality_selection_keyboard(
                movie_id=movie_id, 
                view="main", 
                page=1, 
                total_pages=total_pages, 
                current_files=current_files
            )
            
            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard_markup,
                parse_mode='HTML'
            )
            return

            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
            
            track_message_for_deletion(context, update.effective_chat.id, query.message.message_id, 60)


        # ==================== SEASON SELECTION (NEW) ====================
        elif query.data.startswith("showseason_"):
            parts = query.data.split('_', 2)
            movie_id = int(parts[1])
            selected_season = parts[2]
            
            # Context me season save karo
            context.user_data['selected_season'] = selected_season
            context.user_data['active_filter'] = None
            
            # 🚀 FIX: `query.data` read-only hai, usko badalna allowed nahi hai. 
            # Iski jagah sidha update.callback_query_data object modify nahi karke
            # manually call karte hain ya redirect code yahi execute karte hain.
            
            # Naye UI logic ki taraf redirect
            # Hum data ko sidha bhej rahe hain taaki button_callback khud ise handle kare, bina modify kiye
            class FakeQuery:
                def __init__(self, from_user, message, data):
                    self.from_user = from_user
                    self.message = message
                    self.data = data
                    
                async def answer(self, *args, **kwargs):
                    pass # Silent ignore
                    
                async def edit_message_text(self, *args, **kwargs):
                    return await query.edit_message_text(*args, **kwargs)

            # Ek naya fake query object banaya taki read-only error na aaye
            update._callback_query = FakeQuery(query.from_user, query.message, f"v_main_{movie_id}")
            
            await button_callback(update, context)
            return
                
            title = movie_data['title']
            all_qualities = movie_data['qualities']
            
            # Sirf wahi files filter karo jo is selected season ki hain
            filtered_qualities = []
            for file_data in all_qualities:
                extra_info = file_data[5] if len(file_data) > 5 else ""
                if extract_season_name(extra_info) == selected_season:
                    filtered_qualities.append(file_data)
                    
            if not filtered_qualities:
                await query.answer("❌ No files found for this season!", show_alert=True)
                return
                
            # Ab sirf is Season ki files list karo
            # Video jaisa Text List format banana
            file_list_text = f"📺 **{title} - {selected_season}**\n\n👇 **Your Requested Files Are Here**\n\n"
            
            for idx, file_data in enumerate(filtered_qualities[:10], start=1):
                quality = file_data[0]
                file_size = file_data[3] if len(file_data) > 3 else "Unknown Size"
                extra_info = file_data[5] if len(file_data) > 5 else ""
                
                ep_tag = f"[{extra_info}] " if extra_info else ""
                file_list_text += f"**{idx}.** 💾 {file_size} | {title} {ep_tag}{quality}\n\n"

            selection_text = file_list_text
            keyboard_markup = create_quality_selection_keyboard(movie_id, title, filtered_qualities, page=0, season=selected_season, view="main")
            
            # Hum wahi purana keyboard function use kar rahe hain, bas list chhoti bhej rahe hain
            keyboard_markup = create_quality_selection_keyboard(movie_id, title, filtered_qualities, page=0, season=selected_season)
            
            # ✅ FIX: InlineKeyboardMarkup ke andar list 'inline_keyboard' ek tuple ki tarah return hoti hai naye python-telegram-bot versions me.
            # Isliye humein pehle usko list mein badalna padega, tab usme Naya button daalna hoga.
            
            keyboard_list = list(keyboard_markup.inline_keyboard)
            keyboard_list.insert(0, [InlineKeyboardButton("🔙 Back to Seasons", callback_data=f"movie_{movie_id}")])
            
            new_keyboard = InlineKeyboardMarkup(keyboard_list)
            
            await query.edit_message_text(
                selection_text,
                reply_markup=new_keyboard,
                parse_mode='Markdown'
            )
            return
            
            await query.edit_message_text(
                selection_text,
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
        # ==================== ADMIN ACTIONS ====================
        
        # ==================== NAYA UI VIEWS, FILTERS & PAGINATION ====================
        elif query.data.startswith("v_") or query.data.startswith("fl_") or query.data.startswith("vpage_"):
            movie_data = context.user_data.get('selected_movie_data')
            if not movie_data:
                await query.answer("❌ Session expired. Search again.", show_alert=True)
                return

            movie_id = movie_data['id']
            title = movie_data['title']
            all_qualities = movie_data['qualities']

            if 'active_filter' not in context.user_data:
                context.user_data['active_filter'] = None

            # Filter Handle Karna
            if query.data.startswith("fl_"):
                parts = query.data.split('_', 3)
                f_type = parts[1]
                if f_type == "clear":
                    context.user_data['active_filter'] = None
                    await query.answer("✅ Filters Cleared!")
                else:
                    f_val = parts[3]
                    context.user_data['active_filter'] = {'type': f_type, 'value': f_val}
                    await query.answer(f"✅ Filter Applied: {f_val}")
                view_type = "main"
                page = 1
                
            # Pagination Handle Karna
            elif query.data.startswith("vpage_"):
                parts = query.data.split('_')
                page = int(parts[2])
                view_type = "main"
                
            # Menu Navigation
            else:
                parts = query.data.split('_')
                view_type = parts[1]
                page = 1 
                
                # ✅ NAYA: Video wale cool popups!
                if view_type in ["lang", "qual", "seas"]:
                    await query.answer("Share & Support Us ❤️", show_alert=False)

            # ==========================================
            # 🚀 SMART FILTER LOGIC (Seasons + Lang + Qual)
            # ==========================================
            filtered_qualities = all_qualities
            active_filter = context.user_data.get('active_filter')
            
            if active_filter:
                f_type = active_filter.get('type')
                f_val = active_filter.get('value').lower()
                temp_list = []
                
                for f in all_qualities:
                    # File ki saari details combine kar rahe hain
                    quality_str = str(f[0]).lower()
                    lang_name = str(f[4]).lower() if len(f) > 4 else ""
                    extra_info = str(f[5]).lower() if len(f) > 5 else ""
                    combined_text = f"{quality_str} {lang_name} {extra_info}"
                    
                    if f_type == 'seas':
                        s_name = extract_season_name(f[5] if len(f) > 5 else "").lower()
                        if s_name == f_val:
                            temp_list.append(f)
                            
                    elif f_type == 'lang':
                        if f_val in combined_text:
                            temp_list.append(f)
                            
                    elif f_type == 'qual':
                        if f_val in combined_text:
                            temp_list.append(f)
                            
                # ✅ NAYA POP-UP LOGIC: Agar is filter ki koi file nahi mili
                if not temp_list:
                    # 1. Telegram ka in-built Popup dikhao
                    await query.answer(f"❌ {active_filter['value'].upper()} format me file abhi available nahi hai!", show_alert=True)
                    # 2. Galat filter ko history se uda do taaki bot aage na atke
                    context.user_data['active_filter'] = None 
                    # 3. Yahi se waapis bhej do (UI change nahi hoga, waisa hi rahega)
                    return
                            
                filtered_qualities = temp_list

            # ==========================================
            # Pagination Logic (10 files per page)
            # ==========================================
            limit = 10
            total_pages = (len(filtered_qualities) + limit - 1) // limit if filtered_qualities else 1
            if page > total_pages: page = total_pages
            if page < 1: page = 1
            
            start_idx = (page - 1) * limit
            end_idx = start_idx + limit
            current_page_files = filtered_qualities[start_idx:end_idx]

            # UI Text Banana
            if view_type == "main" or view_type == "seas":
                text = f"📁 <b>{title}</b>\n"
                
                # 🚀 NAYA FIX: Season ko alag se bada aur highlight dikhane ke liye
                if 'selected_season' in context.user_data and context.user_data['selected_season']:
                    s_name = context.user_data['selected_season'].upper()
                    text += f"━━━━━━━━━━━━━━━━━━━━\n"
                    text += f" <b>[ {s_name} ]</b> \n"
                    text += f"━━━━━━━━━━━━━━━━━━━━\n"
                    
                if active_filter:
                    text += f"🔍 Filter: <b>{active_filter['value']}</b>\n"
                text += f"\n👇 <b>Your Requested Files Are Here</b>\n\n"
                
                if not filtered_qualities:
                    text += "❌ No files found for this filter.\n"
                else:
                    bot_username = context.bot.username
                    import re # Text clean karne ke liye tool
                    
                    for idx, file_data in enumerate(current_page_files, start=start_idx + 1):
                        quality = str(file_data[0])
                        
                        # 🚀 NAYA FIX: Doosre Bot (Manvi Bot) ke links ko hamesha ke liye uda do
                        quality = re.sub(r'\[([^\]]+)\]\(https?://[^\)]+\)', r'\1', quality)
                        quality = re.sub(r'\(https?://[^\)]+\)', '', quality)
                        quality = re.sub(r'https?://[^\s]+', '', quality)
                        # 👇 Ye 2 lines nayi add karni hain: t.me aur @usernames udane ke liye
                        quality = re.sub(r'(?i)t\.me/[^\s]+', '', quality)
                        quality = re.sub(r'@[a-zA-Z0-9_]+', '', quality)
                        
                        file_size = file_data[3] if len(file_data) > 3 else "Unknown"
                        
                        # Extra Info (Episodes) se bhi link saaf karo
                        extra_info = str(file_data[5]) if len(file_data) > 5 else ""
                        extra_info = re.sub(r'\[([^\]]+)\]\(https?://[^\)]+\)', r'\1', extra_info)
                        extra_info = re.sub(r'\(https?://[^\)]+\)', '', extra_info)
                        extra_info = re.sub(r'https?://[^\s]+', '', extra_info)
                        # 👇 Ye 2 lines yahan bhi add karni hain
                        extra_info = re.sub(r'(?i)t\.me/[^\s]+', '', extra_info)
                        extra_info = re.sub(r'@[a-zA-Z0-9_]+', '', extra_info)
                        
                        ep_tag = f"[{extra_info.strip()}] " if extra_info.strip() else ""
                        
                        text += f"<b>{idx}.</b> <b><a href='https://t.me/{bot_username}?start=file_{movie_id}_{idx-1}'>{file_size} | {title} {ep_tag}{quality.strip()}</a></b>\n\n"

            elif view_type in ["lang", "qual"]:
                text = f"📁 <b>{title}</b>\n\n👇 <b>Select {view_type.upper()} Filter:</b>\n\n"

            # Keyboard Banana
            keyboard = []
            
            # 1. MAIN MENU: Yahan normal buttons dikhenge
            if view_type == "main":
                if filtered_qualities:
                    keyboard.append([InlineKeyboardButton("🚀 SEND ALL", callback_data=f"sendall_{movie_id}")])
                
                keyboard.append([
                    InlineKeyboardButton("QUALITY", callback_data=f"v_qual_{movie_id}"),
                    InlineKeyboardButton("LANGUAGE", callback_data=f"v_lang_{movie_id}"),
                    InlineKeyboardButton("SEASON", callback_data=f"v_seas_{movie_id}")
                ])
                
                nav_buttons = []
                nav_buttons.append(InlineKeyboardButton("◀️ PREV" if page > 1 else "PAGE", callback_data=f"vpage_{movie_id}_{page-1}" if page > 1 else "ignore"))
                nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="ignore"))
                nav_buttons.append(InlineKeyboardButton("NEXT ▶️" if page < total_pages else "NEXT >", callback_data=f"vpage_{movie_id}_{page+1}" if page < total_pages else "ignore"))
                keyboard.append(nav_buttons)

            # 2. SEASON MENU: 🚀 NAYA FIX - Yahan baaki kachra gayab, sirf Seasons!
            elif view_type == "seas":
                keyboard.append([InlineKeyboardButton("⬇ SELECT SEASON ⬇", callback_data="ignore")])
                
                seasons = set()
                for f in all_qualities:
                    extra = f[5] if len(f) > 5 else ""
                    if extra:
                        s = extract_season_name(extra)
                        if s != "Extra Files": seasons.add(s)
                        
                s_list = sorted(list(seasons))
                row = []
                for s in s_list:
                    btn_text = s.upper()
                    if btn_text.startswith("SEASON ") and len(btn_text.split(" ")[1]) == 1:
                        btn_text = btn_text.replace("SEASON ", "SEASON 0")
                        
                    row.append(InlineKeyboardButton(btn_text, callback_data=f"fl_seas_{movie_id}_{s}"))
                    if len(row) == 2:
                        keyboard.append(row)
                        row = []
                if row: keyboard.append(row)
                
                keyboard.append([
                    InlineKeyboardButton("🔄 CLEAR FILTER", callback_data=f"fl_clear_{movie_id}_all"),
                    InlineKeyboardButton("🔼 BACK TO MENU", callback_data=f"v_main_{movie_id}")
                ])

            # 3. LANGUAGE MENU
            elif view_type == "lang":
                keyboard.append([InlineKeyboardButton("MALAYALAM", callback_data=f"fl_lang_{movie_id}_Malayalam"), InlineKeyboardButton("TAMIL", callback_data=f"fl_lang_{movie_id}_Tamil")])
                keyboard.append([InlineKeyboardButton("ENGLISH", callback_data=f"fl_lang_{movie_id}_English"), InlineKeyboardButton("HINDI", callback_data=f"fl_lang_{movie_id}_Hindi")])
                keyboard.append([InlineKeyboardButton("TELUGU", callback_data=f"fl_lang_{movie_id}_Telugu"), InlineKeyboardButton("KANNADA", callback_data=f"fl_lang_{movie_id}_Kannada")])
                keyboard.append([InlineKeyboardButton("GUJARATI", callback_data=f"fl_lang_{movie_id}_Gujarati"), InlineKeyboardButton("MARATHI", callback_data=f"fl_lang_{movie_id}_Marathi")])
                keyboard.append([InlineKeyboardButton("PUNJABI", callback_data=f"fl_lang_{movie_id}_Punjabi")])
                keyboard.append([InlineKeyboardButton("<< BACK TO MENU >>", callback_data=f"v_main_{movie_id}")])

            # 4. QUALITY MENU
            elif view_type == "qual":
                keyboard.append([InlineKeyboardButton("360P", callback_data=f"fl_qual_{movie_id}_360p"), InlineKeyboardButton("480P", callback_data=f"fl_qual_{movie_id}_480p")])
                keyboard.append([InlineKeyboardButton("720P", callback_data=f"fl_qual_{movie_id}_720p"), InlineKeyboardButton("1080P", callback_data=f"fl_qual_{movie_id}_1080p")])
                keyboard.append([InlineKeyboardButton("1440P", callback_data=f"fl_qual_{movie_id}_1440p"), InlineKeyboardButton("2160P", callback_data=f"fl_qual_{movie_id}_2160p")])
                keyboard.append([InlineKeyboardButton("4K", callback_data=f"fl_qual_{movie_id}_4K")])
                keyboard.append([InlineKeyboardButton("<< BACK TO MENU >>", callback_data=f"v_main_{movie_id}")])

            # 👇 YAHAN disable_web_page_preview=True ADD KAR DIYA HAI 👇
            await query.edit_message_text(
                text=text, 
                reply_markup=InlineKeyboardMarkup(keyboard), 
                parse_mode='HTML',
                disable_web_page_preview=True 
            )
            return
        
        # ==================== QUALITY PAGINATION (NEXT/BACK) ====================
        elif query.data.startswith("qualpage_"):
            # FIX: Split up to 3 times to get the season name safely
            parts = query.data.split('_', 3)
            movie_id = int(parts[1])
            page = int(parts[2])
            selected_season = parts[3] if len(parts) > 3 else None

            # Try fetching data from user_data first (Fast)
            movie_data = context.user_data.get('selected_movie_data')
            
            # Agar data expire ho gaya ho ya ID match na kare, to DB se nikalo
            if not movie_data or movie_data.get('id') != movie_id:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
                res = cur.fetchone()
                cur.close()
                close_db_connection(conn)
                
                title = res[0] if res else "Movie"
                qualities = get_all_movie_qualities(movie_id)
                
                # Context update karo
                context.user_data['selected_movie_data'] = {
                    'id': movie_id,
                    'title': title,
                    'qualities': qualities
                }
            else:
                title = movie_data['title']
                qualities = movie_data['qualities']

            # 👇 FIX: Agar Season select kiya tha, toh pehle wapas files filter karo page badalne se pehle
            if selected_season:
                filtered_qualities = []
                for file_data in qualities:
                    extra_info = file_data[5] if len(file_data) > 5 else ""
                    if extract_season_name(extra_info) == selected_season:
                        filtered_qualities.append(file_data)
                
                keyboard_markup = create_quality_selection_keyboard(movie_id, title, filtered_qualities, page=page, season=selected_season)
                
                # Season wale Next/Back mein bhi Upar "Back to Seasons" daalna zaroori hai
                keyboard_list = list(keyboard_markup.inline_keyboard)
                keyboard_list.insert(0, [InlineKeyboardButton("🔙 Back to Seasons", callback_data=f"movie_{movie_id}")])
                keyboard = InlineKeyboardMarkup(keyboard_list)
            else:
                # Normal Movie Pagination
                keyboard = create_quality_selection_keyboard(movie_id, title, qualities, page=page)
            
            # Sirf buttons update karein (Text same rahega)
            await query.edit_message_reply_markup(reply_markup=keyboard)
            return
        
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
                close_db_connection(conn)
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
                close_db_connection(conn)
                await query.edit_message_text(f"❌ DELETED: Request for '{movie_title}' from User ID {user_id} removed.", parse_mode='Markdown')
            else:
                await query.edit_message_text("❌ Database error during deletion.")

        # ==================== QUALITY SELECTION ====================
        # ==================== QUALITY SELECTION ====================
        elif query.data.startswith("quality_"):
            parts = query.data.split('_')
            movie_id = int(parts[1])
            selected_quality = parts[2]

            movie_data = context.user_data.get('selected_movie_data')

            if not movie_data or movie_data.get('id') != movie_id:
                qualities = get_all_movie_qualities(movie_id)
                movie_data = {'id': movie_id, 'title': 'Movie', 'qualities': qualities}

            if not movie_data or 'qualities' not in movie_data:
                await query.edit_message_text("❌ Error: Could not retrieve movie data. Please search again.")
                return

            chosen_file = None
            
            # 👇 NAYA BULLETPROOF CODE 👇
            for file_data in movie_data['qualities']:
                quality = file_data[0]
                url = file_data[1]
                file_id = file_data[2]
                
                if quality == selected_quality:
                    chosen_file = {'url': url, 'file_id': file_id}
                    break

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
            close_db_connection(conn)

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

# ==================== NEW MULTI-CHANNEL BACKUP FUNCTIONS ====================

def get_storage_channels():
    """Load channel list from .env"""
    channels_str = os.environ.get('STORAGE_CHANNELS', '')
    return [int(c.strip()) for c in channels_str.split(',') if c.strip()]

def generate_quality_label(file_name, file_size_str, ai_language=""):
    # Pehle episode format ko hamesha ke liye theek karo (S07E12 22 -> S07E12-22)
    name_lower = normalize_episodes(file_name.lower())
    quality = "HD"
    
    # 1. Detect Quality
    if "4k" in name_lower or "2160p" in name_lower: quality = "4K"
    elif "1080p" in name_lower: quality = "1080p"
    elif "720p" in name_lower: quality = "720p"
    elif "480p" in name_lower: quality = "480p"
    elif "360p" in name_lower: quality = "360p"
    elif "cam" in name_lower or "rip" in name_lower: quality = "CamRip"
    
    # 2. Add AI Detected Language
    lang_tag = f" ({ai_language})" if ai_language else ""
    
    # 3. Detect Series (S01, S02, S01E01, S01P01, Season 1, etc.)
    # 👇 नया पैटर्न जो S01P01 और S01E01 दोनों पकड़ेगा
    season_match = re.search(
        r'(?i)(s\d{1,2}\s*(?:[ep]\d{1,3})?'                 # S01 या S01E01 या S01P01
        r'|s\d{1,2}\s*\[?(?:e|ep|episode|p|part)\s*\d{1,3}' # S01E01, S01P01
        r'|\[?(?:e|ep|episode|p|part)\s*\d{1,3}(?:\s*(?:[-~_]|to)\s*(?:e|ep|episode|p|part)?\s*\d{1,3})?\]?'
        r'|season\s?\d+\b)', 
        name_lower
    )
    
    if season_match:
        episode_tag = season_match.group(0).upper().replace("P", "E")  # P01 को E01 जैसा दिखाने के लिए
        return f"{episode_tag} {quality}{lang_tag} [{file_size_str}]"
        
    return f"{quality}{lang_tag} [{file_size_str}]"

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

# ============================================================================
# 🎬 BATCH ID COMMAND (Fully Automatic via TMDB/IMDb)
# ============================================================================
async def batch_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID: return
    if not context.args:
        await update.message.reply_text("❌ Usage: `/batchid tt1234567`")
        return
        
    imdb_id = context.args[0].strip()
    status_msg = await update.message.reply_text(f"⏳ Extracting all details for {imdb_id}...")
    
    try:
        # 1. Metadata + Poster
        data = await run_async(fetch_movie_metadata, imdb_id)
        if not data:
            await status_msg.edit_text("❌ IMDb से डेटा नहीं मिला। API Key चेक करें।")
            return
        
        title, year, poster, genre, imdb_id_f, rating, plot, category = data
        
        # 2. Cast/Stars लाना
        cast_str = await run_async(fetch_cast_from_imdb, imdb_id_f, 5)
        
        # 3. DB Insertion (All Fields)
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 🛑 "cast" quoted and year is integer
        # 🎯 NAYA LOGIC: Title ki jagah IMDb ID par conflict check karega
        cur.execute("""
            INSERT INTO movies (title, url, imdb_id, poster_url, year, genre, rating, description, category, language, "cast") 
            VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (imdb_id) DO UPDATE SET 
            title = EXCLUDED.title,
            poster_url = EXCLUDED.poster_url, 
            year = EXCLUDED.year,
            genre = EXCLUDED.genre, 
            rating = EXCLUDED.rating, 
            description = EXCLUDED.description, 
            category = EXCLUDED.category, 
            "cast" = EXCLUDED."cast"
            RETURNING id
        """, (title, imdb_id_f, poster, year, genre, rating, plot, category, "Hindi", cast_str))
        
        movie_id = cur.fetchone()[0]
        
        # 👇 NAYA: Database se check karein ki kya pehle se files hain
        cur.execute("SELECT COUNT(*) FROM movie_files WHERE movie_id = %s", (movie_id,))
        file_count = cur.fetchone()[0]
        
        conn.commit()
        cur.close()
        close_db_connection(conn)

        # 4. Start Batch Session
        BATCH_SESSION.update({
            'active': True, 'movie_id': movie_id, 'movie_title': title, 
            'file_count': file_count, 'admin_id': update.effective_user.id, 
            'language': 'Hindi', 'category': category
        })

        # 5. Success Message with Details
        success_msg = (
            f"✅ **Dada! Metadata Fetched Successfully**\n\n"
            f"🎬 **Title:** `{title}`\n"
            f"📅 **Year:** {year}\n"
            f"🎭 **Genre:** {genre}\n"
            f"⭐️ **Rating:** {rating}\n"
            f"🏷️ **Category:** {category}\n"
            f"👥 **Cast:** {cast_str}\n\n"
        )
        
        if file_count > 0:
            success_msg += f"⚠️ **Old Files Found:** {file_count} (Aap inhe delete kar sakte hain ya nayi add kar sakte hain)\n\n"
            
        success_msg += f"🚀 **अब फाइल्स भेजें, फिर /done लिखें।**"
        
        # 👇 NAYA: Button Add Karein (Agar files hain tabhi delete button aayega)
        # 👇 NAYA: Button Add Karein (Agar files hain tabhi delete button aayega)
        keyboard = []
        if file_count > 0:
            keyboard.append([InlineKeyboardButton("🗑️ Delete OLD Files", callback_data=f"clearfiles_{movie_id}")])
        keyboard.append([InlineKeyboardButton("❌ Cancel Batch", callback_data="cancel_batch")])
        
        await status_msg.edit_text(success_msg, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    # ✅ BAS YE 3 LINES YAHAN ADD KARNI HAIN 👇
    except Exception as e:
        print(f"Error in batch_id_command: {e}")
        await status_msg.edit_text(f"❌ Kuch galat ho gaya: {e}")


# ============================================================================
# ✍️ BATCH MANUAL COMMAND (For Custom Names & Details)
# ============================================================================

async def batch_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID: return

    if not context.args: 
        await update.message.reply_text(
            "❌ **Galat Format!** Aise use karein:\n\n"
            "`/batch Movie Name, Year, Language, Genre, Category`\n\n"
            "**Example:**\n"
            "`/batch Pink Bra, 2023, Hindi, Adult, Web Series`", 
            parse_mode='Markdown'
        )
        return

    # 1. Parsing Custom Format
    raw_text = " ".join(context.args)
    parts = [p.strip() for p in raw_text.split(',')]
    
    title = parts[0] if len(parts) > 0 else "Unknown Title"
    year = parts[1] if len(parts) > 1 else ""
    language = parts[2] if len(parts) > 2 else "Hindi"
    genre = parts[3] if len(parts) > 3 else "Adult, Drama"
    category = parts[4] if len(parts) > 4 else "Web Series"
    
    rating = "N/A"
    plot = "Watch exclusive content on FlimfyBox Premium."
    poster_url = None
    
    # Retrieve stored IMDb ID and cast from user_data (if batchid was used)
    imdb_id = context.user_data.pop('batch_imdb_id', None)
    cast_str = context.user_data.pop('batch_cast', None)

    status_msg = await update.message.reply_text(f"⏳ Saving '{title}' to Database...", parse_mode='Markdown')

    conn = get_db_connection()
    if not conn: return
    
    try:
        cur = conn.cursor()
        
        # ✅ FIXED: Quote "cast" because it's a reserved keyword
        cur.execute(
            """
            INSERT INTO movies (title, url, imdb_id, poster_url, year, genre, rating, description, category, language, "cast") 
            VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (title) DO UPDATE 
            SET year = EXCLUDED.year, 
                genre = EXCLUDED.genre, 
                category = EXCLUDED.category, 
                language = EXCLUDED.language,
                "cast" = COALESCE(EXCLUDED."cast", movies."cast")
            RETURNING id
            """,
            (title, imdb_id, poster_url, year, genre, rating, plot, category, language, cast_str)
        )
        movie_id = cur.fetchone()[0]
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM movie_files WHERE movie_id = %s", (movie_id,))
        file_count = cur.fetchone()[0]
        cur.close()

        BATCH_SESSION.update({
            'active': True,
            'movie_id': movie_id,
            'movie_title': title,
            'file_count': 0,
            'admin_id': user_id,
            'language': language,
            'category': category
        })

        # Show cast in confirmation message (if any)
        cast_display = f"👥 **Cast:** {cast_str}\n" if cast_str else ""
        msg_text = (
            f"✅ **Batch Custom Mode Started!**\n\n"
            f"🎬 **Title:** {title}\n"
            f"📅 **Year:** {year}\n"
            f"🎭 **Genre:** {genre}\n"
            f"🗣️ **Language:** {language}\n"
            f"🏷️ **Category:** {category}\n"
            f"{cast_display}"
            f"🚀 **Step 1:** Ab movie/series ki Files (Video/Doc) bhejo.\n"
            f"🖼️ **Step 2:** Poster ke liye koi bhi ek Image bhej do.\n"
            f"✅ **Step 3:** Jab sab ho jaye to `/done` bhejo."
        )

        keyboard = []
        if file_count > 0:
            keyboard.append([InlineKeyboardButton("🗑️ Delete OLD Files", callback_data=f"clearfiles_{movie_id}")])
        keyboard.append([InlineKeyboardButton("❌ Cancel Batch", callback_data="cancel_batch")])
        
        await status_msg.edit_text(msg_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        logger.error(f"Batch Error: {e}")
        await status_msg.edit_text(f"❌ DB Error: {e}")
    finally:
        if conn: close_db_connection(conn)

# ============================================================================
# 🚀 SUPER BATCH SYSTEM (Smart Grouping + Auto Post)
# ============================================================================

async def superbatch_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Super Batch shuru karega"""
    if update.effective_user.id != ADMIN_USER_ID: return
    
    SUPER_BATCH_SESSION['active'] = True
    SUPER_BATCH_SESSION['admin_id'] = update.effective_user.id
    SUPER_BATCH_SESSION['files'] = []
    
    await update.message.reply_text(
        "🚀 **SUPER BATCH MODE ON!**\n\n"
        "👉 Ab aap ek sath 50-100 files (alag-alag movies ki) yahan forward kar dein.\n"
        "👉 Bot khud unhe movies ke hisaab se group karega.\n"
        "👉 Jab sab bhej dein, to type karein: `/superdone`",
        parse_mode='Markdown'
    )

async def superbatch_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Super batch me aayi hui files ko chupchap memory me save karega"""
    if not SUPER_BATCH_SESSION['active'] or update.effective_user.id != SUPER_BATCH_SESSION['admin_id']:
        return

    message = update.effective_message
    if not (message.document or message.video or message.audio): return
    
    caption = message.caption or message.text or ""
    file_id = None
    file_name = "File"
    file_size = 0
    thumb_id = None
    
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        file_size = message.document.file_size
        if message.document.thumbnail: thumb_id = message.document.thumbnail.file_id
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name
        file_size = message.video.file_size
        if message.video.thumbnail: thumb_id = message.video.thumbnail.file_id

    # Queue me save karo
    SUPER_BATCH_SESSION['files'].append({
        'file_id': file_id,
        'file_name': file_name,
        'file_size': file_size,
        'caption': caption,
        'thumb_id': thumb_id,
        'message_obj': message
    })
    
    count = len(SUPER_BATCH_SESSION['files'])
    if count % 10 == 0:
        await message.reply_text(f"📥 Received {count} files so far...")

async def superbatch_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Smart Grouping -> Gemini AI -> TMDB -> Auto Post"""
    if not SUPER_BATCH_SESSION['active'] or update.effective_user.id != SUPER_BATCH_SESSION['admin_id']:
        return

    # 🚀 YAHAN PAR TURANT 'OFF' KAR DIYA TAAKI NORMAL BATCH KAAM KAR SAKE
    SUPER_BATCH_SESSION['active'] = False
    files = SUPER_BATCH_SESSION['files']
    SUPER_BATCH_SESSION['files'] = [] 
    
    if not files:
        await update.message.reply_text("❌ Koi file nahi mili!")
        return

    status_msg = await update.message.reply_text(f"🔄 **Grouping {len(files)} files... Please wait.**", parse_mode='Markdown')

    grouped_movies = defaultdict(list)
    for f in files:
        raw_text = f['caption'] if f['caption'] else f['file_name']
        basic_data = await fallback_extraction(raw_text)
        temp_title = basic_data.get('title', 'Unknown_Movie').lower()
        grouped_movies[temp_title].append(f)

    total_movies = len(grouped_movies)
    await status_msg.edit_text(f"✅ **Files grouped into {total_movies} unique movies!**\n\n🚀 Auto-Processing & Posting starts now...", parse_mode='Markdown')

    success_movies = 0
    channels = get_storage_channels()
    target_channels = [ch.strip() for ch in os.environ.get('BROADCAST_CHANNELS', '').split(',') if ch.strip()]

    for i, (temp_title, movie_files) in enumerate(grouped_movies.items(), 1):
        try:
            await status_msg.edit_text(f"⚙️ Processing Movie {i}/{total_movies}...\n🎬 Name: `{temp_title}`")
            
            first_file = movie_files[0]
            image_bytes = None
            if first_file['thumb_id']:
                try:
                    # tg_file = await context.bot.get_file(first_file['thumb_id'])
                    # image_bytes = bytes(await tg_file.download_as_bytearray())
                    # 🛑 TEMPORARY BYPASS: API bachane ke liye Gemini ko image nahi de rahe
                    image_bytes = None
                except Exception: pass
            
            # 🎯 1. SMART EXTRACTION (Gemini Vision)
            ai_data = await get_movie_name_from_caption(first_file['caption'] or first_file['file_name'], image_bytes)
            movie_name = ai_data.get("title", temp_title)
            movie_year = ai_data.get("year", "")
            movie_lang = ai_data.get("language", "")
            gemini_category = ai_data.get("category", "Movies")
            movie_extra = "" # Naya: Extra Info

            # 🎯 2. TMDB & IMDB METADATA
            metadata = await run_async(fetch_movie_metadata, movie_name, movie_year, movie_lang)
            
            if metadata:
                title, year, poster_url, genre, imdb_id, rating, plot, category = metadata
            else:
                title, year, poster_url, imdb_id = movie_name, (int(movie_year) if str(movie_year).isdigit() else 0), None, None
                genre, rating, plot, category = "Unknown", "N/A", "Auto Added", gemini_category

            # 🎯 3. CAST FETCHING
            cast_str = ""
            if imdb_id:
                cast_str = await run_async(fetch_cast_from_imdb, imdb_id, 5)

            conn = get_db_connection()
            if not conn: continue
            
            try:  
                cur = conn.cursor()
                
                # 🎯 4. SMART DB INSERTION (Same as pm_file_listener)
                cur.execute(
                    """
                    INSERT INTO movies (title, url, imdb_id, poster_url, year, genre, rating, description, category, language, extra_info, "cast") 
                    VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    ON CONFLICT (title) DO UPDATE 
                    SET poster_url = COALESCE(EXCLUDED.poster_url, movies.poster_url),
                        year = CASE WHEN movies.year = 0 THEN EXCLUDED.year ELSE movies.year END,
                        genre = COALESCE(EXCLUDED.genre, movies.genre),
                        rating = COALESCE(EXCLUDED.rating, movies.rating),
                        description = COALESCE(EXCLUDED.description, movies.description),
                        category = COALESCE(EXCLUDED.category, movies.category),
                        language = CASE WHEN EXCLUDED.language != '' THEN EXCLUDED.language ELSE movies.language END,
                        extra_info = CASE WHEN EXCLUDED.extra_info != '' THEN EXCLUDED.extra_info ELSE movies.extra_info END,
                        "cast" = COALESCE(EXCLUDED."cast", movies."cast")
                    RETURNING id
                    """,
                    (title, imdb_id, poster_url, year, genre, rating, plot, category, movie_lang, movie_extra, cast_str)
                )
                movie_id = cur.fetchone()[0]

                # 📂 Insert Files
                for f in movie_files:
                    backup_map = {}
                    main_url = ""
                    
                    if channels:
                        for chat_id in channels:
                            try:
                                sent = await f['message_obj'].copy(chat_id=chat_id)
                                backup_map[str(chat_id)] = sent.message_id
                                
                                # Main URL hamesha pehle (Main Dump) channel ka banega
                                if chat_id == channels[0]:
                                    main_url = f"https://t.me/c/{str(chat_id).replace('-100', '')}/{sent.message_id}"
                                    
                                await asyncio.sleep(0.5) # Telegram flood se bachne ke liye
                            except Exception as e:
                                logger.error(f"Backup failed for channel {chat_id}: {e}")

                    file_size_str = get_readable_file_size(f['file_size'])
                    
                    # 👇 NAYA FIX: Yahan bhi Caption use karenge 👇
                    text_for_detection = f['caption'] if f['caption'] else f['file_name']
                    label = generate_quality_label(text_for_detection, file_size_str, movie_lang)

                    f_ai_data = await fallback_extraction(text_for_detection)
                    f_lang = f_ai_data.get('language', '')
                    f_extra = f_ai_data.get('extra_info', '')

                    cur.execute(
                        """
                        INSERT INTO movie_files (movie_id, quality, file_size, url, backup_map, languages, extra_info) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (movie_id, quality) DO UPDATE SET 
                        url = EXCLUDED.url, file_size = EXCLUDED.file_size, backup_map = EXCLUDED.backup_map, file_id = NULL,
                        languages = EXCLUDED.languages, extra_info = EXCLUDED.extra_info
                        """,
                        (movie_id, label, file_size_str, main_url, json.dumps(backup_map), f_lang, f_extra)
                    )
                
                # 🛑 CRITICAL FIX 1: Pehle Movie aur Files ko save karlo taaki Aliases ke error se ye delete na ho!
                conn.commit()

                # 🤖 CRITICAL FIX 2: Generate and Save AI Aliases in Superbatch SAFELY (Non-blocking)
                try:
                    # Async function use karna zaroori hai taaki bot hang na ho
                    aliases = await run_async(generate_aliases_gemini, title, str(year), category)
                    
                    # Fallback agar Gemini API fail ho
                    if not aliases:
                        aliases = [title.lower().strip(), title.replace(" ", "").lower()]
                    
                    for alias in set(aliases): # Duplicates hata do
                        if not alias or len(alias) > 255: continue
                        try:
                            cur.execute("INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING", (movie_id, alias.lower().strip()))
                        except Exception as alias_insert_err:
                            # Agar ek alias galat ho to use ignore karke baaki save karo (Current transaction rollback)
                            conn.rollback() 
                            
                    conn.commit() # Aliases DB me save ho gaye!
                except Exception as alias_err:
                    logger.error(f"SuperBatch Alias Generation Error: {alias_err}")
                    conn.rollback()

                cur.close()
                
            except Exception as db_err:
                logger.error(f"SuperBatch DB Error: {db_err}")
                if conn: conn.rollback()
            finally:
                if conn: close_db_connection(conn)
            
            # --- POSTER PROCESSING (Landscape Blur Effect) ---
            raw_photo = poster_url if (poster_url and poster_url != 'N/A' and poster_url.startswith('http')) else None
            if not raw_photo and image_bytes: 
                raw_photo = image_bytes
                
            if raw_photo:
                # Yahan hamara naya function call hoga
                photo_to_send = await make_landscape_poster(raw_photo)
            else: 
                photo_to_send = DEFAULT_POSTER

            # 🛑 100% SAFE HTML CAPTION + RANDOM STYLES
            safe_rating = rating if rating else "N/A"
            safe_genre = genre if genre else "Unknown"

            res_set = set()
            for f in movie_files:
                match = re.search(r'(\d{3,4}p)', str(f.get('file_name', '')).lower())
                if match:
                    res_set.add(match.group(1))
            res_list = sorted(list(res_set), key=lambda x: int(x.replace('p','')), reverse=True)
            dynamic_res = " | ".join(res_list) if res_list else "1080p | 720p | 480p"
            
            safe_title = title.replace('<', '').replace('>', '')
            unicode_title = get_safe_font(safe_title)

            # 🎲 2 RANDOM STYLES 🎲 (Box wala hat gaya)
            style_choice = random.choice([1, 2])

            if style_choice == 1:
                # 🌟 Style 1: Clean Minimalist Divider (Mobile & PC Friendly)
                caption = (
                    f"🎬 <b>{safe_title}</b>\n"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                    f"✨ <b>Genre:</b> {safe_genre}\n"
                    f"🔊 <b>Language:</b> {movie_lang if movie_lang else 'Hindi'}\n"
                    f"💿 <b>Quality:</b> V2 HQ-HDTC {dynamic_res}\n"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                    f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
                    f"👇 <b>Download Below</b> 👇"
                )
            else:
                # Style 2: Tree Line + Premium Font (Pehle ye Style 3 tha)
                caption = (
                    f"🔥 <b>{unicode_title}</b>\n"
                    f" ├ ✨ Genre: {safe_genre}\n"
                    f" ├ 🔊 Language: {movie_lang if movie_lang else 'Hindi'}\n"
                    f" └ 💿 Quality: V2 HQ-HDTC {dynamic_res}\n"
                    f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
                    f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
                    f"👇 <b>Download Below</b> 👇"
                )

            # --- SECURE LINK & BUTTONS (As it was) ---
            secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"

            post_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("Download Now", url=secure_url), InlineKeyboardButton("Download Now", url=secure_url)],
                [InlineKeyboardButton("⚡ Download Now", url=secure_url)],
                [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
            ])

            topic_id = 1
            cat_lower = str(category or "").lower()
            for tid, keywords in TOPIC_MAPPING.items():
                if any(k.lower() in cat_lower for k in keywords):
                    topic_id = tid
                    break
            
            if topic_id == 1:
                if "south" in cat_lower: topic_id = 20
                elif "hollywood" in cat_lower: topic_id = 32
                elif "bollywood" in cat_lower: topic_id = 16
                elif "anime" in cat_lower: topic_id = 22
                elif "series" in cat_lower: topic_id = 18

            try:
                if topic_id == 1:
                    await context.bot.send_photo(chat_id=FORUM_GROUP_ID, photo=photo_to_send, caption=caption, parse_mode='HTML', reply_markup=post_keyboard)
                else:
                    await context.bot.send_photo(chat_id=FORUM_GROUP_ID, message_thread_id=topic_id, photo=photo_to_send, caption=caption, parse_mode='HTML', reply_markup=post_keyboard)
            except Exception as e:
                logger.error(f"SuperBatch Forum Post Error: {e}")

            # 👇 YAHAN SE MAIN CHANNEL PAR BHEJNE KA ASLI LOGIC SHURU HOTA HAI 👇
            
            # --- THE "NINJA FIX" --- 
            # Pehle decide karte hain ki photo kya bhejna hai
            is_bytes = hasattr(photo_to_send, 'read')
            current_media = photo_to_send
            
            uploaded_file_id = None # Isme Telegram ki File ID store hogi
            
            if target_channels:
                for chat_id_str in target_channels: 
                    try:
                        chat_id = int(chat_id_str)
                        sent_msg = None
                        
                        # Agar humare pass pehle se ID hai, toh file upload nahi karni
                        if uploaded_file_id:
                            sent_msg = await context.bot.send_photo(
                                chat_id=chat_id,
                                photo=uploaded_file_id, # 👈 Direct ID
                                caption=caption,
                                parse_mode='HTML',
                                reply_markup=post_keyboard
                            )
                        else:
                            # Pehli baar upload karna hai (Bytes se)
                            if is_bytes:
                                current_media.seek(0) # File pointer ko shuru me laao
                                
                            sent_msg = await context.bot.send_photo(
                                chat_id=chat_id,
                                photo=current_media, # 👈 Actual bytes
                                caption=caption,
                                parse_mode='HTML',
                                reply_markup=post_keyboard
                            )
                            # Ek baar upload hone ke baad, Telegram se permanent File ID save karlo
                            if sent_msg and sent_msg.photo:
                                uploaded_file_id = sent_msg.photo[-1].file_id 
                                
                        # ✅ DB me save karna zaroori hai taaki baad me /restore kaam kare
                        if sent_msg:
                            save_post_to_db(movie_id, chat_id, sent_msg.message_id, "FlimfyBox_Bot", caption, uploaded_file_id or poster_url, "photo", post_keyboard.to_dict(), None, "movies")
                            await asyncio.sleep(1.5)
                            
                    except Exception as e:
                        logger.error(f"❌ Failed to post in channel {chat_id_str}: {e}")

            success_movies += 1
            await asyncio.sleep(2) # Flood limit se bachne ke liye delay

        except Exception as e:
            logger.error(f"SuperBatch Movie Error: {e}")
            continue

    await status_msg.edit_text(
        f"🎉 **SUPER BATCH COMPLETED!**\n\n"
        f"✅ Total Movies Processed: {success_movies}/{total_movies}\n"
        f"🚀 All movies auto-posted to Channels & Forum successfully!",
        parse_mode='Markdown'
    )

    SUPER_BATCH_SESSION.update({'active': False, 'admin_id': None, 'files': []})

async def pm_file_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 🛡️ Ye line add karo (Line 2145 ke aas-paas)
    if BATCH_18_SESSION.get('active') or SUPER_BATCH_SESSION.get('active'):
        return
    # 👇 NAYA CODE: VIP Payment Screenshot Check 👇
    if context.user_data.get('payment_step') == 'screenshot' and update.message.photo:
        await payment_photo_handler(update, context)
        return

    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID: return
    if SUPER_BATCH_SESSION.get('active'): return
    if BATCH_18_SESSION.get('active'): return

    message = update.effective_message

    # ==========================================
    # 🖼️ CUSTOM POSTER UPLOAD LOGIC (Photo & URL Both Supported)
    # ==========================================
    if BATCH_SESSION.get('active'):
        is_poster_update = False
        public_url = None
        
        # 1. Agar Admin ne Photo bheji hai
        if message.photo:
            is_poster_update = True
            status_msg = await message.reply_text("🖼️ Image received! Uploading poster to cloud...")
            photo_file_id = message.photo[-1].file_id
            public_url = await upload_image_to_telegraph(context.bot, photo_file_id)
            
        # 2. Agar Admin ne direct Image URL bheja hai (http/https se shuru hone wala)
        elif message.text and message.text.strip().startswith("http"):
            is_poster_update = True
            status_msg = await message.reply_text("🔗 Image URL received! Linking poster directly...")
            public_url = message.text.strip()

        # Agar dono mein se koi bhi step trigger hua hai (Photo ya URL)
        if is_poster_update:
            if public_url:
                movie_id = BATCH_SESSION['movie_id']
                conn = get_db_connection()
                if conn:
                    try:
                        cur = conn.cursor()
                        cur.execute("UPDATE movies SET poster_url = %s WHERE id = %s", (public_url, movie_id))
                        conn.commit()
                        cur.close()
                    except Exception as e:
                        logger.error(f"Poster Update Error: {e}")
                    finally:
                        close_db_connection(conn)

                await status_msg.edit_text("✅ **Poster Successfully Updated!**\nAb aap files bhej sakte hain ya `/done` kar sakte hain.", parse_mode='Markdown')
            else:
                await status_msg.edit_text("❌ Poster upload fail ho gaya. Kripya image ya URL dobara bhejein.")
            
            return # Yahan ruk jao taaki image/url aage file ki tarah save na ho
        
    # --- ISKE NEECHE TUMHARA PURANA PHASE 2 WALA CODE AAYEGA JO FILES SAVE KARTA HAI ---
    if not (message.document or message.video): return
    
    # ... (purana file save aur forward logic) ...
    message = update.effective_message
    if not (message.document or message.video or message.photo): return

    caption = message.caption or ""
    if caption.startswith('/post_query'):
        return

    # 🚀 THE MAIN FIX: Agar sirf Photo aayi hai (bina caption ke) aur Batch OFF hai,
    # toh isko Poster maan lo aur koi Error message mat do (Takrav khatam).
    if message.photo and not caption and not BATCH_SESSION.get('active'):
        return 

    async with auto_batch_lock:
        
        # ==========================================
        # 🤖 PHASE 1: START BATCH (2-Step Verification)
        # ==========================================
        if not BATCH_SESSION.get('active'):
            
            raw_caption = message.caption or message.text
            if not raw_caption:
                await message.reply_text("❌ **Batch Off!**\nFile ke sath CAPTION mein movie naam likho.", parse_mode='Markdown')
                return
            
            status_msg = await message.reply_text("🧠 Analyzing Thumbnail & Caption with Gemini Vision...", quote=True)

            # 🛑 STEP 1: THUMBNAIL DOWNLOAD FOR GEMINI VISION
            image_bytes = None
            thumb_file_id = None
            
            try:
                if message.photo:
                    thumb_file_id = message.photo[-1].file_id
                elif message.video and message.video.thumbnail:
                    thumb_file_id = message.video.thumbnail.file_id
                elif message.document and message.document.thumbnail:
                    thumb_file_id = message.document.thumbnail.file_id

                if thumb_file_id:
                    # Backup for Auto Post later (Agar TMDB fail hua)
                    BATCH_SESSION['extracted_thumb'] = thumb_file_id
                    
                    # Download immediately for Gemini to "SEE"
                    # tg_file = await context.bot.get_file(thumb_file_id)
                    # image_bytes = bytes(await tg_file.download_as_bytearray())
                    # 🛑 TEMPORARY BYPASS: API bachane ke liye Gemini ko image nahi de rahe
                    image_bytes = None
            except Exception as e:
                logger.error(f"Failed to extract thumbnail for Gemini: {e}")

            # 🎯 STEP 2: GEMINI AI SE DATA NIKALO (Passing Image & Caption!)
            ai_data = await get_movie_name_from_caption(raw_caption, image_bytes)
            
            movie_name = ai_data.get("title", "UNKNOWN")
            movie_year = ai_data.get("year", "")
            movie_lang = ai_data.get("language", "")
            movie_extra = ""
            gemini_category = ai_data.get("category", "")
            
            if movie_name == "UNKNOWN" or len(movie_name) < 2:
                await status_msg.edit_text("❌ Movie naam extract nahi ho paya.\n\n`/batch Movie Name` use karein.")
                return
            
            await status_msg.edit_text(f"✅ **Gemini Extracted:** 🎬 {movie_name}\n⏳ Fetching IMDb Data & TMDB HD Poster...")

            # 🎯 STEP 3: IMDb (Data) + TMDB (HD Poster) FETCH
            metadata = await run_async(fetch_movie_metadata, movie_name, movie_year, movie_lang)
            
            if metadata:
                title, year, poster_url, genre, imdb_id, rating, plot, category = metadata
            else:
                # Fallback: Jo Gemini AI ne dekha wahi maan lo
                title = movie_name
                year = int(movie_year) if movie_year and str(movie_year).isdigit() else 0
                poster_url, imdb_id = None, None
                genre, rating, plot = "Unknown", "N/A", "Auto Added"
                category = gemini_category if gemini_category else "Movies"

            # 👇 NAYA CODE: IMDb ID milne par Cast (Stars) fetch karein
            cast_str = ""
            if imdb_id:
                cast_str = await run_async(fetch_cast_from_imdb, imdb_id, 5)

            # Database Insert...
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    # 👇 UPDATE: "cast" column aur COALESCE logic add kiya gaya hai
                    cur.execute(
                        """
                        INSERT INTO movies (title, url, imdb_id, poster_url, year, genre, rating, description, category, language, extra_info, "cast") 
                        VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                        ON CONFLICT (title) DO UPDATE 
                        SET imdb_id = COALESCE(EXCLUDED.imdb_id, movies.imdb_id),
                            poster_url = COALESCE(EXCLUDED.poster_url, movies.poster_url),
                            year = CASE WHEN movies.year = 0 THEN EXCLUDED.year ELSE movies.year END,
                            category = COALESCE(EXCLUDED.category, movies.category),
                            genre = COALESCE(EXCLUDED.genre, movies.genre),
                            rating = COALESCE(EXCLUDED.rating, movies.rating),
                            description = COALESCE(EXCLUDED.description, movies.description),
                            language = CASE WHEN EXCLUDED.language != '' THEN EXCLUDED.language ELSE movies.language END,
                            extra_info = CASE WHEN EXCLUDED.extra_info != '' THEN EXCLUDED.extra_info ELSE movies.extra_info END,
                            "cast" = COALESCE(EXCLUDED."cast", movies."cast")
                        RETURNING id
                        """,
                        (title, imdb_id, poster_url, year, genre, rating, plot, category, movie_lang, movie_extra, cast_str)
                    )
                    movie_id = cur.fetchone()[0]
                    conn.commit()
                    
                    cur.execute("SELECT COUNT(*) FROM movie_files WHERE movie_id = %s", (movie_id,))
                    file_count = cur.fetchone()[0]
                    cur.close()

                    BATCH_SESSION.update({
                        'active': True, 'movie_id': movie_id, 'movie_title': title, 
                        'file_count': file_count, 'admin_id': user_id,
                        'year': str(year) if year else movie_year, 'category': category, 'language': movie_lang
                    })
                    
                    keyboard = []
                    if file_count > 0:
                        keyboard.append([InlineKeyboardButton("🗑️ Delete OLD Files", callback_data=f"clearfiles_{movie_id}")])
                    keyboard.append([InlineKeyboardButton("❌ Cancel Batch", callback_data="cancel_batch")])
                    
                    await status_msg.edit_text(
                        f"✅ **Batch Started!**\n\n🎬 Movie: **{title}**\n📅 Year: {year if year else 'N/A'}\n🏷️ Category: {category}\n\n🚀 **Ab apni files bhejna shuru karo!**\nJab ho jaye: `/done`",
                        parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    
                except Exception as e:
                    logger.error(f"DB Error: {e}")
                    await status_msg.edit_text(f"❌ Database Error: {e}")
                finally:
                    close_db_connection(conn)
            return

        # ==========================================
        # 📤 PHASE 2: SAVE FILES (Jab Batch ON ho) - NO CHANGES HERE
        # ==========================================
        upload_status = await message.reply_text("⏳ Uploading file...", quote=True)
        # ... (Baaki ka Phase 2 ka code aapka same rahega)

        channels = get_storage_channels()
        if not channels:
            await upload_status.edit_text("❌ No STORAGE_CHANNELS found")
            return

        backup_map = {}
        success_uploads = 0

        for chat_id in channels:
            try:
                sent = await message.copy(chat_id=chat_id)
                backup_map[str(chat_id)] = sent.message_id
                success_uploads += 1
            except Exception as e:
                logger.error(f"Upload failed: {e}")

        if success_uploads == 0:
            await upload_status.edit_text("❌ Upload fail ho gaya.")
            return

        file_name = message.document.file_name if message.document else (message.video.file_name if message.video else "File")
        file_size = message.document.file_size if message.document else (message.video.file_size if message.video else 0)
        
        # 👇👇👇 NAYA CODE: File/Video se Poster (Thumbnail) nikalna 👇👇👇
        if message.video and message.video.thumbnail:
            BATCH_SESSION['extracted_thumb'] = message.video.thumbnail.file_id
        elif message.document and message.document.thumbnail:
            BATCH_SESSION['extracted_thumb'] = message.document.thumbnail.file_id
        # 👆👆👆 ---------------------------------------------------- 👆👆👆

        file_size_str = get_readable_file_size(file_size)
        current_lang = BATCH_SESSION.get('language', '')
        
        # 👇 NAYA FIX: Telegram file name cut kar deta hai, isliye Caption check karenge 👇
        text_for_detection = message.caption if message.caption else file_name
        
        label = generate_quality_label(text_for_detection, file_size_str, current_lang)
        
        main_channel_id = channels[0]
        main_url = f"https://t.me/c/{str(main_channel_id).replace('-100', '')}/{backup_map.get(str(main_channel_id))}"

        # Yahan bhi kate hue naam ki jagah text_for_detection denge
        ai_data = await fallback_extraction(text_for_detection)
        f_lang = ai_data.get('language', '')
        f_extra = ai_data.get('extra_info', '')

        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO movie_files (movie_id, quality, file_size, url, backup_map, languages, extra_info) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (movie_id, quality) DO UPDATE SET 
                    url = EXCLUDED.url, file_size = EXCLUDED.file_size, backup_map = EXCLUDED.backup_map, file_id = NULL,
                    languages = EXCLUDED.languages, extra_info = EXCLUDED.extra_info
                    """,
                    (BATCH_SESSION['movie_id'], label, file_size_str, main_url, json.dumps(backup_map), f_lang, f_extra)
                )
                conn.commit()
                cur.close()
                BATCH_SESSION['file_count'] += 1
                movie_title = BATCH_SESSION.get('movie_title', 'Movie')
                await upload_status.edit_text(f"✅ **Saved:** `{movie_title} {label}`\n🔢 Total Files: {BATCH_SESSION['file_count']}", parse_mode='Markdown')
            except Exception as e:
                await upload_status.edit_text(f"❌ DB Save Failed: {e}")
            finally:
                close_db_connection(conn)
    
async def batch_done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not BATCH_SESSION.get('active'): 
        await update.message.reply_text("❌ Koi batch active nahi hai!")
        return
    
    status_msg = await update.message.reply_text("🔄 **Batch complete kar raha hoon...**\n🧠 AI Aliases generate ho rahe hain...", parse_mode='Markdown')

    try:
        movie_id = BATCH_SESSION.get('movie_id')
        movie_title = BATCH_SESSION.get('movie_title', 'Unknown')
        movie_year = BATCH_SESSION.get('year', '')
        movie_category = BATCH_SESSION.get('category', '')
        
        # DB से क्वालिटी और डेटा निकालें
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT genre, language, \"cast\", poster_url, rating FROM movies WHERE id = %s", (movie_id,))
        minfo = cur.fetchone()
        cur.execute("SELECT quality FROM movie_files WHERE movie_id = %s", (movie_id,))
        qrows = cur.fetchall()
        cur.close()
        close_db_connection(conn)

        db_genre = minfo[0] if minfo and minfo[0] else "Unknown"
        db_lang = minfo[1] if minfo and minfo[1] else "Hindi (LiNE) + HC-ESubs"
        m_poster = minfo[3] if minfo else None
        m_rating = minfo[4] if minfo else "N/A"

        # क्वालिटी अलाइनमेंट
        res_list = sorted(list(set(re.search(r'(\d{3,4}p)', r[0]).group(1) for r in qrows if re.search(r'(\d{3,4}p)', r[0]))), key=lambda x: int(x.replace('p','')), reverse=True)
        dynamic_res = " | ".join(res_list) if res_list else "1080p | 720p | 480p"

        # 🎯 आपका पसंदीदा क्लीन फॉर्मेट
        caption = (
            f"🎬 <b>{movie_title}</b>\n"
            f"✨ Genre: {db_genre}\n"
            f"Language: {db_lang}\n"
            f"Quality: V2 HQ-HDTC {dynamic_res}\n"
            f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
            f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
            f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
            f"👇 <b>Download Below</b> 👇"
        )
        
        # 👇 यहाँ स्पेसिंग (Indentation) ठीक कर दी गई है
        aliases = generate_aliases_gemini(movie_title, movie_year, movie_category)
        alias_count = 0
        conn = get_db_connection()
        
        if conn and aliases:
            try:
                cur = conn.cursor()
                for alias in aliases:
                    if not alias or len(alias) > 255: continue
                    try:
                        cur.execute("SAVEPOINT sp_alias")
                        cur.execute("INSERT INTO movie_aliases (movie_id, alias) VALUES (%s, %s) ON CONFLICT (movie_id, alias) DO NOTHING", (movie_id, alias.lower().strip()))
                        cur.execute("RELEASE SAVEPOINT sp_alias")
                        alias_count += 1
                    except Exception:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_alias")
                conn.commit()
                cur.close()
            except Exception:
                if conn: conn.rollback()
            finally:
                close_db_connection(conn)

        # 🚀 POST TO FORUM
        forum_post_status = "⏳ Posting to Forum..."
        
        topic_id = 1
        cat_lower = str(movie_category or "").lower()
        for tid, keywords in TOPIC_MAPPING.items():
            if any(k.lower() in cat_lower for k in keywords):
                topic_id = tid
                break
        if topic_id == 1:
            if "south" in cat_lower: topic_id = 20
            elif "hollywood" in cat_lower: topic_id = 32
            elif "bollywood" in cat_lower: topic_id = 16
            elif "anime" in cat_lower: topic_id = 22
            elif "series" in cat_lower: topic_id = 18
            
        # --- SECURE LINK FOR SUPERBATCH POST ---
        secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"

        post_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Download Now", url=secure_url), InlineKeyboardButton("Download Now", url=secure_url)],
            [InlineKeyboardButton("⚡ Download Now", url=secure_url)],
            [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
        ])
        
        photo_to_send = m_poster if (m_poster and m_poster != 'N/A' and m_poster.startswith('http')) else None
        if not photo_to_send:
            thumb_file_id = context.bot_data.get(f"auto_thumb_{movie_id}")
            if thumb_file_id:
                photo_to_send = thumb_file_id
        if not photo_to_send: photo_to_send = DEFAULT_POSTER

        target_channels = [ch.strip() for ch in os.environ.get('BROADCAST_CHANNELS', '').split(',') if ch.strip()]
        
        try:
            if topic_id == 1:
                await context.bot.send_photo(chat_id=FORUM_GROUP_ID, photo=photo_to_send, caption=caption, parse_mode='HTML', reply_markup=post_keyboard)
            else:
                await context.bot.send_photo(chat_id=FORUM_GROUP_ID, message_thread_id=topic_id, photo=photo_to_send, caption=caption, parse_mode='HTML', reply_markup=post_keyboard)
            forum_post_status = f"✅ Auto-Posted to Forum (Topic ID: {topic_id})"
        except Exception as e:
            logger.error(f"Auto Forum Post Error: {e}")
            forum_post_status = f"⚠️ Forum Post Failed"

        channels_count = len(get_storage_channels())
        report = (
            f"🎉 **Batch Completed!**\n\n"
            f"🎬 **Movie:** `{movie_title}`\n"
            f"📅 **Year:** {movie_year if movie_year else 'N/A'}\n"
            f"🏷️ **Category:** {movie_category}\n"
            f"📂 **Files Saved:** {BATCH_SESSION.get('file_count', 0)}\n"
            f"🤖 **AI Aliases:** {alias_count}\n\n"
            f"✅ Backups: {channels_count} channels\n"
            f"💬 {forum_post_status}"
        )

        extracted_thumb = BATCH_SESSION.get('extracted_thumb')
        if extracted_thumb: context.bot_data[f"auto_thumb_{movie_id}"] = extracted_thumb

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 Auto Post (HD TMDB Poster)", callback_data=f"autopost_{movie_id}")],
            [InlineKeyboardButton("📢 Manual Post (Send Poster)", callback_data=f"askposter_{movie_id}")]
        ])

        await status_msg.edit_text(report, parse_mode='Markdown', reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Error in batch_done_command: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Error during /done: {e}")

    finally:
        BATCH_SESSION.update({
            'active': False, 'movie_id': None, 'movie_title': None, 
            'file_count': 0, 'admin_id': None, 'year': '', 'category': '', 
            'extracted_thumb': None
        })

                
async def handle_admin_poster(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin se photo lekar clean caption ke sath channel me post karega"""
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID: 
        return

    # Check karo ki bot photo ka wait kar raha tha ya nahi
    movie_id = context.user_data.get('waiting_for_poster')
    if not movie_id: 
        return # Agar wait nahi kar raha tha, to ignore karo

    if not update.message.photo:
        await update.message.reply_text("❌ Please send a valid PHOTO.")
        return

    # Sabse acchi quality ki photo nikalo
    file_id = update.message.photo[-1].file_id
    status_msg = await update.message.reply_text("⏳ Publishing to channels...")

    # 1. Database se sirf Title nikalo
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()
    cur.execute("SELECT title FROM movies WHERE id = %s", (movie_id,))
    res = cur.fetchone()
    cur.close()
    close_db_connection(conn)

    if not res:
        await status_msg.edit_text("❌ Movie not found in DB.")
        context.user_data.pop('waiting_for_poster', None)
        return
    
    m_title = res[0]

    # 🎯 FIX: This block must be indented to match the rest of the function
    channel_caption = (
        f"🎬 <b>{m_title}</b>\n"
        f"✨ Genre: {m_genre}\n"
        f"Language: {m_lang}\n"
        f"Quality: V2 HQ-HDTC {dynamic_res}\n"
        f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
        f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
        f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
        f"👇 <b>Download Below</b> 👇"
    )

    # 3. Download Buttons Banao
    secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Download Now", url=secure_url),
            InlineKeyboardButton("Download Now", url=secure_url)
        ],
        [InlineKeyboardButton("⚡ Download Now", url=secure_url)],
        [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
    ])

    # 4. Channels me Post karo
    channels_str = os.environ.get('BROADCAST_CHANNELS', '')
    target_channels = [ch.strip() for ch in channels_str.split(',') if ch.strip()]

    if not target_channels:
        await status_msg.edit_text("❌ Error: No BROADCAST_CHANNELS found in .env")
        context.user_data.pop('waiting_for_poster', None)
        return

    sent_count = 0
    for chat_id_str in target_channels:
        try:
            chat_id = int(chat_id_str)
            sent_msg = await context.bot.send_photo(
                chat_id=chat_id,
                photo=file_id,
                caption=channel_caption,
                parse_mode='HTML',
                reply_markup=keyboard
            )
            
            # Restore Feature ke liye DB me save karo
            if sent_msg:
                save_post_to_db(
                    movie_id, chat_id, sent_msg.message_id, "FlimfyBox_Bot",  # ✅ NAYA: bot3 hat gaya!
                    channel_caption, file_id, "photo", keyboard.to_dict(), None, "movies"
                )
                sent_count += 1
        except Exception as e:
            logger.error(f"Auto-post failed for {chat_id_str}: {e}")

    # 5. Finish and Clear State
    await status_msg.edit_text(f"✅ <b>Posted successfully to {sent_count} channels!</b>", parse_mode='HTML')
    context.user_data.pop('waiting_for_poster', None)

async def admin_post_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ FIXED: Smart Post Generator with proper error handling
    """
    try:
        user_id = update.effective_user.id
        if user_id != ADMIN_USER_ID:
            return

        message = update.message
        
        # 1. Check Media
        if not (message.photo or message.video):
            await message.reply_text("❌ Photo ya Video bhejo caption ke sath")
            return

        caption_text = message.caption or ""
        if not caption_text.startswith('/post_query'):
            return

        # 2. Extract Media
        file_id = None
        media_type = 'photo'

        if message.photo:
            file_id = message.photo[-1].file_id
            media_type = 'photo'
        elif message.video:
            file_id = message.video.file_id
            media_type = 'video'

        # 3. Parse Query
        raw_input = caption_text.replace('/post_query', '').strip()
        
        if ',' in raw_input:
            parts = raw_input.split(',', 1)
            query_text = parts[0].strip()
            custom_msg = parts[1].strip()
        else:
            query_text = raw_input
            custom_msg = ""

        if not query_text:
            await message.reply_text("❌ Movie name missing")
            return

        # 4. Find Movie in DB
        movie_id = None
        conn = get_db_connection()

        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM movies WHERE title ILIKE %s LIMIT 1",
                    (f"%{query_text}%",)
                )
                row = cur.fetchone()
                movie_id = row[0] if row else None
                cur.close()
            except Exception as e:
                logger.error(f"DB Error: {e}")
            finally:
                close_db_connection(conn)

        # 5. Generate Secure Links (Anti-Bot)
        bot1 = "FlimfyBox_SearchBot"
        bot2 = "urmoviebot"
        bot3 = "FlimfyBox_Bot"
        
        if movie_id:
            # ✅ FIXED: Web App Secure Link (Exactly like /superdone)
            secure_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"
            link1 = secure_link
            link2 = secure_link
            link3 = secure_link
        else:
            # ⚠️ Agar movie DB me nahi hai (Sirf search query hai), to purana link chalega
            link_param = f"q_{query_text.replace(' ', '_')}"
            link1 = f"https://t.me/{bot1}?start={link_param}"
            link2 = f"https://t.me/{bot2}?start={link_param}"
            link3 = f"https://t.me/{bot3}?start={link_param}"

        # 6. Build Keyboard
        if movie_id:
            # ✅ Yahan se web_app= hata diya hai, ab direct tumhara /watch/ wala link khulega
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Download Now", url=link1),
                    InlineKeyboardButton("Download Now", url=link2),
                ],
                [InlineKeyboardButton("Download Now", url=link3)],
                [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
            ])
        else:
            # Agar fallback tg:// link hai, toh normal URL rehne do
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Download Now", url=link1),
                    InlineKeyboardButton("Download Now", url=link2),
                ],
                [InlineKeyboardButton("Download Now", url=link3)],
                [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
            ])
        # 7. Build Caption
        channel_caption = f"🎬 <b>{query_text}</b>\n"
        if custom_msg:
            channel_caption += f"✨ <b>{custom_msg}</b>\n\n"
        else:
            channel_caption += "\n"
        
        channel_caption += (
            "➖➖➖➖➖➖➖\n"
            f"<b>Support:</b> <a href='https://t.me/+2hFeRL4DYfBjZDQ1'>Join Chat</a>\n"
            "➖➖➖➖➖➖➖\n"
            "<b>👇 Download Below</b>"
        )

        # 8. Send to Channels
        channels_str = os.environ.get('BROADCAST_CHANNELS', '')
        target_channels = [ch.strip() for ch in channels_str.split(',') if ch.strip()]

        if not target_channels:
            await message.reply_text("❌ No BROADCAST_CHANNELS configured in .env")
            return

        sent_count = 0
        failed_list = []

        for chat_id_str in target_channels:
            try:
                # ✅ FIXED: Parse channel ID properly
                try:
                    chat_id = int(chat_id_str)
                except ValueError:
                    failed_list.append(f"Invalid ID: {chat_id_str}")
                    continue

                logger.info(f"📤 Sending to {chat_id}...")

                sent_msg = None

                if media_type == 'video':
                    sent_msg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_id,
                        caption=channel_caption,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )
                else:
                    sent_msg = await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=file_id,
                        caption=channel_caption,
                        reply_markup=keyboard,
                        parse_mode='HTML'
                    )

                if sent_msg:
                    logger.info(f"✅ Sent to {chat_id}, Message ID: {sent_msg.message_id}")
                    sent_count += 1

            except telegram.error.BadRequest as e:
                error = str(e)
                if "group is deactivated" in error or "not found" in error:
                    failed_list.append(f"{chat_id_str}: Channel inactive/deleted")
                else:
                    failed_list.append(f"{chat_id_str}: {error}")
                logger.error(f"BadRequest for {chat_id_str}: {e}")
                
            except telegram.error.Forbidden as e:
                failed_list.append(f"{chat_id_str}: Bot blocked/no access")
                logger.error(f"Forbidden for {chat_id_str}: {e}")
                
            except Exception as e:
                failed_list.append(f"{chat_id_str}: {str(e)[:30]}")
                logger.error(f"Error sending to {chat_id_str}: {e}")

        # 9. Final Report
        report = f"""✅ <b>Post Processed ({media_type.capitalize()})</b>

📤 <b>Sent:</b> {sent_count}/{len(target_channels)}
❌ <b>Failed:</b> {len(failed_list)}

🎬 <b>Movie:</b> {query_text}
📝 <b>Extra:</b> {custom_msg or 'None'}"""

        if failed_list:
            report += "\n\n<b>Errors:</b>\n"
            for err in failed_list[:3]:  # Show first 3 errors
                report += f"• {err}\n"

        await message.reply_text(report, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Critical error in post_query: {e}", exc_info=True)
        await message.reply_text(f"❌ Error: {str(e)[:100]}")

# ==========================================
# 🚀 AUTO MASS-FORWARD & LINK SHORTENER
# ==========================================

async def shorten_link(long_url):
    """GPLinks API se link chota karke Earning link banata hai."""
    api_key = os.environ.get('GPLINKS_API_KEY')
    if not api_key:
        return long_url # Agar API key nahi hai, toh purana link hi chalne do
        
    api_url = f"https://gplinks.in/api?api={api_key}&url={long_url}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                data = await resp.json()
                if data.get("status") == "success":
                    return data.get("shortenedUrl")
    except Exception as e:
        print(f"Shortener Error: {e}")
    return long_url


# ==========================================
# 🚀 18+ MASS-FORWARD BATCH SYSTEM (SAFE)
# ==========================================

async def shorten_link(long_url):
    """GPLinks API se link chota karke Earning link banata hai."""
    api_key = os.environ.get('GPLINKS_API_KEY')
    if not api_key:
        return long_url
        
    api_url = f"https://gplinks.in/api?api={api_key}&url={long_url}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as resp:
                data = await resp.json()
                if data.get("status") == "success":
                    return data.get("shortenedUrl")
    except Exception as e:
        logger.error(f"Shortener Error: {e}")
    return long_url

# ==================== 18+ BATCH SYSTEM (SAME AS NORMAL BATCH) ====================

BATCH_18_SESSION = {'active': False, 'movie_id': None, 'movie_title': None, 'file_count': 0, 'admin_id': None}

async def batch18_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """18+ बैच शुरू करें - बिल्कुल /batch की तरह काम करेगा"""
    if update.effective_user.id != ADMIN_USER_ID:
        return

    if BATCH_SESSION.get('active'):  # अगर नॉर्मल बैच चल रहा है तो 18+ नहीं चलेगा
        await update.message.reply_text("❌ पहले से नॉर्मल बैच चल रहा है। कृपया उसे /done करें या /cancel करें।")
        return

    BATCH_18_SESSION.update({
        'active': True,
        'admin_id': update.effective_user.id,
        'movie_id': None,
        'movie_title': None,
        'file_count': 0
    })

    await update.message.reply_text(
        "🔞 **18+ बैच मोड चालू!**\n\n"
        "👉 अब आप जिस 18+ मूवी/सीरीज़ की फ़ाइलें भेजना चाहते हैं, उसकी **पहली फ़ाइल** कैप्शन के साथ भेजें।\n"
        "👉 बॉट उसका टाइटल, साल, भाषा आदि निकालकर आपको दिखाएगा।\n"
        "👉 इसके बाद आप उसी मूवी की बाकी सभी फ़ाइलें (कोई भी क्वालिटी/एपिसोड) एक-एक करके भेज सकते हैं।\n"
        "👉 सब भेजने के बाद `/done18` लिखें।",
        parse_mode='Markdown'
    )

# ============================================================================
# 🔞 18+ BATCH LISTENER (Fully Optimized & Fixed)
# ============================================================================

async def batch18_listener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    🔞 18+ BATCH LISTENER: Auto-extracts metadata and saves files.
    Fixed: Clean logs, better adult detection, proper error handling.
    """
    # === GUARD CLAUSES ===
    if not BATCH_18_SESSION.get('active'):
        return
    
    if update.effective_user.id != BATCH_18_SESSION.get('admin_id'):
        return

    # Takrav se bachne ke liye checks
    if BATCH_SESSION.get('active') or SUPER_BATCH_SESSION.get('active'):
        return

    message = update.effective_message
    if not message or not (message.document or message.video):
        return

    # === PHASE 1: FIRST FILE = METADATA & MOVIE CREATION ===
    if BATCH_18_SESSION.get('movie_id') is None:
        raw_caption = message.caption or ""
        if not raw_caption:
            await message.reply_text(
                "❌ **18+ बैच:** पहली फाइल के साथ caption में movie का naam zaroor dein.",
                parse_mode='Markdown'
            )
            return
        
        # 🎯 Adult content auto-detection from filename
        raw_lower = raw_caption.lower()
        force_adult = any(tag in raw_lower for tag in ['unrated', '18+', 'adult', 'hot', 'bhabhi', 'mastani'])
        
        status_msg = await message.reply_text(
            "🔞 **Analyzing 18+ content...**" + (" (Forced Adult Mode)" if force_adult else ""),
            quote=True
        )

        # === AI EXTRACTION ===
        image_bytes = None
        try:
            ai_data = await get_movie_name_from_caption(raw_caption, image_bytes)
            movie_name = ai_data.get("title", "UNKNOWN")
            movie_year = ai_data.get("year", "")
            movie_lang = ai_data.get("language", "Hindi") or "Hindi"
            movie_extra = ai_data.get("extra_info", "")
            
            # Force adult if detected in filename
            gemini_category = "Adult" if force_adult else (ai_data.get("category", "Web Series") or "Web Series")
            
        except Exception as e:
            logger.error(f"AI extraction failed: {e}")
            movie_name = clean_telegram_text(raw_caption.split('\n')[0][:50])
            movie_year = ""
            movie_lang = "Hindi"
            movie_extra = ""
            gemini_category = "Adult" if force_adult else "Web Series"

        if movie_name == "UNKNOWN" or len(movie_name) < 2:
            await status_msg.edit_text(
                "❌ Name identify nahi ho paya. Sahi naam ke sath dobara bhejein."
            )
            return
        
        await status_msg.edit_text(
            f"✅ **Extracted:** 🎬 `{movie_name}` ({movie_year or 'N/A'})\n"
            f"⏳ Fetching metadata from TMDB/Google...",
            parse_mode='Markdown'
        )

        # === METADATA FETCHING WITH CLEAN FALLBACK ===
        title, year, poster_url, genre, imdb_id, rating, plot, category = (
            movie_name, 
            int(movie_year) if str(movie_year).isdigit() else 0,
            None,  # poster_url
            "Adult, Romance, Drama",  # default genre
            None,  # imdb_id
            "N/A",  # rating
            "Watch exclusive 18+ content on FlimfyBox Premium.",  # default plot
            gemini_category  # category
        )
        
        cast_str = ""

        # Try TMDB first (with adult_mode=True)
        try:
            metadata = await run_async(
                fetch_movie_metadata, 
                movie_name, 
                movie_year, 
                movie_lang, 
                adult_mode=True  # Critical for 18+ content
            )
            
            if metadata:
                t_title, t_year, t_poster, t_genre, t_imdb, t_rating, t_plot, t_cat = metadata
                # Only update if we got valid data
                if t_title and t_title != "N/A": title = t_title
                if t_year and t_year > 0: year = t_year
                if t_poster and t_poster != "N/A": poster_url = t_poster
                if t_genre and t_genre != "N/A": genre = t_genre
                if t_imdb and t_imdb != "N/A": imdb_id = t_imdb
                if t_rating and t_rating != "N/A": rating = t_rating
                if t_plot and t_plot != "N/A": plot = t_plot
                # Keep Adult category even if TMDB says something else
                category = gemini_category if force_adult else (t_cat if t_cat else gemini_category)
                
                logger.info(f"✅ TMDB metadata found for: {title}")
                
                # Fetch cast if we have IMDB ID
                if imdb_id:
                    cast_str = await run_async(fetch_cast_from_imdb, imdb_id, 5)
                    
        except Exception as e:
            logger.warning(f"⚠️ TMDB failed for {movie_name}: {e}")

        # If TMDB failed completely, try Google
        if not poster_url or poster_url == "N/A":
            try:
                google_data = await fetch_metadata_from_google(movie_name)
                if google_data:
                    # Only fill missing fields from Google
                    if not poster_url or poster_url == "N/A":
                        poster_url = google_data.get('poster')
                    if not genre or genre == "Adult, Romance, Drama":
                        genre = google_data.get('genre', "Adult, Romance, Drama")
                    if not plot or plot == "Watch exclusive 18+ content on FlimfyBox Premium.":
                        plot = google_data.get('plot', plot)
                    
                    logger.info(f"✅ Google fallback used for: {title}")
                else:
                    logger.info(f"⚠️ No Google results for: {title}, using defaults")
                    
            except Exception as e:
                logger.warning(f"⚠️ Google search failed for {title}: {e}")

        # === DATABASE INSERTION ===
        conn = get_db_connection()
        if not conn:
            await status_msg.edit_text("❌ Database Connection Failed.")
            return

        try:
            cur = conn.cursor()
            
            # Check for existing movie
            cur.execute(
                "SELECT id, poster_url, year FROM movies WHERE title ILIKE %s",
                (title,)
            )
            existing = cur.fetchone()

            if existing:
                # Update existing with better data if available
                existing_id, existing_poster, existing_year = existing
                final_poster = poster_url if poster_url else existing_poster
                final_year = year if (year and year > 0) else existing_year
                
                cur.execute("""
                    UPDATE movies 
                    SET poster_url = COALESCE(%s, poster_url),
                        year = CASE WHEN %s > 0 THEN %s ELSE year END,
                        genre = COALESCE(%s, genre),
                        rating = COALESCE(%s, rating),
                        description = COALESCE(%s, description),
                        category = %s,
                        language = COALESCE(NULLIF(%s, ''), language),
                        extra_info = COALESCE(NULLIF(%s, ''), extra_info),
                        "cast" = COALESCE(%s, "cast")
                    WHERE id = %s
                    RETURNING id
                """, (final_poster, final_year, final_year, genre, rating, plot, 
                      category, movie_lang, movie_extra, cast_str, existing_id))
                movie_id = cur.fetchone()[0]
                logger.info(f"🔄 Updated existing movie: {title} (ID: {movie_id})")
                
            else:
                # Insert new movie
                cur.execute("""
                    INSERT INTO movies 
                    (title, url, imdb_id, poster_url, year, genre, rating, 
                     description, category, language, extra_info, "cast") 
                    VALUES (%s, '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (title, imdb_id, poster_url, year, genre, rating, 
                      plot, category, movie_lang, movie_extra, cast_str))
                movie_id = cur.fetchone()[0]
                logger.info(f"✅ Created new movie: {title} (ID: {movie_id})")

            conn.commit()

            # Check for old files
            cur.execute("SELECT COUNT(*) FROM movie_files WHERE movie_id = %s", (movie_id,))
            file_count_old = cur.fetchone()[0]
            cur.close()

            # Update session
            BATCH_18_SESSION.update({
                'movie_id': movie_id,
                'movie_title': title,
                'file_count': 0,
                'year': str(year) if year else movie_year,
                'category': category,
                'language': movie_lang
            })

            # Build success message
            cast_display = f"\n👥 **Cast:** {cast_str}" if cast_str else ""
            source_info = "TMDB" if imdb_id else ("Google" if poster_url else "Default")
            
            success_msg = (
                f"✅ **18+ Metadata Ready** ({source_info})\n\n"
                f"🎬 **Title:** `{title}`\n"
                f"📅 **Year:** {year if year else 'N/A'}\n"
                f"🎭 **Genre:** {genre}\n"
                f"⭐️ **Rating:** {rating}\n"
                f"🏷️ **Category:** {category}\n"
                f"{cast_display}\n"
                f"🚀 **Ab files bhejein, phir `/done18` likhein.**"
            )

            # Build keyboard
            keyboard = []
            if file_count_old > 0:
                keyboard.append([InlineKeyboardButton(
                    "🗑️ Delete OLD Files", 
                    callback_data=f"clearfiles_{movie_id}"
                )])
            keyboard.append([InlineKeyboardButton(
                "❌ Cancel Batch", 
                callback_data="cancel_batch18"
            )])

            await status_msg.edit_text(
                success_msg, 
                parse_mode='Markdown', 
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"❌ 18+ DB Error: {e}")
            if conn: conn.rollback()
            await status_msg.edit_text(f"❌ Database Error: {e}")
        finally:
            close_db_connection(conn)
        
        return  # First file processed, wait for more

    # === PHASE 2: SUBSEQUENT FILES ===
    upload_status = await message.reply_text(
        "⏳ Saving 18+ file...", 
        quote=True
    )

    # Get storage channels
    channels = get_storage_channels()
    backup_map = {}
    
    if channels:
        for chat_id in channels:
            try:
                sent = await message.copy(chat_id=chat_id)
                backup_map[str(chat_id)] = sent.message_id
            except Exception as e:
                logger.error(f"18+ Backup failed for {chat_id}: {e}")

    # Extract file info
    file_name = (message.document.file_name if message.document 
                 else (message.video.file_name if message.video else "File"))
    file_size = (message.document.file_size if message.document 
                 else (message.video.file_size if message.video else 0))
    file_size_str = get_readable_file_size(file_size)

    # Generate quality label
    text_for_detection = message.caption if message.caption else file_name
    current_lang = BATCH_18_SESSION.get('language', 'Hindi')
    label = generate_quality_label(text_for_detection, file_size_str, current_lang)

    # Extract extra info
    try:
        ai_data_f = await fallback_extraction(text_for_detection)
        f_lang = ai_data_f.get('language', '')
        f_extra = ai_data_f.get('extra_info', '')
    except:
        f_lang = ''
        f_extra = ''

    # Build main URL
    main_url = ""
    if channels and backup_map:
        main_channel = channels[0]
        main_url = f"https://t.me/c/{str(main_channel).replace('-100', '')}/{backup_map.get(str(main_channel))}"

    # Save to database
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO movie_files 
                (movie_id, quality, file_size, url, backup_map, languages, extra_info) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (movie_id, quality) DO UPDATE SET 
                    url = EXCLUDED.url, 
                    file_size = EXCLUDED.file_size, 
                    backup_map = EXCLUDED.backup_map,
                    file_id = NULL,
                    languages = EXCLUDED.languages, 
                    extra_info = EXCLUDED.extra_info
            """, (
                BATCH_18_SESSION['movie_id'], 
                label, 
                file_size_str, 
                main_url, 
                json.dumps(backup_map),
                f_lang, 
                f_extra
            ))
            conn.commit()
            cur.close()
            
            BATCH_18_SESSION['file_count'] += 1
            
            await upload_status.edit_text(
                f"✅ **Saved:** `{BATCH_18_SESSION['movie_title']} {label}`\n"
                f"📦 Total Files: {BATCH_18_SESSION['file_count']}",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"18+ File Save Error: {e}")
            if conn: conn.rollback()
            await upload_status.edit_text(f"❌ Save Error: {e}")
        finally:
            close_db_connection(conn)
    else:
        await upload_status.edit_text("❌ Database connection failed")


# ============================================================================
# 🔞 18+ BATCH DONE (Optimized)
# ============================================================================

async def batch18_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Complete 18+ batch and post to adult channel"""
    
    # Validation
    if not BATCH_18_SESSION.get('active'):
        await update.message.reply_text("❌ कोई सक्रिय 18+ बैच नहीं है।")
        return

    if update.effective_user.id != BATCH_18_SESSION.get('admin_id'):
        return

    movie_id = BATCH_18_SESSION.get('movie_id')
    movie_title = BATCH_18_SESSION.get('movie_title', 'Unknown')
    file_count = BATCH_18_SESSION.get('file_count', 0)

    if not movie_id or file_count == 0:
        await update.message.reply_text(
            "❌ कोई फ़ाइल सेव नहीं की गई। बैच रद्द किया जा रहा है।"
        )
        BATCH_18_SESSION.update({
            'active': False, 'movie_id': None, 'movie_title': None,
            'file_count': 0, 'admin_id': None
        })
        return

    # Get adult channel
    adult_channel_id_str = os.environ.get('ADULT_CHANNEL_ID')
    if not adult_channel_id_str:
        await update.message.reply_text("❌ .env में ADULT_CHANNEL_ID सेट नहीं है।")
        return
    
    try:
        ADULT_CHANNEL_ID = int(adult_channel_id_str)
    except ValueError:
        await update.message.reply_text("❌ ADULT_CHANNEL_ID invalid है।")
        return

    status_msg = await update.message.reply_text(
        f"🔄 **{movie_title}** का 18+ पोस्ट बन रहा है..."
    )

    # Fetch movie data
    conn = get_db_connection()
    if not conn:
        await status_msg.edit_text("❌ Database error.")
        return

    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT poster_url, year, genre, rating, language, description 
            FROM movies WHERE id = %s
        """, (movie_id,))
        m_data = cur.fetchone()
        
        if not m_data:
            await status_msg.edit_text("❌ Movie DB में नहीं मिली।")
            return
            
        poster_url, year, genre, rating, language, description = m_data

        # Get qualities
        cur.execute(
            "SELECT quality FROM movie_files WHERE movie_id = %s", 
            (movie_id,)
        )
        qrows = cur.fetchall()
        cur.close()
        
    except Exception as e:
        await status_msg.edit_text(f"❌ DB Error: {e}")
        return
    finally:
        close_db_connection(conn)

    # Build quality string
    res_list = set()
    for r in qrows:
        match = re.search(r'(\d{3,4}p)', r[0])
        if match:
            res_list.add(match.group(1))
    
    res_list = sorted(list(res_list), key=lambda x: int(x.replace('p', '')), reverse=True)
    dynamic_res = " | ".join(res_list) if res_list else "1080p | 720p | 480p"

    # Process poster
    raw_photo = poster_url if (poster_url and poster_url != 'N/A' and poster_url.startswith('http')) else None
    if raw_photo:
        photo_to_send = await make_landscape_poster(raw_photo)
    else:
        photo_to_send = DEFAULT_POSTER

    # Build caption
    safe_title = movie_title.replace('<', '').replace('>', '')
    unicode_title = get_safe_font(safe_title)
    
    style_choice = random.choice([1, 2])
    
    if style_choice == 1:
        caption = (
            f"🔞 <b>{safe_title}</b>\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"✨ <b>Genre:</b> {genre or 'Romance, Drama'}\n"
            f"🔊 <b>Language:</b> {language or 'Hindi'}\n"
            f"💿 <b>Quality:</b> V2 HQ-HDTC {dynamic_res}\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
            f"👇 <b>Download Below</b> 👇"
        )
    else:
        caption = (
            f"🔥 <b>{unicode_title}</b>\n"
            f" ├ ✨ Genre: {genre or 'Romance, Drama'}\n"
            f" ├ 🔊 Language: {language or 'Hindi'}\n"
            f" └ 💿 Quality: V2 HQ-HDTC {dynamic_res}\n"
            f"━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━\n"
            f"🔞 <b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
            f"👇 <b>Download Below</b> 👇"
        )

    # Build keyboard
    secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_id}"
    post_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Download Now", url=secure_url),
            InlineKeyboardButton("Download Now", url=secure_url)
        ],
        [InlineKeyboardButton("⚡ Download Now", url=secure_url)],
        [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
    ])

    # Send to adult channel
    try:
        if hasattr(photo_to_send, 'read'):
            photo_to_send.seek(0)
            
        sent = await context.bot.send_photo(
            chat_id=ADULT_CHANNEL_ID,
            photo=photo_to_send,
            caption=caption,
            parse_mode='HTML',
            reply_markup=post_keyboard
        )

        # Save to DB for restore feature
        if sent:
            save_post_to_db(
                movie_id=movie_id,
                channel_id=ADULT_CHANNEL_ID,
                message_id=sent.message_id,
                bot_username="FlimfyBox_Bot",
                caption=caption,
                media_file_id=sent.photo[-1].file_id if sent.photo else None,
                media_type="photo",
                keyboard_data=post_keyboard.to_dict(),
                topic_id=None,
                content_type="adult"
            )

        await status_msg.edit_text(
            f"✅ **18+ बैच पूर्ण!**\n\n"
            f"🎬 {movie_title}\n"
            f"📦 कुल फ़ाइलें: {file_count}\n"
            f"📢 एडल्ट चैनल में पोस्ट भेज दी गई।"
        )

    except Exception as e:
        logger.error(f"18+ Post Error: {e}")
        await status_msg.edit_text(f"❌ पोस्ट भेजने में एरर: {e}")

    # Clear session
    BATCH_18_SESSION.update({
        'active': False,
        'movie_id': None,
        'movie_title': None,
        'file_count': 0,
        'admin_id': None,
        'year': '',
        'category': '',
        'language': ''
    })


# ============================================================================
# 🔞 18+ BATCH CANCEL
# ============================================================================

async def batch18_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel active 18+ batch"""
    if update.effective_user.id == BATCH_18_SESSION.get('admin_id'):
        BATCH_18_SESSION.update({
            'active': False,
            'movie_id': None,
            'movie_title': None,
            'file_count': 0,
            'admin_id': None
        })
        await update.message.reply_text("🛑 18+ बैच रद्द कर दिया गया।")

async def admin_post_18(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Premium 18+ Post - Single Item (Fixed Crash)"""
    try:
        user_id = update.effective_user.id
        if user_id != ADMIN_USER_ID:
            return

        message = update.message
        replied_msg = message.reply_to_message

        media_msg    = None
        command_text = ""
        embed_link   = "" 

        if message.text and message.text.startswith('/post18'):
            command_text = message.text
            if replied_msg and (replied_msg.photo or replied_msg.video or replied_msg.document):
                media_msg = replied_msg
        elif message.caption and message.caption.startswith('/post18'):
            media_msg    = message
            command_text = message.caption

        if not command_text.startswith('/post18'): return

        status_msg = await message.reply_text("⏳ <b>Processing Premium Post...</b>", parse_mode='HTML')

        if "|" in command_text:
            parts        = command_text.split('|', 1)
            command_text = parts[0].strip()
            embed_link   = parts[1].strip()

        user_photo_id, user_video_id = None, None
        if media_msg:
            if media_msg.photo: user_photo_id = media_msg.photo[-1].file_id
            elif media_msg.video: user_video_id = media_msg.video.file_id
            elif media_msg.document:
                mime = getattr(media_msg.document, 'mime_type', '') or ''
                if "image" in mime: user_photo_id = media_msg.document.file_id
                else: user_video_id = media_msg.document.file_id

        raw_input = command_text.replace('/post18', '').strip()
        if ',' in raw_input:
            parts = raw_input.split(',', 1)
            query_text, custom_msg = parts[0].strip(), parts[1].strip()
        else:
            query_text, custom_msg = raw_input, ""

        if not query_text:
            await status_msg.edit_text("❌ Movie name missing!")
            return

        metadata = await run_async(fetch_movie_metadata, query_text)

        display_title = f"<b>{get_safe_font(query_text)}</b>"
        year_str, rating_str, genre_str = "", "", "Romance, Drama"
        plot_str = custom_msg or "Exclusive Full HD Episode."
        imdb_poster = None

        if metadata:
            m_title, m_year, m_poster, m_genre, m_imdb, m_rating, m_plot, m_cat = metadata
            if m_title and m_title != "N/A": display_title = f"<b>{get_safe_font(m_title)}</b>"
            if m_year and str(m_year) != "0": year_str = str(m_year)
            if m_genre and m_genre != "N/A": genre_str = m_genre
            if not custom_msg and m_plot and m_plot != "N/A": plot_str = m_plot[:220] + "..."
            if m_poster and m_poster != "N/A": imdb_poster = m_poster

        link_section = ""
        if embed_link:
            short_link = await shorten_link(embed_link) # Naya GPLink integration
            link_section = (
                f"\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
                f'📺 <b>Watch Online & Download:</b>\n👉 {short_link}'
            )

        year_display = f" ({year_str})" if year_str else ""
        channel_caption = (
            f"╔═══════════════════════╗\n"
            f"      🔥 {display_title} 🔥\n"
            f"      ━━━{year_display}━━━\n"
            f"╚═══════════════════════╝\n"
            f"\n"
            f"🔞 18+  |  💎 <b>Premium Quality</b>\n"
            f"🚨 <i>Only For Adults (18+)</i>"
            f"{link_section}\n\n"
            f"🔞 <b>Join Premium:</b> https://t.me/+wcYoTQhIz-ZmOTY1" 
        )

        target_channel = os.environ.get('ADULT_CHANNEL_ID')
        if not target_channel:
            await status_msg.edit_text("❌ ADULT_CHANNEL_ID missing!")
            return

        poster_final = user_photo_id or imdb_poster or DEFAULT_POSTER
        sent_post = None

        try:
            if user_video_id:
                sent_post = await context.bot.send_video(chat_id=int(target_channel), video=user_video_id, caption=channel_caption, parse_mode='HTML')
            else:
                sent_post = await context.bot.send_photo(chat_id=int(target_channel), photo=poster_final, caption=channel_caption, parse_mode='HTML')
        except Exception as post_err:
            await status_msg.edit_text(f"❌ Post failed:\n<code>{post_err}</code>", parse_mode='HTML')
            return

        await status_msg.edit_text(f"✅ <b>Premium Post Done!</b>\n🎬 Movie: <b>{query_text}</b>", parse_mode='HTML')

    except Exception as e:
        logger.error(f"Post18 Critical Error: {e}")
        try: await message.reply_text(f"❌ Error: {e}")
        except: pass
# ==================== ADMIN COMMANDS ====================
async def add_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to add a movie manually (Supports Unreleased)"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("Sorry Darling, sirf 𝑶𝒘𝒏𝒆𝒓 hi is command ka istemal kar sakte hain.")
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
            close_db_connection(conn)

ASK_MOVIE, ASK_USER = range(20, 22) # Naye states

async def notify_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 1: Admin types /notify"""
    if update.effective_user.id != ADMIN_USER_ID: return ConversationHandler.END
    
    await update.message.reply_text("🎬 <b>Smart Notify Started!</b>\n\n👉 सबसे पहले मुझे <b>Movie / Series</b> का नाम बताइए:", parse_mode='HTML')
    return ASK_MOVIE

async def notify_ask_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 2: Admin gives Movie Name"""
    # Cancel command check
    if update.message.text == '/cancel':
        await update.message.reply_text("❌ Notify Cancelled.")
        return ConversationHandler.END
        
    context.user_data['notify_movie'] = update.message.text
    await update.message.reply_text("👤 <b>अब User का Username या User ID बताइए:</b>\n(जैसे @username या 123456789)", parse_mode='HTML')
    return ASK_USER

async def notify_ask_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 3: Admin gives Username/ID -> Bot sends Template using Multi-Bot"""
    if update.message.text == '/cancel':
        await update.message.reply_text("❌ Notify Cancelled.")
        return ConversationHandler.END

    user_input = update.message.text.replace('@', '').strip()
    movie_name = context.user_data.get('notify_movie', 'Movie')

    # Find user ID from DB
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("❌ DB Error!")
        return ConversationHandler.END

    try:
        cur = conn.cursor()
        if user_input.isdigit(): # ID di hai
            cur.execute("SELECT first_name FROM user_requests WHERE user_id = %s LIMIT 1", (int(user_input),))
            target_user_id = int(user_input)
            res = cur.fetchone()
            first_name = res[0] if res else "User"
        else: # Username diya hai
            cur.execute("SELECT user_id, first_name FROM user_requests WHERE username ILIKE %s LIMIT 1", (user_input,))
            res = cur.fetchone()
            if not res:
                await update.message.reply_text(f"❌ '{user_input}' database me nahi mila. ID try karein.")
                return ConversationHandler.END
            target_user_id, first_name = res

        # 🎨 Beautiful Template
        msg = (
            f"🎉 <b>Good News!</b> 👋\n\n"
            f"Hello <b>{first_name}!</b> आपकी Requested File अब उपलब्ध है।\n\n"
            f"🎬 File: <b>{movie_name}</b>\n\n"
            f"इसे पाने के लिए अभी बॉट में मूवी का नाम टाइप करें और एन्जॉय करें! 😊\n\n"
            f"━━━━━━━━━━━━━━━━━━━\nRegards, <b>@ownermahi</b>"
        )

        # Multi-bot send function call karo
        success = await send_multi_bot_message(target_user_id, msg)

        if success:
            await update.message.reply_text(f"✅ <b>Perfect!</b> Notification successfully {first_name} ko bhej di gayi hai.", parse_mode='HTML')
        else:
            await update.message.reply_text("❌ <b>Fail!</b> User ne teeno bots ko block kar diya hai.", parse_mode='HTML')

    finally:
        close_db_connection(conn)

    return ConversationHandler.END

async def update_buttons_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        return

    if len(context.args) < 2:
        await update.message.reply_text("Usage: /fixbuttons <old_bot_username> <new_bot_username>")
        return

    old_bot = context.args[0].lstrip("@")
    new_bot = context.args[1].lstrip("@")

    status_msg = await update.message.reply_text(
        "🚀 **Safe Update Mode On...**\nStarting to fix buttons slowly to avoid ban.",
        parse_mode='Markdown'
    )

    conn = get_db_connection()
    if not conn:
        await status_msg.edit_text("❌ DB connection failed.")
        return

    cur = conn.cursor()
    cur.execute(
        "SELECT movie_id, channel_id, message_id FROM channel_posts WHERE bot_username = %s",
        (old_bot,)
    )
    posts = cur.fetchall()

    total = len(posts)
    success = 0

    for (m_id, ch_id, msg_id) in posts:
        try:
            # --- SECURE LINK FOR OLD POSTS UPDATE ---
            secure_url = f"https://flimfybox-bot-yht0.onrender.com/watch/{m_id}"

            new_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Download Server 1", url=secure_url)],
                [InlineKeyboardButton("📢 Join Channel", url=FILMFYBOX_CHANNEL_URL)]
            ])
            await context.bot.edit_message_reply_markup(
                chat_id=ch_id,
                message_id=msg_id,
                reply_markup=new_keyboard
            )

            success += 1
            await asyncio.sleep(3)
            if success % 50 == 0:
                await asyncio.sleep(10)
                await status_msg.edit_text(f"☕ Break...\nUpdated: {success}/{total}")

        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 5)
            continue
        except TelegramError as e:
            if "Message to edit not found" in str(e):
                cur.execute("DELETE FROM channel_posts WHERE channel_id = %s AND message_id = %s", (ch_id, msg_id))
                conn.commit()
            logger.error(f"Error editing {msg_id}: {e}")

    cur.close()
    close_db_connection(conn)
    await status_msg.edit_text(f"✅ Updated {success}/{total} posts safely.", parse_mode='Markdown')

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
                close_db_connection(conn)

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
            close_db_connection(conn)

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
            close_db_connection(conn)
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
            close_db_connection(conn)

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
        close_db_connection(conn)

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
            close_db_connection(conn)
            return

        user_id, first_name = user

        await context.bot.send_message(
            chat_id=user_id,
            text=message_text
        )

        await update.message.reply_text(f"✅ Message sent to `@{target_username}` ({first_name})", parse_mode='Markdown')

        cur.close()
        close_db_connection(conn)

    except telegram.error.Forbidden:
        await update.message.reply_text(f"❌ User blocked the bot.")
    except Exception as e:
        logger.error(f"Error in notify_user_by_username: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast HTML message to all users with formatting support"""
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Admin only command.")
        return

    try:
        # Command ke baad wala pura text (Formatting ke sath)
        if not context.args:
            await update.message.reply_text("Usage: /broadcast <b>Message Title</b>\n\nYour formatted text here...")
            return

        # Pure message ko extract karein
        message_text = update.message.text.replace('/broadcast', '').strip()

        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection failed.")
            return

        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id FROM user_requests")
        all_users = cur.fetchall()

        if not all_users:
            await update.message.reply_text("No users found in database.")
            cur.close()
            close_db_connection(conn)
            return

        status_msg = await update.message.reply_text(f"📤 Broadcasting to {len(all_users)} users...\n⏳ Please wait...")

        success_count = 0
        failed_count = 0

        for user_id_tuple in all_users:
            user_id = user_id_tuple[0]
            try:
                # 📢 YAHAN PAR 'HTML' USE HOGA
                await context.bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode='HTML',  # Isse Enter aur Bold kaam karega
                    disable_web_page_preview=True
                )
                success_count += 1
                await asyncio.sleep(0.05) # Flood protection
            except telegram.error.Forbidden:
                failed_count += 1
            except Exception as e:
                failed_count += 1

        await status_msg.edit_text(
            f"📊 <b>Broadcast Complete</b>\n\n"
            f"✅ Sent: {success_count}\n"
            f"❌ Failed: {failed_count}",
            parse_mode='HTML'
        )

        cur.close()
        close_db_connection(conn)

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
            close_db_connection(conn)
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
        close_db_connection(conn)

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
            close_db_connection(conn)
            return

        user_id, first_name = user

        notification_header = ""
        if optional_message:
            notification_header = optional_message

        warning_msg = await context.bot.send_message(
            chat_id=user_id,
            text="ᯓ➤This file automatically❕️deletes after 1 minute❕️so please forward it to another chat જ⁀➴",
            parse_mode='Markdown'
        )

        sent_msg = None
        media_type = "unknown"
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/MovieHdmoviehub")]])

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
        if sent_msg:
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                # Hum save kar rahe hain ki is movie ka post is channel me is ID par hai
                cur.execute(
                    "INSERT INTO channel_posts (movie_id, channel_id, message_id, bot_username) VALUES (%s, %s, %s, %s)",
                    (movie_id, chat_id, sent_msg.message_id, "FlimfyBox_Bot") # Current Main Bot Username
                )
                conn.commit()
                cur.close()
                close_db_connection(conn)
            except Exception as e:
                logger.error(f"Failed to save post ID: {e}")
        
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
            close_db_connection(conn)
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
        close_db_connection(conn)

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
            close_db_connection(conn)
            return

        status_msg = await update.message.reply_text(
            f"📤 Broadcasting media to {len(all_users)} users...\n⏳ Please wait..."
        )

        success_count = 0
        failed_count = 0
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/MovieHdmoviehub")]])

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
        close_db_connection(conn)

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
            close_db_connection(conn)
            return

        success_count = 0
        failed_count = 0
        join_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Channel", url="https://t.me/MovieHdmoviehub")]])

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
        close_db_connection(conn)

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
            close_db_connection(conn)
            return

        user_id, first_name = user

        await replied_message.forward(chat_id=user_id)

        await update.message.reply_text(f"✅ Forwarded to `@{target_username}` ({first_name})", parse_mode='Markdown')

        cur.close()
        close_db_connection(conn)

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
            close_db_connection(conn)
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
        close_db_connection(conn)

    except Exception as e:
        logger.error(f"Error in get_user_info: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def list_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all bot users with Accurate Count from Activity Log"""
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

        # 1. ✅ REAL TOTAL COUNT (From user_activity table)
        # Ye un sabhi unique users ko ginega jinhone kabhi bhi bot use kiya hai
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_activity")
        result = cur.fetchone()
        total_users = result[0] if result else 0

        # 2. GET LIST (From user_requests table because it has Names)
        # Note: List mein shayad kam log dikhein (sirf wo jinhone request kiya hai), 
        # lekin uppar Total Count sahi dikhega.
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

        # Calculate pages based on the list available (user_requests)
        cur.execute("SELECT COUNT(DISTINCT user_id) FROM user_requests")
        listable_users = cur.fetchone()[0]
        total_pages = (listable_users + per_page - 1) // per_page if listable_users > 0 else 1

        users_text = f"👥 **Bot Users** (Page {page}/{total_pages})\n"
        users_text += f"📊 **Total Unique Users: {total_users}**\n\n"

        if not users:
            users_text += "No active requesters found on this page."
        else:
            for idx, (user_id, username, first_name, req_count, last_seen) in enumerate(users, start=offset+1):
                username_str = f"`@{username}`" if username else "N/A"
                safe_name = (first_name or "Unknown").replace("<", "&lt;").replace(">", "&gt;")
                
                users_text += f"{idx}. <b>{safe_name}</b> ({username_str})\n"
                users_text += f"   🆔 `{user_id}` | 📥 Reqs: {req_count}\n"
                users_text += f"   🕒 {last_seen.strftime('%Y-%m-%d %H:%M')}\n\n"

        if total_users > listable_users:
            users_text += f"\n⚠️ *Note:* {total_users - listable_users} users ne bot use kiya hai par koi Request nahi bheji (isliye list me naam nahi hai)."

        await update.message.reply_text(users_text, parse_mode='HTML')

        cur.close()
        close_db_connection(conn)

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
        if conn: close_db_connection(conn)

async def fix_missing_metadata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Magic Command: Finds movies with missing info and fixes them - UPDATED
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ सिर्फ एडमिन के लिए!")
        return

    status_msg = await update.message.reply_text("⏳ **Scanning Database for incomplete movies...**", parse_mode='Markdown')

    conn = get_db_connection()
    if not conn:
        await status_msg.edit_text("❌ Database connection failed.")
        return

    try:
        cur = conn.cursor()
        # Find movies where ANY key info is missing (Genre, Poster, or Year)
        cur.execute("SELECT title FROM movies WHERE genre IS NULL OR poster_url IS NULL OR year IS NULL")
        movies_to_fix = cur.fetchall()
        
        if not movies_to_fix:
            await status_msg.edit_text("✅ **All Good!** Database mein sabhi movies ka metadata complete hai.")
            return

        total = len(movies_to_fix)
        await status_msg.edit_text(f"🧐 Found **{total}** movies to fix. Starting update process... (This may take time)")

        success_count = 0
        failed_count = 0

        for index, (title,) in enumerate(movies_to_fix):
            try:
                # Progress update every 10 movies
                if index % 10 == 0:
                    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

                # ✅ FETCH CORRECT METADATA (6 Values)
                metadata = fetch_movie_metadata(title)
                if metadata:
                    new_title, year, poster_url, genre, imdb_id, rating, plot, category = metadata

                    # Only update if we found something useful
                    if genre or poster_url or year > 0:
                        # ✅ CORRECT SQL UPDATE QUERY (Order Matters!)
                        cur.execute("""
                            UPDATE movies 
                            SET genre = %s, 
                                poster_url = %s, 
                                year = %s, 
                                imdb_id = %s, 
                                rating = %s
                            WHERE title = %s
                        """, (genre, poster_url, year, imdb_id, rating, title))
                        
                        conn.commit()
                        success_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
                
                # Sleep slightly to respect API limits
                await asyncio.sleep(0.5) 

            except Exception as e:
                # 🛑 ROLLBACK IS CRITICAL HERE
                if conn:
                    conn.rollback() 
                logger.error(f"Failed to fix {title}: {e}")
                failed_count += 1

        # Final Report
        await status_msg.edit_text(
            f"🎉 **Repair Complete!**\n\n"
            f"✅ Fixed: {success_count}\n"
            f"❌ Failed: {failed_count}\n"
            f"📊 Total Processed: {total}\n\n"
            f"Database updated successfully! 🚀",
            parse_mode='Markdown'
        )

    except Exception as e:
        logger.error(f"Error in fix_metadata: {e}")
        await status_msg.edit_text(f"❌ Error: {e}")
    finally:
        if cur: cur.close()
        if conn: close_db_connection(conn)

async def restore_posts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /restore <new_channel_id> <content_type> [delay]

    Examples:
    /restore -100111111111 movies       -> Sirf movies restore
    /restore -100222222222 adult        -> Sirf 18+ restore
    /restore -100333333333 series 5     -> Series, 5 sec delay
    /restore -100444444444 anime 3      -> Anime, 3 sec delay
    /restore -100111111111 all 3        -> Sab kuch (careful!)
    """
    if update.effective_user.id != ADMIN_USER_ID:
        return

    # --- Argument Check ---
    if len(context.args) < 2:
        await update.message.reply_text(
            "📋 <b>Restore Command Guide:</b>\n\n"
            "<code>/restore &lt;channel_id&gt; &lt;type&gt; [delay]</code>\n\n"
            "<b>Types Available:</b>\n"
            "🎬 <code>movies</code>  - Normal movies\n"
            "🔞 <code>adult</code>   - 18+ content\n"
            "📺 <code>series</code>  - Web series\n"
            "🎌 <code>anime</code>   - Anime\n"
            "📦 <code>all</code>     - Everything\n\n"
            "<b>Examples:</b>\n"
            "<code>/restore -100123456789 movies</code>\n"
            "<code>/restore -100987654321 adult 5</code>",
            parse_mode='HTML'
        )
        return

    # --- Parse Arguments ---
    try:
        new_channel_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(
            "❌ Channel ID galat hai!\n"
            "Sahi format: <code>-100XXXXXXXXXX</code>",
            parse_mode='HTML'
        )
        return

    content_type = context.args[1].lower().strip()

    # Valid types check
    valid_types = ['movies', 'adult', 'series', 'anime', 'all']
    if content_type not in valid_types:
        await update.message.reply_text(
            f"❌ Type galat hai: <code>{content_type}</code>\n\n"
            f"✅ Valid types: <code>{', '.join(valid_types)}</code>",
            parse_mode='HTML'
        )
        return

    # Delay (default 3 sec)
    delay = 3
    if len(context.args) > 2:
        try:
            delay = int(context.args[2])
            delay = max(2, min(delay, 30))  # 2 se 30 ke beech
        except ValueError:
            pass

    # --- Database Se Posts Nikalo ---
    conn = get_db_connection()
    if not conn:
        await update.message.reply_text("❌ Database error.")
        return

    cur = conn.cursor()

    if content_type == "all":
        cur.execute("""
            SELECT id, movie_id, caption, media_file_id,
                   media_type, keyboard_data, topic_id, content_type
            FROM channel_posts
            WHERE is_restored = FALSE OR is_restored IS NULL
            ORDER BY posted_at ASC
        """)
    else:
        cur.execute("""
            SELECT id, movie_id, caption, media_file_id,
                   media_type, keyboard_data, topic_id, content_type
            FROM channel_posts
            WHERE (is_restored = FALSE OR is_restored IS NULL)
              AND content_type = %s
            ORDER BY posted_at ASC
        """, (content_type,))

    posts = cur.fetchall()
    cur.close()
    close_db_connection(conn)

    if not posts:
        type_emoji = {
            'movies': '🎬', 'adult': '🔞',
            'series': '📺', 'anime': '🎌', 'all': '📦'
        }
        await update.message.reply_text(
            f"{type_emoji.get(content_type, '📦')} "
            f"<b>{content_type.upper()}</b> type ki koi bhi "
            f"post restore ke liye nahi mili.",
            parse_mode='HTML'
        )
        return

    total = len(posts)
    est_minutes = (total * delay) // 60

    status_msg = await update.message.reply_text(
        f"🔄 <b>Restore Starting...</b>\n\n"
        f"📦 Type: <code>{content_type.upper()}</code>\n"
        f"📊 Total Posts: <code>{total}</code>\n"
        f"⏱ Delay: <code>{delay}</code> seconds\n"
        f"⌛ Est. Time: ~<code>{est_minutes}</code> min\n\n"
        f"<i>Please wait, do not stop the bot...</i>",
        parse_mode='HTML'
    )

    success = 0
    failed  = 0
    skipped = 0

    bot_info = await context.bot.get_me()
    new_bot  = bot_info.username

    for idx, (post_id, movie_id, caption, media_file_id,
              media_type, keyboard_data_raw, topic_id, c_type) in enumerate(posts, 1):
        try:
            # 1. Keyboard Rebuild (Naye bot ke links ke saath)
            new_keyboard = None
            if keyboard_data_raw:
                try:
                    kd = (keyboard_data_raw
                          if isinstance(keyboard_data_raw, dict)
                          else json.loads(keyboard_data_raw))

                    rebuilt_rows = []
                    for row in kd.get("inline_keyboard", []):
                        new_row = []
                        for btn in row:
                            new_url = btn.get("url", "")
                            # Purane bot names replace karo
                            for old_b in [
                                "FlimfyBox_SearchBot",
                                "urmoviebot",
                                "FlimfyBox_Bot"
                            ]:
                                if old_b in new_url:
                                    new_url = new_url.replace(old_b, new_bot)
                            new_row.append(
                                InlineKeyboardButton(btn["text"], url=new_url)
                            )
                        rebuilt_rows.append(new_row)

                    if rebuilt_rows:
                        new_keyboard = InlineKeyboardMarkup(rebuilt_rows)
                except Exception as kb_err:
                    logger.warning(f"Keyboard error post {post_id}: {kb_err}")

            # 2. Post Bhejo
            sent = None
            extra = {}
            if topic_id and topic_id != 100:
                extra['message_thread_id'] = topic_id

            if media_type == "photo" and media_file_id:
                sent = await context.bot.send_photo(
                    chat_id      = new_channel_id,
                    photo        = media_file_id,
                    caption      = caption or "",
                    parse_mode   = 'Markdown',
                    reply_markup = new_keyboard,
                    **extra
                )
            elif media_type == "video" and media_file_id:
                sent = await context.bot.send_video(
                    chat_id      = new_channel_id,
                    video        = media_file_id,
                    caption      = caption or "",
                    parse_mode   = 'Markdown',
                    reply_markup = new_keyboard,
                    **extra
                )
            elif caption:
                sent = await context.bot.send_message(
                    chat_id      = new_channel_id,
                    text         = caption,
                    parse_mode   = 'Markdown',
                    reply_markup = new_keyboard,
                    **extra
                )
            else:
                skipped += 1
                continue

            # 3. DB Update
            if sent:
                conn2 = get_db_connection()
                if conn2:
                    try:
                        cur2 = conn2.cursor()
                        cur2.execute("""
                            UPDATE channel_posts
                            SET is_restored  = TRUE,
                                restored_at  = NOW(),
                                channel_id   = %s,
                                message_id   = %s,
                                bot_username = %s
                            WHERE id = %s
                        """, (new_channel_id, sent.message_id, new_bot, post_id))
                        conn2.commit()
                        cur2.close()
                    except Exception as db_e:
                        logger.error(f"DB update error: {db_e}")
                    finally:
                        close_db_connection(conn2)
                success += 1

            # 4. Progress (Har 10 posts pe update)
            if idx % 10 == 0 or idx == total:
                try:
                    await status_msg.edit_text(
                        f"🔄 <b>Restoring {content_type.upper()}...</b>\n\n"
                        f"📊 Progress: <code>{idx}/{total}</code>\n"
                        f"✅ Success:  <code>{success}</code>\n"
                        f"❌ Failed:   <code>{failed}</code>\n"
                        f"⏭ Skipped:  <code>{skipped}</code>",
                        parse_mode='HTML'
                    )
                except Exception:
                    pass

            # 5. Delay (Telegram Flood se bachao)
            await asyncio.sleep(delay)

        except RetryAfter as e:
            wait = e.retry_after + 5
            logger.warning(f"Rate limited! Waiting {wait}s")
            try:
                await status_msg.edit_text(
                    f"⏸ <b>Telegram ne slow kiya!</b>\n"
                    f"Waiting <code>{wait}</code> seconds...\n"
                    f"Progress: <code>{idx}/{total}</code>",
                    parse_mode='HTML'
                )
            except Exception:
                pass
            await asyncio.sleep(wait)

        except telegram.error.Forbidden:
            await status_msg.edit_text(
                f"❌ <b>Bot ko channel mein admin access nahi!</b>\n\n"
                f"Steps:\n"
                f"1. Channel open karo\n"
                f"2. Bot ko Admin banao\n"
                f"3. Dobara /restore karo"
            )
            return

        except Exception as e:
            failed += 1
            logger.error(f"Restore failed post {post_id}: {e}")
            await asyncio.sleep(1)

    # Final Report
    type_emoji = {
        'movies': '🎬', 'adult': '🔞',
        'series': '📺', 'anime': '🎌', 'all': '📦'
    }
    await status_msg.edit_text(
        f"🎉 <b>Restore Complete!</b>\n\n"
        f"{type_emoji.get(content_type,'📦')} Type: "
        f"<code>{content_type.upper()}</code>\n"
        f"📦 Total:   <code>{total}</code>\n"
        f"✅ Success: <code>{success}</code>\n"
        f"❌ Failed:  <code>{failed}</code>\n"
        f"⏭ Skipped: <code>{skipped}</code>\n\n"
        f"📢 New Channel: <code>{new_channel_id}</code>\n"
        f"🤖 Bot: @{new_bot}",
        parse_mode='HTML'
    )

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
            # ✅ IMPROVED: Show more helpful error message
            error_msg = str(context.error)
            if "too many values to unpack" in error_msg:
                await update.effective_message.reply_text(
                    "❌ Error:  Data format issue. Please try again.",
                    reply_markup=get_main_keyboard()
                )
            elif "unpacking" in error_msg:
                await update.effective_message.reply_text(
                    "❌ Error: Could not process your request. Please try again.",
                    reply_markup=get_main_keyboard()
                )
            else:
                await update.effective_message.reply_text(
                    "Sorry, something went wrong. Please try again later.",
                    reply_markup=get_main_keyboard()
                )
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")

# ==================== FLASK APP (Premium Edition) ====================

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import os
import logging
import json
import psycopg2
from datetime import datetime
import requests
from urllib.parse import quote
import random
import re
import secrets

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
flask_app = Flask(__name__)
CORS(flask_app, resources={r"/*": {"origins": "*"}})

# --- TMDB API Key (for fetching trailers & cast) ---
api_key = os.environ.get("TMDB_API_KEY")

# ==================== DATABASE HELPERS (use existing functions) ====================
# Make sure these functions are already defined in your main code:
# get_db_connection(), close_db_connection(), store_user_request()
# We'll assume they are available.

# ==================== API ROUTES ====================

# 👇 NAYA FIX: UptimeRobot ke liye Root URL (Taaki 404 na aaye) 👇
@flask_app.route('/', methods=['GET', 'HEAD'])
def home():
    return "Bot is Alive & Running!", 200
# 👆 NAYA FIX END 👆

@flask_app.route('/api/movies', methods=['GET'])
def get_movies():
    """
    Return list of movies with pagination (Infinite Scroll).
    """
    # Pagination Logic
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 40)) # Ek baar mein 40 movies bhejo
    offset = (page - 1) * limit

    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, year, poster_url, rating, genre, category,
                   COALESCE(language, '') as language
            FROM movies
            WHERE poster_url IS NOT NULL AND poster_url != ''
            ORDER BY id DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        
        rows = cur.fetchall()
        movies = []
        for r in rows:
            movies.append({
                'id': r[0],
                'title': r[1],
                'year': r[2] if r[2] else '',
                'image': r[3] if r[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
                'rating': r[4] if r[4] else 'N/A',
                'genre': r[5] if r[5] else 'Unknown',
                'category': r[6] if r[6] else 'Movie',
                'language': r[7]
            })
        cur.close()
        close_db_connection(conn)
        
        # Check if more movies exist
        has_more = len(movies) == limit 
        
        return jsonify({'status': 'success', 'movies': movies, 'has_more': has_more})
    except Exception as e:
        logger.error(f"Error in /api/movies: {e}")
        close_db_connection(conn)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@flask_app.route('/api/movie/<int:movie_id>', methods=['GET'])
def get_movie_details(movie_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, year, poster_url, rating, genre, description, category, language
            FROM movies WHERE id = %s
        """, (movie_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'status': 'error', 'message': 'Movie not found'}), 404

        movie = {
            'id': row[0],
            'title': row[1],
            'year': row[2] if row[2] else '',
            'image': row[3] if row[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
            'rating': row[4] if row[4] else 'N/A',
            'genre': row[5] if row[5] else 'Unknown',
            'description': row[6] if row[6] else 'No description available.',
            'category': row[7] if row[7] else 'Movie',
            'language': row[8] if row[8] else ''
        }

        # Get files
        cur.execute("SELECT quality, file_size FROM movie_files WHERE movie_id = %s", (movie_id,))
        files = [{'quality': f[0], 'size': f[1]} for f in cur.fetchall()]
        movie['files'] = files

        cur.close()
        close_db_connection(conn)

        # TMDB: include year to disambiguate
        try:
            # Build search query with title and year if present
            search_term = movie['title']
            if movie['year']:
                search_term += f" {movie['year']}"
            search_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(search_term)}"
            resp = requests.get(search_url, timeout=5).json()
            if resp.get('results'):
                first = resp['results'][0]
                media_type = first.get('media_type', 'movie')
                tmdb_id = first['id']

                # Backdrop
                backdrop_path = first.get('backdrop_path')
                if backdrop_path:
                    movie['backdrop'] = f"https://image.tmdb.org/t/p/w1280{backdrop_path}"
                else:
                    movie['backdrop'] = None

                # Cast
                credits_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/credits?api_key={TMDB_API_KEY}"
                credits = requests.get(credits_url, timeout=5).json()
                cast = credits.get('cast', [])[:5]
                movie['cast'] = [{'name': c['name'], 'character': c['character'], 'profile': f"https://image.tmdb.org/t/p/w185{c['profile_path']}" if c.get('profile_path') else None} for c in cast]

                # Trailer
                videos_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/videos?api_key={TMDB_API_KEY}"
                videos = requests.get(videos_url, timeout=5).json()
                trailer = next((v for v in videos.get('results', []) if v['type'] == 'Trailer' and v['site'] == 'YouTube'), None)
                if trailer:
                    movie['trailer_key'] = trailer['key']
        except Exception as e:
            logger.warning(f"TMDB fetch failed for {movie['title']}: {e}")
            movie['cast'] = []
            movie['trailer_key'] = None
            movie['backdrop'] = None

        return jsonify({'status': 'success', 'movie': movie})
    except Exception as e:
        logger.error(f"Error in /api/movie/{movie_id}: {e}")
        close_db_connection(conn)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@flask_app.route('/api/search', methods=['GET'])
def search_movies_api():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'status': 'error', 'message': 'Missing query'}), 400

    conn = get_db_connection()
    local_results = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, title, year, poster_url, rating, genre, category
                FROM movies
                WHERE title ILIKE %s OR title ILIKE %s
                LIMIT 20
            """, (f'%{query}%', f'%{query.replace(" ", "%")}%'))
            rows = cur.fetchall()
            for r in rows:
                local_results.append({
                    'id': r[0],
                    'title': r[1],
                    'year': r[2] if r[2] else '',
                    'image': r[3] if r[3] else 'https://via.placeholder.com/300x450?text=No+Poster',
                    'rating': r[4] if r[4] else 'N/A',
                    'genre': r[5] if r[5] else 'Unknown',
                    'category': r[6] if r[6] else 'Movie',
                    'source': 'local'
                })
            cur.close()
        except Exception as e:
            logger.error(f"Local search error: {e}")
        finally:
            close_db_connection(conn)

    tmdb_results = []
    if len(local_results) < 15:
        try:
            # Use original query (including year) – no stripping
            tmdb_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(query)}"
            resp = requests.get(tmdb_url, timeout=5).json()
            for item in resp.get('results', []):
                img_path = item.get('poster_path') or item.get('backdrop_path')
                if not img_path:
                    continue

                tmdb_results.append({
                    'id': 'tmdb_' + str(item['id']),
                    'title': item.get('title') or item.get('name') or 'Unknown',
                    'year': (item.get('release_date') or item.get('first_air_date') or '')[:4],
                    'image': f"https://image.tmdb.org/t/p/w500{img_path}",
                    'rating': round(item.get('vote_average', 0), 1),
                    'genre': 'Action, Drama',
                    'category': 'Movie' if item.get('media_type') == 'movie' else 'TV Series',
                    'source': 'tmdb',
                    'description': item.get('overview', '')
                })
        except Exception as e:
            logger.error(f"TMDB search error: {e}")

    # Deduplicate: normalize title (remove punctuation, spaces, lowercase)
    def normalize_title(t):
        return re.sub(r'[^\w\s]', '', t).lower().replace(" ", "")

    seen = set()
    combined = []
    # Local movies first
    for m in local_results:
        key = normalize_title(m['title'])
        if key not in seen:
            seen.add(key)
            combined.append(m)
    # Then TMDB movies (only if not already seen)
    for m in tmdb_results:
        key = normalize_title(m['title'])
        if key not in seen and len(combined) < 30:
            seen.add(key)
            combined.append(m)

    return jsonify({'status': 'success', 'results': combined})
    
    # 🔥 TMDB Fix for "Thamma 2025" -> API ko saal (year) se confusion hoti hai, isliye usko hata do
    clean_query = re.sub(r'\b(19|20)\d{2}\b', '', query).strip() 
    if not clean_query: 
        clean_query = query # Agar user ne sirf saal hi likh diya ho

    tmdb_results = []
    if len(local_results) < 15: # Agar local DB me kam movies mili tabhi TMDB me dhoondo
        try:
            tmdb_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(clean_query)}"
            resp = requests.get(tmdb_url, timeout=5).json()
            for item in resp.get('results', []):
                # Agar poster na ho, toh backdrop image utha lo (Thamma ke liye fix)
                img_path = item.get('poster_path') or item.get('backdrop_path')
                if not img_path:
                    continue
                
                tmdb_results.append({
                    'id': 'tmdb_' + str(item['id']),
                    'title': item.get('title') or item.get('name') or 'Unknown',
                    'year': (item.get('release_date') or item.get('first_air_date') or '')[:4],
                    'image': f"https://image.tmdb.org/t/p/w500{img_path}",
                    'rating': round(item.get('vote_average', 0), 1),
                    'genre': 'Action, Drama',
                    'category': 'Movie' if item.get('media_type') == 'movie' else 'TV Series',
                    'source': 'tmdb',
                    'description': item.get('overview', '')
                })
        except Exception as e:
            logger.error(f"TMDB search error: {e}")
    
    # 🔥 FIX: "Avengers: Endgame" aur "Avengers Endgame" ko ek hi movie manega
    def normalize_title(t):
        return re.sub(r'[^\w\s]', '', t).lower().replace(" ", "")

    seen_titles = set()
    combined = []
    
    # Pehle Local movies daalo (Taaki unpar Download ka button aaye)
    for m in local_results:
        key = normalize_title(m['title'])
        if key not in seen_titles:
            seen_titles.add(key)
            combined.append(m)
            
    # Phir TMDB movies daalo (Jo duplicate nahi hain)
    for m in tmdb_results:
        key = normalize_title(m['title'])
        if key not in seen_titles and len(combined) < 30:
            seen_titles.add(key)
            combined.append(m)
            
    return jsonify({'status': 'success', 'results': combined})


@flask_app.route('/api/request', methods=['POST'])
def request_movie_api():
    """
    Store a user request from web app AND Notify Admin.
    """
    data = request.get_json()
    if not data or 'title' not in data:
        return jsonify({'status': 'error', 'message': 'Missing movie title'}), 400
    
    title = data['title'][:200]
    user_id = data.get('user_id', 0)
    username = data.get('username', '')
    first_name = data.get('first_name', 'WebApp User')
    
    success = store_user_request(user_id, username, first_name, title, None, None)
    
    if success:
        # 🔥 FIX: Web App se aayi request ko turant Admin Channel me send karein
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        request_channel = os.environ.get('REQUEST_CHANNEL_ID')
        
        if bot_token and request_channel:
            try:
                # Beautiful Admin Notification Format
                msg_text = (
                    f"🎬 <b>New WebApp Request!</b> 🎬\n\n"
                    f"Movie: <b>{title}</b>\n"
                    f"User: {first_name} (<code>{user_id}</code>)\n"
                )
                if username:
                    msg_text += f"Username: @{username}\n"
                msg_text += f"From: 🌐 Web Portal"

                # Inline Buttons for Admin
                short_title = title[:15].replace('_', ' ')
                reply_markup = {
                    "inline_keyboard": [
                        [{"text": "✅ Movie Add Kar Di Gai Hai", "callback_data": f"reqA_{user_id}_{short_title}"}],
                        [{"text": "❌ Nahi Mili", "callback_data": f"reqN_{user_id}_{short_title}"}]
                    ]
                }
                
                # Direct Telegram API Call
                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                payload = {
                    "chat_id": request_channel,
                    "text": msg_text,
                    "parse_mode": "HTML",
                    "reply_markup": reply_markup
                }
                requests.post(url, json=payload, timeout=5)
            except Exception as e:
                logger.error(f"Failed to notify admin from WebApp: {e}")

        return jsonify({'status': 'success', 'message': 'Request saved & Admin Notified'})
    else:
        return jsonify({'status': 'error', 'message': 'Could not save request'}), 500

# 🤖 GOOGLE AUTO-SUGGEST PROXY (Spelling Fixer)
@flask_app.route('/api/suggest', methods=['GET'])
def get_suggestions():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify([])
    try:
        # Firefox client wali API direct JSON list deti hai, jo use karne me aasan hai
        url = f"https://suggestqueries.google.com/complete/search?client=firefox&q={quote(q + ' movie')}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        resp = requests.get(url, headers=headers, timeout=3).json()
        
        # resp ka format: ["query", ["suggestion1", "suggestion2", ...]]
        suggestions = resp[1] if len(resp) > 1 else []
        
        # 'movie' word hata kar clean naam nikalna aur top 6 suggestions dikhana
        clean_suggs = [s.replace(' movie', '').title() for s in suggestions][:6] 
        return jsonify(clean_suggs)
    except Exception as e:
        logger.error(f"Suggest API Error: {e}")
        return jsonify([])

# ==================== MAIN WEB APP PAGE (Premium HTML) ====================

# 🛡️ MIDDLEMAN REDIRECT PAGE (Anti-Bot)
@flask_app.route('/watch/<int:movie_id>')
def secure_watch(movie_id):
    # Yeh HTML page user ko dikhega. Bots JS run nahi kar pate.
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>FlimfyBox - Verifying Secure Connection...</title>
        <style>
            body { background: #09090b; color: white; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; font-family: sans-serif; }
            .loader { border: 4px solid rgba(255,255,255,0.1); border-top: 4px solid #f43f5e; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin-bottom: 20px; }
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    </head>
    <body>
        <div class="loader"></div>
        <h3>Securely verifying your connection...</h3>
        <p style="color: #a1a1aa; font-size: 13px;">Please wait 2 seconds. You will be redirected automatically.</p>
        
        <script>
            // Invisible JS Challenge
            setTimeout(() => {
                fetch('/api/gen_link/""" + str(movie_id) + """', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    if(data.url) {
                        window.location.href = data.url; 
                    } else {
                        document.body.innerHTML = "<h3>❌ Server Error. Please try again.</h3>";
                    }
                }).catch(e => {
                    document.body.innerHTML = "<h3>❌ Connection failed.</h3>";
                });
            }, 1500); 
        </script>
    </body>
    </html>
    """
    return html

# 🔐 SECRET LINK GENERATOR API (Auto Delete Logic)
@flask_app.route('/api/gen_link/<int:movie_id>', methods=['POST'])
def gen_secure_link(movie_id):
    token = "tmp_" + secrets.token_hex(6)
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # Delete old tokens (1 minute se purane)
            cur.execute("DELETE FROM temp_links WHERE created_at < NOW() - INTERVAL '1 minute'")
            # Save new token
            cur.execute("INSERT INTO temp_links (token, movie_id) VALUES (%s, %s)", (token, movie_id))
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"Token Error: {e}")
        finally:
            close_db_connection(conn)
            
    bot_username = os.environ.get('BOT_USERNAME', 'FlimfyBox_Bot')
    tg_url = f"tg://resolve?domain={bot_username}&start={token}"
    return jsonify({"url": tg_url})

@flask_app.route('/webapp')
def serve_mini_app():
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>FlimfyBox · PREMIUM</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Inter', sans-serif; user-select: none; -webkit-tap-highlight-color: transparent; }
        :root {
            --bg: #0f0f0f;
            --surface: #1a1a1a;
            --surface-light: #2a2a2a;
            --primary: #e50914;       /* Netflix red */
            --primary-soft: #b20710;
            --primary-glow: rgba(229, 9, 20, 0.4);
            --text: #ffffff;
            --text-muted: #a0a0a0;
            --border: rgba(229, 9, 20, 0.25);
            --shadow: 0 10px 30px -10px black;
            --shadow-glow: 0 8px 30px var(--primary-glow);
        }
        body { background: var(--bg); color: var(--text); overflow-x: hidden; padding-bottom: 40px; }
        /* Header */
        header {
            display: flex; justify-content: space-between; align-items: center;
            padding: 16px 24px; background: rgba(15,15,15,0.9); backdrop-filter: blur(20px);
            border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 100;
        }
        .logo { font-size: 28px; font-weight: 800; background: linear-gradient(135deg, #fff, #e50914); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .crown-icon { color: var(--primary); font-size: 24px; filter: drop-shadow(0 0 8px var(--primary)); }
        /* Search */
        .search-section { padding: 20px 24px; }
        .search-box {
            display: flex; align-items: center; background: var(--surface); border-radius: 40px;
            padding: 14px 24px; border: 1px solid var(--border); transition: 0.3s;
            box-shadow: var(--shadow);
        }
        .search-box:focus-within { border-color: var(--primary); box-shadow: var(--shadow-glow); }
        .search-box i { color: var(--primary); margin-right: 12px; font-size: 18px; opacity: 0.8; }
        .search-box input {
            flex: 1; background: transparent; border: none; color: white; outline: none;
            font-size: 16px; font-weight: 400;
        }
        .search-box input::placeholder { color: #5a5a5a; }
        /* Genre pills */
        .genre-scroll {
            display: flex; overflow-x: auto; gap: 12px; padding: 0 24px 20px 24px;
            scrollbar-width: none; scroll-behavior: smooth;
        }
        .genre-scroll::-webkit-scrollbar { display: none; }
        .genre-pill {
            background: var(--surface); border: 1px solid var(--border); padding: 8px 24px;
            border-radius: 40px; font-size: 14px; font-weight: 600; color: var(--text-muted);
            white-space: nowrap; cursor: pointer; transition: 0.2s;
        }
        .genre-pill.active { background: var(--primary); color: #0f0f0f; border-color: var(--primary); box-shadow: 0 0 20px var(--primary); }
        /* Hero slider */
        .hero-slider {
            width: 100%; height: 380px; position: relative; background-size: cover;
            background-position: top 20% center; transition: background-image 0.6s ease;
            border-radius: 0 0 30px 30px; box-shadow: 0 30px 40px -20px black;
            margin-bottom: 10px;
        }
        .hero-overlay {
            position: absolute; inset: 0; background: linear-gradient(to top, var(--bg) 0%, transparent 70%);
            display: flex; align-items: flex-end; padding: 30px 24px;
        }
        .hero-info h2 {
            font-size: 40px; font-weight: 800; text-shadow: 0 4px 20px black; line-height: 1.1;
            background: linear-gradient(180deg, #fff, #ffb3b3); -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }
        .hero-info span {
            font-size: 13px; font-weight: 600; color: var(--primary); background: rgba(0,0,0,0.5);
            backdrop-filter: blur(10px); padding: 5px 16px; border-radius: 40px; display: inline-block;
            border: 1px solid rgba(229,9,20,0.3);
        }
        /* Row header with scroll buttons */
        .row-header {
            display: flex; justify-content: space-between; align-items: center;
            padding: 28px 24px 12px 24px; font-size: 20px; font-weight: 700;
        }
        .row-header-left { display: flex; align-items: center; gap: 12px; }
        .row-header i { color: var(--primary); font-size: 20px; }
        .scroll-buttons { display: flex; gap: 10px; }
        .scroll-btn {
            width: 36px; height: 36px; border-radius: 50%; background: var(--surface);
            border: 1px solid var(--border); color: var(--primary); cursor: pointer;
            display: flex; align-items: center; justify-content: center; transition: 0.2s;
        }
        .scroll-btn:active { background: var(--primary); color: black; }
        /* Horizontal scroll */
        .horizontal-scroll {
            display: flex; overflow-x: auto; gap: 16px; padding: 0 24px 10px 24px;
            scroll-snap-type: x mandatory; scrollbar-width: none; scroll-behavior: smooth;
        }
        .horizontal-scroll::-webkit-scrollbar { display: none; }
        /* Card styles */
        .card {
            flex: 0 0 150px; position: relative; cursor: pointer; transition: transform 0.15s;
            scroll-snap-align: start; border-radius: 16px; overflow: hidden;
        }
        .card:active { transform: scale(0.96); }
        .card-img {
            width: 100%; aspect-ratio: 2/3; object-fit: cover; border-radius: 16px;
            box-shadow: 0 10px 20px -5px black; background: var(--surface);
        }
        .card-rating {
            position: absolute; top: 8px; right: 8px; background: rgba(0,0,0,0.7);
            backdrop-filter: blur(4px); font-size: 12px; font-weight: 700; padding: 4px 8px;
            border-radius: 40px; color: var(--primary); border: 1px solid rgba(229,9,20,0.3);
        }
        .card-title {
            margin-top: 8px; font-size: 14px; font-weight: 600; white-space: nowrap;
            overflow: hidden; text-overflow: ellipsis; padding: 0 4px;
        }
        .card-meta { font-size: 12px; color: var(--text-muted); padding: 0 4px; }
        /* Movie Grid Container */
        .movie-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr); /* Ek row mein 2 movies */
            gap: 16px; /* Cards ke beech ka gap */
            padding: 0 24px 20px 24px; /* Side margins ko theek rakhne ke liye */
        }
        
        /* Movie Grid Container - Fixed for 2 Columns */
        .movie-grid {
            display: grid;
            grid-template-columns: 1fr 1fr; /* Exactly 2 columns */
            gap: 16px;
            padding: 0 24px 24px 24px;
            width: 100%;
        }
        
        /* Grid card (for search results / genre view) */
        .grid-card {
            width: 100%;
            display: flex;
            flex-direction: column;
            position: relative; 
            cursor: pointer; 
            transition: transform 0.15s;
            border-radius: 16px; 
            overflow: hidden;
        }
        .grid-card:active { transform: scale(0.96); }
        .grid-card .card-img { 
            width: 100%; 
            aspect-ratio: 2/3; 
            object-fit: cover; 
            border-radius: 16px; 
        }
        /* Skeleton loader */
        .skeleton {
            background: linear-gradient(90deg, var(--surface) 25%, var(--surface-light) 50%, var(--surface) 75%);
            background-size: 200% 100%; animation: shimmer 1.2s infinite; border-radius: 16px;
        }
        @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
        .skeleton-card {
            flex: 0 0 150px; height: 250px; border-radius: 16px; background: var(--surface);
        }
        /* Details page - Netflix style backdrop + floating poster */
        .details-page {
            position: fixed; inset: 0; background: var(--bg); z-index: 2000; overflow-y: auto;
            transform: translateX(100%); transition: transform 0.4s cubic-bezier(0.2,0.8,0.2,1);
        }
        .details-page.open { transform: translateX(0); }
        .dp-header {
            position: absolute; top: 20px; left: 20px; z-index: 2010;
        }
        .btn-back {
            background: rgba(20,20,20,0.7); backdrop-filter: blur(16px); border: 1px solid var(--border);
            color: white; width: 48px; height: 48px; border-radius: 50%; font-size: 20px;
            display: flex; align-items: center; justify-content: center; cursor: pointer;
            transition: 0.2s; box-shadow: 0 8px 20px rgba(0,0,0,0.6);
        }
        .btn-back:active { transform: scale(0.9); border-color: var(--primary); }
        .dp-backdrop {
            position: relative;
            width: 100%;
            height: 50vh;
            min-height: 320px;
            background-size: cover;
            background-position: top center; /* 👉 'center 20%' ki jagah 'top center' karein taaki landscape image ka main hissa dikhe */
            overflow: hidden;
        }
        .dp-backdrop::after {
            content: '';
            position: absolute;
            inset: 0;
            background: linear-gradient(to top, var(--bg) 0%, rgba(0,0,0,0.6) 70%, transparent 100%);
        }
        .dp-poster-float {
            position: absolute;
            bottom: -80px;
            left: 28px;
            z-index: 2015;
            width: 140px;
            border-radius: 12px;
            box-shadow: 0 20px 40px -10px black;
            border: 2px solid rgba(255,255,255,0.2);
        }
        .dp-poster-float img {
            width: 100%;
            aspect-ratio: 2 / 3; /* 👉 Yeh poster ko hamesha LAMBA (Vertical) banayega */
            object-fit: cover;   /* 👉 Yeh image ko pichakne nahi dega, perfectly crop karega */
            border-radius: 12px;
            display: block;
            box-shadow: 0 10px 20px rgba(0,0,0,0.8); /* Poster ko thoda background se alag dikhane ke liye */
        }
        .dp-info {
            padding: 28px;
            margin-top: 40px; /* Ise thoda kam kar dein */
            position: relative;
            z-index: 2005;
        }
        
        .dp-title {
            font-size: 32px;
            font-weight: 800;
            line-height: 1.2;
            margin-bottom: 16px;
            text-shadow: 0 4px 20px black;
            margin-left: 155px; /* 👈 Title ko chote poster ke right side dhakel dega */
            min-height: 80px;   /* 👈 Poster ki height ko balance karne ke liye */
        }
        .rich-info-box {
            background: rgba(20,20,20,0.7);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border);
            border-radius: 28px;
            padding: 24px;
            margin-bottom: 28px;
            box-shadow: 0 30px 50px -20px black;
        }
        .rich-info-box > div {
            margin-bottom: 14px;
            font-size: 15px;
            display: flex;
            align-items: baseline;
            gap: 12px;
            flex-wrap: wrap;
        }
        .rich-info-box span { color: var(--text-muted); font-weight: 500; min-width: 80px; }
        .rich-info-box label { font-weight: 700; color: var(--primary); }
        .rich-desc { margin-top: 18px; font-size: 14px; color: var(--text-muted); line-height: 1.7; border-top: 1px dashed var(--border); padding-top: 18px; }
        /* Cast */
        .cast-list { display: flex; gap: 15px; overflow-x: auto; padding: 10px 0 20px; }
        .cast-item { flex: 0 0 80px; text-align: center; }
        .cast-item img { width: 70px; height: 70px; border-radius: 50%; object-fit: cover; border: 2px solid var(--primary); }
        .cast-item span { display: block; font-size: 12px; margin-top: 5px; color: var(--text-muted); }
        /* Buttons */
        .btn-trailer {
            width: 100%; padding: 18px; border-radius: 60px; border: none; font-size: 17px;
            font-weight: 800; display: flex; align-items: center; justify-content: center; gap: 12px;
            cursor: pointer; background: linear-gradient(145deg, var(--primary), var(--primary-soft)); color: white;
            box-shadow: 0 15px 30px -5px var(--primary-glow); margin-bottom: 28px; transition: 0.2s;
        }
        .btn-trailer:active { transform: scale(0.97); box-shadow: 0 8px 20px var(--primary); }
        .btn-request {
            width: 100%; padding: 18px; border-radius: 60px; border: 1px solid var(--border);
            font-size: 17px; font-weight: 800; display: flex; align-items: center; justify-content: center;
            gap: 12px; cursor: pointer; background: var(--surface); color: var(--primary); margin-bottom: 20px;
        }
        .dl-section { margin-top: 25px; }
        .dl-heading { font-size: 13px; font-weight: 800; color: var(--primary); text-align: center; margin-bottom: 20px; letter-spacing: 2px; opacity: 0.9; }
        .dl-btn {
            width: 100%; background: var(--surface); border: 1px solid var(--border); border-radius: 20px;
            padding: 16px 20px; margin-bottom: 12px; display: flex; justify-content: space-between;
            align-items: center; color: white; font-size: 15px; font-weight: 600; cursor: pointer;
            transition: 0.2s; box-shadow: 0 6px 14px rgba(0,0,0,0.4);
        }
        .dl-btn:active { border-color: var(--primary); transform: scale(0.98); background: var(--surface-light); }
        .quality-text { display: flex; align-items: center; gap: 12px; }
        .file-size { color: var(--text-muted); font-size: 13px; font-weight: 500; }
        .action {
            color: white; font-size: 13px; font-weight: 800; background: var(--primary);
            padding: 8px 18px; border-radius: 40px; box-shadow: 0 0 15px var(--primary-glow);
        }
        /* Trailer modal */
        .trailer-modal {
            display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.95); z-index: 3000;
            align-items: center; justify-content: center; flex-direction: column; padding: 20px;
            backdrop-filter: blur(20px);
        }
        .trailer-modal.active { display: flex; }
        .trailer-wrapper {
            width: 100%; max-width: 900px; background: black; border-radius: 24px; overflow: hidden;
            border: 2px solid var(--primary); box-shadow: 0 0 40px var(--primary-glow);
        }
        .trailer-modal iframe { width: 100%; aspect-ratio: 16/9; border: none; display: block; }
        .close-trailer-btn {
            margin-top: 25px; background: rgba(255,255,255,0.05); border: 1px solid var(--primary); color: var(--primary);
            padding: 14px 40px; border-radius: 60px; font-weight: 700; font-size: 16px; cursor: pointer;
            transition: 0.2s;
        }
        .close-trailer-btn:active { background: var(--primary); color: black; }
        /* Toast */
        .toast {
            position: fixed; bottom: -60px; left: 50%; transform: translateX(-50%);
            background: rgba(20,20,20,0.9); backdrop-filter: blur(20px); border: 1px solid var(--primary);
            color: var(--primary); padding: 14px 30px; border-radius: 60px; font-size: 15px; font-weight: 700;
            transition: bottom 0.3s; z-index: 3000; box-shadow: 0 10px 30px var(--primary-glow); white-space: nowrap;
        }
        .toast.show { bottom: 30px; }
        .loader { text-align: center; padding: 40px; color: var(--primary); font-size: 16px; }
    </style>
</head>
<body>
    <header>
        <div class="logo">FlimfyBox</div>
        <i class="fas fa-crown crown-icon"></i>
    </header>
    <div class="search-section">
        <div class="search-box">
            <i class="fas fa-search"></i>
            <input type="text" id="searchInput" placeholder="Search movies, web series...">
        </div>
    </div>
    <div class="genre-scroll" id="genreContainer"></div>

    <div id="mainContent">
        <div class="hero-slider" id="heroSlider">
            <div class="hero-overlay">
                <div class="hero-info">
                    <h2 id="heroTitle">Loading...</h2>
                    <span id="heroMeta">✨ Premium Collection</span>
                </div>
            </div>
        </div>

        <div class="movie-row" id="rowTrending">
            <div class="row-header">
                <div class="row-header-left"><i class="fas fa-fire"></i> Trending Now</div>
                <div class="scroll-buttons">
                    <div class="scroll-btn" onclick="scrollRow('trendingScroll', -400)"><i class="fas fa-chevron-left"></i></div>
                    <div class="scroll-btn" onclick="scrollRow('trendingScroll', 400)"><i class="fas fa-chevron-right"></i></div>
                </div>
            </div>
            <div class="horizontal-scroll" id="trendingScroll"></div>
        </div>

        <div class="movie-row" id="rowBollywood">
            <div class="row-header">
                <div class="row-header-left"><i class="fas fa-film"></i> Bollywood Gold</div>
                <div class="scroll-buttons">
                    <div class="scroll-btn" onclick="scrollRow('bollywoodScroll', -400)"><i class="fas fa-chevron-left"></i></div>
                    <div class="scroll-btn" onclick="scrollRow('bollywoodScroll', 400)"><i class="fas fa-chevron-right"></i></div>
                </div>
            </div>
            <div class="horizontal-scroll" id="bollywoodScroll"></div>
        </div>

        <div class="movie-row" id="rowHollywood">
            <div class="row-header">
                <div class="row-header-left"><i class="fas fa-globe"></i> Hollywood Hits</div>
                <div class="scroll-buttons">
                    <div class="scroll-btn" onclick="scrollRow('hollywoodScroll', -400)"><i class="fas fa-chevron-left"></i></div>
                    <div class="scroll-btn" onclick="scrollRow('hollywoodScroll', 400)"><i class="fas fa-chevron-right"></i></div>
                </div>
            </div>
            <div class="horizontal-scroll" id="hollywoodScroll"></div>
        </div>

        <div style="margin-top: 40px;">
            <div class="row-header"><i class="fas fa-layer-group"></i> Explore All</div>
            <div class="movie-grid" id="moreGrid"></div>
        </div>
    </div>

    <div id="searchResultsContent" style="display: none;">
        <div class="row-header" id="searchHeader"><i class="fas fa-search"></i> Search Results</div>
        <div class="movie-grid" id="searchGrid"></div>
    </div>

    <div class="details-page" id="detailsPage">
        <div class="dp-header">
            <button class="btn-back" onclick="closeDetails()"><i class="fas fa-chevron-left"></i></button>
        </div>
        <div class="dp-layout">
            <div class="dp-backdrop" id="dpBackdrop"></div>
            <div class="dp-poster-float" id="dpPosterFloat">
                <img id="dpFloatPoster" src="" alt="Poster">
            </div>
            <div class="dp-info">
                <h1 class="dp-title" id="dpTitle">Title</h1>
                <div class="rich-info-box">
                    <div><span>IMDb</span> <label id="dpRating" style="color:var(--primary);">—</label></div>
                    <div><span>Genre</span> <label id="dpGenre">—</label></div>
                    <div><span>Cast</span> <label id="dpActors">Fetching...</label></div>
                    <div><span>Audio</span> <label>Dual Audio [Hindi & English] + Subs</label></div>
                    <div class="rich-desc" id="dpDesc">Loading story...</div>
                </div>
                <div id="castSection" class="cast-list"></div>
                <div id="dpTrailerBtn"></div>
                <div class="dl-section" id="dpLinks"></div>
            </div>
        </div>
    </div>

    <div class="trailer-modal" id="trailerModal">
        <div class="trailer-wrapper">
            <iframe id="trailerIframe" src="" allow="autoplay; encrypted-media; picture-in-picture" allowfullscreen></iframe>
        </div>
        <button class="close-trailer-btn" onclick="closeTrailer()"><i class="fas fa-times"></i> Close</button>
    </div>

    <div class="toast" id="toast">✅ Done!</div>

    <script>
        const tg = window.Telegram.WebApp;
        tg.expand();
        tg.ready();
        const BOT_USERNAME = "FlimfyBox_Bot"; // change if needed

        // State
        let allMovies = [];
        let tmdbMoviesMap = {};

        // Utility
        function showToast(msg) {
            const t = document.getElementById('toast');
            t.innerText = msg;
            t.classList.add('show');
            setTimeout(() => t.classList.remove('show'), 2500);
        }

        function scrollRow(elementId, amount) {
            const el = document.getElementById(elementId);
            if (el) el.scrollBy({ left: amount, behavior: 'smooth' });
        }

        // Pagination State
        let currentPage = 1;
        let isFetching = false;
        let hasMoreMovies = true;

        // Load movies from API with Infinite Scroll support
        async function loadMovies(page = 1) {
            if (isFetching || !hasMoreMovies) return;
            isFetching = true;

            try {
                // Agar page 1 se zyada hai, toh neeche ek loading spinner dikhao
                if (page > 1) {
                    document.getElementById('moreGrid').insertAdjacentHTML('beforeend', '<div id="scrollLoader" style="grid-column: 1 / -1; text-align: center; padding: 20px;"><div class="loader" style="width:30px;height:30px;border-width:3px;margin:0 auto;"></div></div>');
                }

                const res = await fetch(`/api/movies?page=${page}&limit=40`);
                const data = await res.json();
                
                // Naya data aate hi loader hata do
                if (page > 1) {
                    const loader = document.getElementById('scrollLoader');
                    if (loader) loader.remove();
                }

                if (data.status === 'success') {
                    const newMovies = data.movies.filter(m => m.image);
                    hasMoreMovies = data.has_more; 
                    
                    if (page === 1) {
                        // Pehli baar: Pura UI setup karo
                        allMovies = newMovies;
                        renderHome(allMovies); 
                        renderGenrePills(allMovies);
                    } else {
                        // Scrolling par: Purani movies mein nayi jod do
                        allMovies = [...allMovies, ...newMovies]; 
                        const newCardsHTML = renderCards(newMovies, 'grid-card', false);
                        document.getElementById('moreGrid').insertAdjacentHTML('beforeend', newCardsHTML);
                    }
                    currentPage++; // Agli baar ke liye page badha do
                } else {
                    console.error('API error:', data.message);
                }
            } catch (e) {
                console.error('Fetch failed', e);
            } finally {
                isFetching = false;
            }
        }

        // 🔥 NAYA: Infinite Scroll Listener
        window.addEventListener('scroll', () => {
            // Agar user page ke bottom se 600px upar hai, toh advance mein next page load kar lo
            if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 600) {
                // Check karo ki normal page open hai (Search result open na ho)
                if (document.getElementById('searchResultsContent').style.display === 'none') {
                    loadMovies(currentPage);
                }
            }
        });

        function renderGenrePills(movies) {
            const genreSet = new Set();
            movies.forEach(m => {
                if (m.genre && m.genre !== 'Unknown') {
                    m.genre.split(',').forEach(g => genreSet.add(g.trim()));
                }
            });
            const genres = ['All', ...Array.from(genreSet).slice(0, 10)];
            const container = document.getElementById('genreContainer');
            container.innerHTML = genres.map(g => `<div class="genre-pill ${g==='All'?'active':''}" onclick="filterByGenre('${g}', this)">${g}</div>`).join('');
        }

        window.filterByGenre = function(genre, el) {
            document.querySelectorAll('.genre-pill').forEach(p => p.classList.remove('active'));
            el.classList.add('active');
            if (genre === 'All') {
                renderHome(allMovies);
            } else {
                const filtered = allMovies.filter(m => m.genre && m.genre.includes(genre));
                // hide rows and show only grid with filtered
                document.querySelectorAll('.movie-row').forEach(r => r.style.display = 'none');
                document.getElementById('moreGrid').innerHTML = renderCards(filtered, 'grid-card', false);
            }
        };

        function renderHome(movies) {
            // Hero slider
            if (movies.length > 0) {
                let idx = 0;
                const top5 = movies.slice(0, 5);
                const updateHero = () => {
                    const m = top5[idx];
                    document.getElementById('heroSlider').style.backgroundImage = `url(${m.image})`;
                    document.getElementById('heroTitle').innerText = m.title;
                    document.getElementById('heroMeta').innerText = `${m.year} • ${m.category}`;
                    idx = (idx + 1) % top5.length;
                };
                updateHero();
                setInterval(updateHero, 5000);
            }

            // Trending (first 15)
            document.getElementById('trendingScroll').innerHTML = renderCards(movies.slice(0, 15), 'card', false);
            // Bollywood
            const bolly = movies.filter(m => m.category?.toLowerCase().includes('bollywood')).slice(0, 15);
            document.getElementById('bollywoodScroll').innerHTML = renderCards(bolly, 'card', false);
            // Hollywood
            const holy = movies.filter(m => m.category?.toLowerCase().includes('hollywood')).slice(0, 15);
            document.getElementById('hollywoodScroll').innerHTML = renderCards(holy, 'card', false);
            // Show remaining movies from the first batch
            document.getElementById('moreGrid').innerHTML = renderCards(movies.slice(15), 'grid-card', false);
        }

        function renderCards(movies, cardClass = 'card', forceTMDB = false) {
            if (!movies.length) return '<div style="color:var(--text-muted); padding:10px;">No movies</div>';
            return movies.map(m => {
                // 🔥 FIX: Automatically detect karega ki poster TMDB (Request) ka hai ya Local DB ka
                const isTMDB = forceTMDB || m.source === 'tmdb'; 
                const rating = m.rating && m.rating !== 'N/A' ? `⭐ ${m.rating}` : '';
                const badge = isTMDB ? '<div class="card-rating" style="color:white; background:var(--primary);">Request</div>' : (rating ? `<div class="card-rating">${rating}</div>` : '');
                
                return `
                    <div class="${cardClass}" onclick="openDetails('${m.id}', ${isTMDB})">
                        <img src="${m.image}" class="card-img" loading="lazy" onerror="this.src='https://via.placeholder.com/300x450?text=No+Poster'">
                        ${badge}
                        <div class="card-title">${m.title}</div>
                        <div class="card-meta">${m.year || ''}</div>
                    </div>
                `;
            }).join('');
        }

        // Search
let searchTimeout;
document.getElementById('searchInput').addEventListener('input', (e) => {
    clearTimeout(searchTimeout);
    const q = e.target.value.trim();
    const main = document.getElementById('mainContent');
    const searchRes = document.getElementById('searchResultsContent');
    const genreCont = document.getElementById('genreContainer');
    
    if (!q) {
        main.style.display = 'block';
        genreCont.style.display = 'flex';
        searchRes.style.display = 'none';
        return;
    }
    
    main.style.display = 'none';
    genreCont.style.display = 'none';
    searchRes.style.display = 'block';
    document.getElementById('searchGrid').innerHTML = '<div class="loader">Searching...</div>';

    searchTimeout = setTimeout(async () => {
        try {
            const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
            const data = await res.json();
            
            if (data.status === 'success' && data.results.length > 0) {
                const results = data.results;
                const local = results.filter(r => r.source === 'local');
                const tmdb = results.filter(r => r.source === 'tmdb');
                
                // 🔥 FIX: Local Search me aayi movies ko main array me save karo (Taaki unpar click ho sake)
                local.forEach(l => {
                    if (!allMovies.find(m => m.id == l.id)) {
                        allMovies.push(l);
                    }
                });

                tmdb.forEach(t => tmdbMoviesMap[t.id] = t);
                document.getElementById('searchHeader').innerHTML = `<i class="fas fa-search"></i> Found ${results.length} results`;
                document.getElementById('searchGrid').innerHTML = renderCards(results, 'grid-card', false);
            } else {
                const term = q;
                const searchHeader = document.getElementById('searchHeader');
                const searchGrid = document.getElementById('searchGrid');
                
                searchHeader.innerHTML = `<i class="fas fa-exclamation-circle" style="color:#ef4444;"></i> Not Found`;
                searchGrid.innerHTML = '<div class="loader">Checking spelling...</div>';
                
                // 🔥 FRONTEND JUGAD: Direct user ke mobile se Google ko call (No Server Block!)
                const script = document.createElement('script');
                window.googleSuggestCb = function(data) {
                    let suggs = data[1] || [];
                    // 'movie' word hata kar clean title banana
                    suggs = suggs.map(s => s.replace(/ movie$/i, '').replace(/\b\w/g, c => c.toUpperCase())).slice(0, 6);
                    
                    if(suggs.length > 0) {
                        let buttonsHtml = suggs.map(s => 
                            `<div onclick="document.getElementById('searchInput').value='${s}'; document.getElementById('searchInput').dispatchEvent(new Event('input'));" 
                            style="padding: 12px 15px; border-bottom: 1px solid rgba(255,255,255,0.05); color: white; cursor: pointer; display: flex; align-items: center; gap: 12px; transition: background 0.2s;"
                            onmouseover="this.style.background='rgba(229,9,20,0.1)'" onmouseout="this.style.background='transparent'">
                                <i class="fas fa-search" style="color: var(--text-muted); font-size: 14px;"></i> 
                                <span style="font-weight: 500;">${s}</span>
                            </div>`
                        ).join('');
                        
                        searchGrid.innerHTML = `
                            <div style="grid-column: 1 / -1; padding: 15px; background: var(--surface); border-radius: 16px; border: 1px solid var(--border); box-shadow: 0 10px 25px rgba(0,0,0,0.5);">
                                <p style="color: var(--primary); margin-bottom: 10px; font-size: 13px; font-weight: bold; padding-left: 10px;">✨ DID YOU MEAN:</p>
                                <div style="background: rgba(0,0,0,0.2); border-radius: 12px; overflow: hidden;">
                                    ${buttonsHtml}
                                </div>
                                <div style="margin-top: 15px; padding: 0 10px;">
                                    <button onclick="requestSilent('${term}')" style="background: #27272a; color: var(--text-muted); border: 1px solid rgba(255,255,255,0.1); padding: 12px; border-radius: 30px; font-size: 13px; cursor: pointer; width: 100%;">
                                        <i class="fas fa-paper-plane"></i> No, Request "${term}" Anyway
                                    </button>
                                </div>
                            </div>
                        `;
                    } else {
                        showFallbackUI(term);
                    }
                    
                    // Script ka kaam khatam, safai kar do
                    document.head.removeChild(script);
                    delete window.googleSuggestCb;
                };
                
                script.onerror = function() {
                    showFallbackUI(term);
                };
                
                function showFallbackUI(term) {
                    searchGrid.innerHTML = `
                        <div style="grid-column: 1 / -1; text-align: center; padding: 30px 20px; background: var(--surface); border-radius: 16px;">
                            <p style="color: var(--text-muted); margin-bottom: 20px;">We couldn't find "${term}".</p>
                            <button onclick="requestSilent('${term}')" style="background: linear-gradient(135deg, var(--primary), var(--primary-soft)); color: white; border: none; padding: 14px; border-radius: 30px; font-weight: bold; cursor: pointer; width: 100%;">
                                <i class="fas fa-paper-plane"></i> Request This Movie
                            </button>
                        </div>
                    `;
                }
                
                // Google ki API ko call (Client side)
                script.src = `https://suggestqueries.google.com/complete/search?client=chrome&q=${encodeURIComponent(term + ' movie')}&callback=googleSuggestCb`;
                document.head.appendChild(script);
            }
        } catch (err) {
            document.getElementById('searchGrid').innerHTML = '<div class="loader">Error</div>';
        }
    }, 500);
});
        // Details
        window.openDetails = function(id, isTMDB) {
            const movie = isTMDB ? tmdbMoviesMap[id] : allMovies.find(m => m.id == id);
            if (!movie) return;
            if (isTMDB) {
                // show request button
                const backdropImg = movie.image; // fallback
                document.getElementById('dpBackdrop').style.backgroundImage = `url(${backdropImg})`;
                document.getElementById('dpFloatPoster').src = movie.image;
                document.getElementById('dpTitle').innerText = movie.title;
                document.getElementById('dpRating').innerText = movie.rating || 'N/A';
                document.getElementById('dpGenre').innerText = movie.genre || 'Action, Drama';
                document.getElementById('dpDesc').innerText = movie.description || 'No description available.';
                document.getElementById('castSection').innerHTML = '';
                document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-request" onclick="requestMovie('${movie.title}')"><i class="fas fa-hand-paper"></i> Request This Title</button>`;
                document.getElementById('dpLinks').innerHTML = '';
                document.getElementById('detailsPage').classList.add('open');
                return;
            }

            // Fetch full details
            fetch(`/api/movie/${id}`)
                .then(res => res.json())
                .then(data => {
                    if (data.status === 'success') {
                        const m = data.movie;
                        // Set backdrop (use TMDB backdrop if exists, else poster)
                        const backdropUrl = m.backdrop ? m.backdrop : m.image;
                        document.getElementById('dpBackdrop').style.backgroundImage = `url(${backdropUrl})`;
                        document.getElementById('dpFloatPoster').src = m.image;
                        document.getElementById('dpTitle').innerText = m.title;
                        document.getElementById('dpRating').innerText = m.rating + '/10';
                        document.getElementById('dpGenre').innerText = m.genre;
                        document.getElementById('dpDesc').innerText = m.description;
                        // Cast
                        if (m.cast && m.cast.length) {
                            let castHtml = '<div class="cast-list">';
                            m.cast.forEach(c => {
                                castHtml += `<div class="cast-item"><img src="${c.profile || 'https://via.placeholder.com/70'}" alt="${c.name}"><span>${c.name}</span></div>`;
                            });
                            castHtml += '</div>';
                            document.getElementById('castSection').innerHTML = castHtml;
                        } else {
                            document.getElementById('castSection').innerHTML = '';
                        }
                        // Trailer button
                        if (m.trailer_key) {
                            document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-trailer" onclick="playTrailer('${m.trailer_key}')"><i class="fab fa-youtube"></i> Watch Trailer</button>`;
                        } else {
                            document.getElementById('dpTrailerBtn').innerHTML = `<button class="btn-trailer" onclick="tg.openLink('https://www.youtube.com/results?search_query=${encodeURIComponent(m.title)}+trailer')"><i class="fas fa-play"></i> Search Trailer</button>`;
                        }
                        // Download links
                        if (m.files && m.files.length) {
                            let links = '<div class="dl-heading">AVAILABLE QUALITIES</div>';
                            m.files.forEach(f => {
                                links += `
                                    <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                        <span class="quality-text">📁 ${f.quality} <span class="file-size">[${f.size || 'N/A'}]</span></span>
                                        <span class="action">Get</span>
                                    </button>
                                `;
                            });
                            document.getElementById('dpLinks').innerHTML = links;
                        } else {
                            document.getElementById('dpLinks').innerHTML = `
                                <div class="dl-heading">DOWNLOAD</div>
                                <button class="dl-btn" onclick="downloadMovie(${m.id})">
                                    <span class="quality-text">📁 1080p Full HD</span>
                                    <span class="action">Get</span>
                                </button>
                            `;
                        }
                        document.getElementById('detailsPage').classList.add('open');
                    }
                });
        };

        window.closeDetails = function() {
            document.getElementById('detailsPage').classList.remove('open');
        };

        window.playTrailer = function(key) {
            document.getElementById('trailerIframe').src = `https://www.youtube.com/embed/${key}?autoplay=1&rel=0`;
            document.getElementById('trailerModal').classList.add('active');
        };

        window.closeTrailer = function() {
            document.getElementById('trailerIframe').src = '';
            document.getElementById('trailerModal').classList.remove('active');
        };

        window.requestMovie = function(title) {
            tg.HapticFeedback.notificationOccurred('success');
            showToast('⏳ Requesting...');
            const user = tg.initDataUnsafe?.user || {id: 0, username: 'webapp', first_name: 'User'};
            fetch('/api/request', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title, user_id: user.id, username: user.username, first_name: user.first_name})
            })
            .then(r => r.json())
            .then(d => {
                if (d.status === 'success') showToast('✅ Request sent!');
                else showToast('❌ Failed');
            })
            .catch(() => showToast('❌ Error'));
        };

        // 🔥 NAYA: Silent Request (Jab TMDB aur Google dono fail ho jayein)
        window.requestSilent = function(title) {
            tg.HapticFeedback.notificationOccurred('success');
            showToast('⏳ Sending Request...');
            const user = tg.initDataUnsafe?.user || {id: 0, username: 'webapp', first_name: 'User'};
            
            fetch('/api/request', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title: title, user_id: user.id, username: user.username, first_name: user.first_name})
            })
            .then(r => r.json())
            .then(d => {
                if (d.status === 'success') {
                    showToast('✅ Request Sent to Admin!');
                    // Request bhejte hi Mini app close kar do (Seamless feel ke liye)
                    setTimeout(() => { tg.close(); }, 1500);
                } else {
                    showToast('❌ Failed to send');
                }
            })
            .catch(() => showToast('❌ Network Error'));
        };

        // 🛡️ NAYA: Anti-Bot Middleware Par Bhejne Wala Function
        window.downloadBot = function(id) {
            tg.HapticFeedback.impactOccurred('heavy');
            // Seedha Bot ki jagah pehle Secure verification page par bhejenge
            tg.openLink(`https://flimfybox-bot-yht0.onrender.com/watch/${id}`);
        };

        window.downloadMovie = function(id) {
            tg.HapticFeedback.impactOccurred('heavy');
            tg.openLink(`https://flimfybox-bot-yht0.onrender.com/watch/${id}`);
        };

        // Start
        loadMovies();
        
        // 🪄 NAYA JUGAD: URL se query nikal kar auto-search karna
        setTimeout(() => {
            const urlParams = new URLSearchParams(window.location.search);
            const reqQuery = urlParams.get('req');
            
            if (reqQuery) {
                const searchInput = document.getElementById('searchInput');
                searchInput.value = reqQuery;
                showToast("🔍 Finding correct spelling...");
                // Search ko trigger karo
                searchInput.dispatchEvent(new Event('input', { bubbles: true }));
            }
        }, 500); // Thoda ruk kar karenge taaki app load ho jaye
    </script>
</body>
</html>"""
    return html

# ==================== RUN FLASK ====================

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False)


# Uncomment the following lines only if you want to run Flask standalone (not recommended inside main)
# if __name__ == '__main__':
#     run_flask()

# ==================== BATCH UPLOAD HANDLERS (OLD - TO BE REMOVED) ====================

# Note: Purane batch functions ko replace kar diya gaya hai naye multi-channel batch functions se
# Isliye ye functions delete kar diye gaye hain aur unki jagah naye functions upar add kiye gaye hain.

# ==================== NEW REQUEST SYSTEM (CONFIRMATION FLOW) ====================

async def start_request_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 1: User clicks 'Request This Movie' -> Show Short & Stylish Guidelines"""
    query = update.callback_query
    await query.answer()
    
    # --- NEW STYLISH & SHORT TEXT ---
    request_instruction_text = (
        "📝 𝗥𝗲𝗾𝘂𝗲𝘀𝘁 𝗥𝘂𝗹𝗲𝘀..!!\n\n"
        "बस मूवी/सीरीज़ का <b>असली नाम</b> लिखें।✔️\n\n"
        "फ़ालतू शब्द (Download, HD, Please) न लिखें।♻️\n\n"
        "<b><a href='https://www.google.com/'>𝗚𝗼𝗼𝗴𝗹𝗲</a></b> से सही स्पेलिंग चेक कर लें। ☜\n\n"
        "✐ᝰ𝗘𝘅𝗮𝗺𝗽𝗹𝗲\n\n"
        "सही है.!‼️    \n"
        "─────────────────────\n"
        "Animal ✔️ | Animal Movie Download ❌\n"
        "─────────────────────\n"
        "Mirzapur S03 ✔️ | Mirzapur New Season ❌\n"
        "─────────────────────\n\n"
        "👇 <b>अब नीचे मूवी का नाम भेजें:</b>"
    )
    
    # Message Edit karein
    await query.edit_message_text(
        text=request_instruction_text,
        parse_mode='HTML',
        disable_web_page_preview=True
    )
    
    # Is instruction message ko bhi delete list me daal dein (2 min baad)
    track_message_for_deletion(context, update.effective_chat.id, query.message.message_id, 120)
    
    # State change -> Ab Bot sirf Name ka wait karega
    return WAITING_FOR_NAME

async def handle_request_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 2: User sends name -> Bot asks for Confirmation (Not saved yet)"""
    user_name_input = update.message.text.strip()
    chat_id = update.effective_chat.id
    
    # User ka message delete karne ke liye (Clean Chat)
    track_message_for_deletion(context, chat_id, update.message.message_id, 120)

    # ✅ FIXED: Safety Check - Agar user ne koi Menu Button daba diya
    MENU_BUTTONS = ['🔍 Search Movies', '📂 Browse by Genre', '🙋 Request Movie', '📊 My Stats', '❓ Help']

    if user_name_input.startswith('/') or user_name_input in MENU_BUTTONS:
        msg = await update.message.reply_text("❌ **Request Process Cancelled.**")
        track_message_for_deletion(context, chat_id, msg.message_id, 10)
        # Us button ka original function chala do
        await main_menu_or_search(update, context)
        return ConversationHandler.END

    # Name ko temporary memory me rakho
    context.user_data['temp_request_name'] = user_name_input
    
    # Confirmation Keyboard (Yes/No)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Confirm", callback_data="confirm_yes"),
            InlineKeyboardButton("❌ No, Cancel", callback_data="confirm_no")
        ]
    ])
    
    msg = await update.message.reply_text(
        f"🔔 <b>Confirmation Required</b>\n\n"
        f"क्या आप <b>'{user_name_input}'</b> को रिक्वेस्ट करना चाहते हैं?\n\n"
        f"नाम सही है तो <b>Yes</b> दबाएं, नहीं तो <b>No</b> दबाकर दोबारा कोशिश करें।",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    # ⚡ Ye Confirmation message 60 seconds me delete ho jayega
    track_message_for_deletion(context, chat_id, msg.message_id, 60)
    
    return CONFIRMATION

async def handle_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 3: Handle Yes/No buttons"""
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    
    choice = query.data
    user = query.from_user
    
    if choice == "confirm_no":
        await query.edit_message_text("❌ Request Cancelled. आप दोबारा सर्च या रिक्वेस्ट कर सकते हैं।")
        # Cancel message auto delete in 10 seconds
        track_message_for_deletion(context, chat_id, query.message.message_id, 10)
        context.user_data.pop('temp_request_name', None)
        return ConversationHandler.END
        
    elif choice == "confirm_yes":
        movie_title = context.user_data.get('temp_request_name')
        
        # --- FINAL SAVE TO DATABASE ---
        stored = store_user_request(
            user.id,
            user.username,
            user.first_name,
            movie_title,
            query.message.chat.id if query.message.chat.type != "private" else None,
            query.message.message_id
        )
        
        if stored:
            # Notify Admin
            group_info = query.message.chat.title if query.message.chat.type != "private" else None
            await send_admin_notification(context, user, movie_title, group_info)
            
            success_text = f"""
✅ <b>Request Sent to Admin!</b>

🎬 Movie: <b>{movie_title}</b>

📝 आपकी रिक्वेस्ट 𝑶𝒘𝒏𝒆𝒓 <b>@ownermahi</b> / <b>@ownermahima</b> को मिली गई है।
⏳ जैसे ही मूवी उपलब्ध होगी, वो खुद आपको यहाँ सूचित (Notify) कर देंगे।

<i>हमसे जुड़े रहने के लिए धन्यवाद! 🙏</i>
            """
            await query.edit_message_text(success_text, parse_mode='HTML')
        else:
            await query.edit_message_text("❌ Error: Request save नहीं हो पाई। शायद यह पहले से पेंडिंग है।")
            
        # ⚡ Success Message Auto Delete (60 Seconds)
        track_message_for_deletion(context, chat_id, query.message.message_id, 60)
            
        context.user_data.pop('temp_request_name', None)
        return ConversationHandler.END

async def timeout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """2 Minute Timeout Handler"""
    if update.effective_message:
        msg = await update.effective_message.reply_text("⏳ <b>Session Expired:</b> रिक्वेस्ट का समय समाप्त हो गया।", parse_mode='HTML')
        track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 30)
    return ConversationHandler.END

async def main_menu_or_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 👇 VIP Payment UTR Check 👇
    if context.user_data.get('payment_step') == 'utr':
        await payment_utr_handler(update, context)
        return

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    # === 1. FSub Check (Only in Private Chat) ===
    if update.effective_chat.type == "private":
        check = await is_user_member(context, user_id)
        if not check['is_member']:
            if update.message and update.message.text:
                context.user_data['pending_search_query'] = update.message.text.strip()

            msg = await update.message.reply_text(
                get_join_message(check['channel'], check['group']),
                reply_markup=get_join_keyboard(),
                parse_mode='Markdown'
            )
            track_message_for_deletion(context, chat_id, msg.message_id, 120)
            return
    # ============================================

    if not update.message or not update.message.text:
        return

    query_text = update.message.text.strip()
    
    # === 2. Menu Button Logic ===
    if query_text == '🔍 Search Movies':
        msg = await update.message.reply_text("Great! Just type the name of the movie you want to search for.")
        track_message_for_deletion(context, chat_id, msg.message_id, 60)
        return

    elif query_text == '🙋 Request Movie':
        web_app_url = "https://flimfybox-bot-yht0.onrender.com/webapp"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Open Request Portal", web_app=WebAppInfo(url=web_app_url))]
        ])
        msg = await update.message.reply_text(
            "👇 **स्मार्ट रिक्वेस्ट पोर्टल:**\n\nयहाँ मूवी का नाम सर्च करें। अगर स्पेलिंग गलत हुई, तो हमारा AI उसे सही कर देगा और आप सीधा रिक्वेस्ट भेज पाएंगे!", 
            reply_markup=keyboard, 
            parse_mode='Markdown'
        )
        track_message_for_deletion(context, chat_id, msg.message_id, 60)
        return

    elif query_text == '📊 My Stats':
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s", (user_id,))
                req = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM user_requests WHERE user_id = %s AND notified = TRUE", (user_id,))
                ful = cur.fetchone()[0]
                
                stats_msg = await update.message.reply_text(
                    f"📊 **Your Stats**\n\n📝 Total Requests: {req}\n✅ Fulfilled: {ful}",
                    parse_mode='Markdown'
                )
                track_message_for_deletion(context, chat_id, stats_msg.message_id, 120)
            except Exception as e:
                logger.error(f"Stats Error: {e}")
            finally:
                close_db_connection(conn)
        return

    elif query_text == '❓ Help':
        help_text = (
            "🤖 **How to use:**\n\n"
            "1. **Search:** Just type any movie name (e.g., 'Avengers').\n"
            "2. **Request:** If not found, use the Request button.\n"
            "3. **Download:** Click the buttons provided."
        )
        msg = await update.message.reply_text(help_text, parse_mode='Markdown')
        track_message_for_deletion(context, chat_id, msg.message_id, 120)
        return

    # === 3. If no button matched, Search for the Movie ===
    await search_movies(update, context)

# 👇👇👇 IS FUNCTION KO REPLACE KARO (Line ~1665) 👇👇👇

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle messages in groups using FAST SQL Search.
    Agar movie database me hai to reply karega, nahi to chup rahega.
    """
    if not update.message or not update.message.text:
        return
    
    text = update.message.text.strip()
    
    # 1. Commands ignore karo
    if text.startswith('/'):
        return
    
    # 2. Bahut chote words ignore karo
    if len(text) < 2:
        return

    # 3. 🚀 FAST SEARCH CALL (Sirf SQL Check)
    # Hum 5 results maang rahe hain taaki agar typos ho to best match mile
    movies = await run_async(get_movies_fast_sql, text, limit=5)

    if not movies:
        # 🤫 Agar movie nahi mili, to YAHIN RUK JAO.
        # Bot kuch reply nahi karega, group me shanti rahegi.
        return

    # 4. Results mil gaye, ab show karo
    context.user_data['search_results'] = movies
    context.user_data['search_query'] = text

    keyboard = create_movie_selection_keyboard(movies, page=0)
    
    # Reply to user
    msg = await update.message.reply_text(
        f"🎬 **Found {len(movies)} results for '{text}'**\n👇 Select movie:",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )
    
    # Auto-delete (Optional - 2 min)
    track_message_for_deletion(context, update.effective_chat.id, msg.message_id, 120)

async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mini App se aane wali movie ID ko receive karega aur movie bhejega"""
    if update.effective_message.web_app_data:
        received_data = update.effective_message.web_app_data.data
        chat_id = update.effective_chat.id
        
        if received_data.startswith("movie_"):
            movie_id = int(received_data.split("_")[1])
            
            # Loading message dikhayein
            status_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ <b>Fetching your movie from Web App...</b>", parse_mode='HTML')
            
            # Movie bhejne wala purana function call karein
            await deliver_movie_on_start(update, context, movie_id)
            
            try:
                await status_msg.delete()
            except:
                pass

async def auto_delete_worker(app: Application):
    """
    Background worker jo har 5 second me DB check karega, 
    messages delete karega aur fir DB se bhi entry uda dega (Self-Cleaning).
    """
    try:
        bot_info = await app.bot.get_me()
        bot_username = bot_info.username
    except Exception as e:
        logger.error(f"Worker bot info error: {e}")
        return

    logger.info(f"🧹 Auto-Delete Worker Started for @{bot_username}")

    while True:
        try:
            conn = get_db_connection()
            if conn:
                cur = conn.cursor()
                # 1. Wo messages dhoondo jinka time pura ho chuka hai
                cur.execute(
                    "SELECT id, chat_id, message_id FROM auto_delete_queue WHERE bot_username = %s AND delete_at <= NOW() LIMIT 50",
                    (bot_username,)
                )
                rows = cur.fetchall()
                
                for row in rows:
                    row_id, chat_id, msg_id = row
                    
                    # 2. Telegram se file delete karo
                    try:
                        await app.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                    except Exception:
                        pass # File pehle hi delete ho chuki hai ya bot block hai
                        
                    # 3. DB se turant delete karo (TAAKI DB CLEAN RAHE!)
                    cur.execute("DELETE FROM auto_delete_queue WHERE id = %s", (row_id,))
                    conn.commit()
                    
                cur.close()
                close_db_connection(conn)
        except Exception as e:
            logger.error(f"Auto-delete worker error: {e}")
            
        # Har 5 second me database check karega
        await asyncio.sleep(5)

# 👇 YAHAN SE COPY KARO AUR EXACTLY 'def register_handlers' KE THEEK UPAR PASTE KARO 👇

async def payment_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Agar user screenshot stage par hai
    if context.user_data.get('payment_step') == 'screenshot':
        context.user_data['screenshot_id'] = update.message.photo[-1].file_id
        context.user_data['payment_step'] = 'utr'
        await update.message.reply_text(
            "✅ <b>Screenshot Received!</b>\n\n🔢 Ab <b>UTR ya Reference Number</b> type karke bhejein.", 
            parse_mode='HTML'
        )
        return True # Matlab photo handle ho gayi
    return False

async def payment_utr_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Agar user UTR stage par hai
    if context.user_data.get('payment_step') == 'utr':
        utr_number = update.message.text.strip()
        user = update.effective_user
        screenshot_id = context.user_data.get('screenshot_id')
        
        # Admin ko alert bhejna
        admin_id = int(os.environ.get('ADMIN_USER_ID', '123456789')) 
        admin_text = (
            f"🔔 <b>NEW PAYMENT PENDING</b>\n\n"
            f"👤 Name: {user.first_name}\n"
            f"🆔 ID: <code>{user.id}</code>\n"
            f"🔢 UTR: <code>{utr_number}</code>"
        )
        try:
            await context.bot.send_photo(chat_id=admin_id, photo=screenshot_id, caption=admin_text, parse_mode='HTML')
        except Exception as e:
            pass
            
        await update.message.reply_text(
            "⏳ <b>Verification Pending!</b>\n\n✅ Payment details admin ko bhej di gayi hai. Thodi der me VIP access mil jayega.", 
            parse_mode='HTML'
        )
        # Process complete, ab reset kar do
        context.user_data.pop('payment_step', None)
        context.user_data.pop('screenshot_id', None)
        return True
    return False


# ==================== MULTI-BOT SETUP (REPLACES OLD MAIN) ====================

def register_handlers(application: Application):
    """
    यह फंक्शन हर बॉट पर लॉजिक (Handlers) सेट करेगा।
    ताकि तीनों बॉट्स सेम काम करें।
    """
    # -----------------------------------------------------------
    # 1. NEW REQUEST SYSTEM HANDLER (With 2 Min Timeout)
    # -----------------------------------------------------------
    # नोट: ConversationHandler को हर बार नया बनाना जरूरी है
    request_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_request_flow, pattern="^request_")],
        states={
            WAITING_FOR_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_request_name_input)
            ],
            CONFIRMATION: [
                CallbackQueryHandler(handle_confirmation_callback, pattern="^confirm_")
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('start', start)
        ],
        conversation_timeout=120,
    )
    application.add_handler(request_conv_handler)

    notify_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("notify", notify_start)],
        states={
            ASK_MOVIE: [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_ask_movie)],
            ASK_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_ask_user)]
        },
        fallbacks=[CommandHandler('cancel', notify_ask_user)], # Dummy fallback to catch /cancel
        conversation_timeout=120
    )
    application.add_handler(notify_conv_handler)
    
    # -----------------------------------------------------------
    # 2. GLOBAL HANDLERS
    # -----------------------------------------------------------

    # 👇 YAHAN PAR 'application' LIKHNA HAI 'app' KI JAGAH 👇
    
    
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, main_menu_or_search))
    
    # Button Callback
    application.add_handler(CallbackQueryHandler(button_callback))

    # -----------------------------------------------------------
    # 3. ADMIN & BATCH COMMANDS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler("addmovie", add_movie))
    application.add_handler(CommandHandler("bulkadd", bulk_add_movies))
    application.add_handler(CommandHandler("addalias", add_alias))
    application.add_handler(CommandHandler("aliases", list_aliases))
    application.add_handler(CommandHandler("aliasbulk", bulk_add_aliases))
    application.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & filters.CaptionRegex(r'^/post_query'), admin_post_query))
    application.add_handler(MessageHandler(filters.Regex(r'^/post18'), admin_post_18))
    application.add_handler(CommandHandler("fixbuttons", update_buttons_command))
    application.add_handler(CommandHandler("restore", restore_posts_command))

    # 🚀 NEW: Add this line to catch the poster image
    application.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, handle_admin_poster), group=0)

    # 🚀 SUPER BATCH COMMANDS
    application.add_handler(CommandHandler("superbatch", superbatch_start))
    application.add_handler(CommandHandler("superdone", superbatch_done))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO), superbatch_listener), group=4)
    
    
    # ==========================================
    # 🔞 18+ BATCH SYSTEM HANDLERS
    # ==========================================
    application.add_handler(CommandHandler("batch18", batch18_start))
    application.add_handler(CommandHandler("done18", batch18_done))
    application.add_handler(CommandHandler("cancel18", batch18_cancel))

    
    # ✅ FIX: group=1 जोड़ा गया ताकि यह दूसरे फाइल्स को ब्लॉक न करे
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.FORWARDED, batch18_listener), group=1)
    
    # Batch Commands
    application.add_handler(CommandHandler("batch", batch_add_command))
    application.add_handler(CommandHandler("done", batch_done_command))
    application.add_handler(CommandHandler("batchid", batch_id_command))
    application.add_handler(CommandHandler("fixdata", fix_missing_metadata))
    application.add_handler(CommandHandler("post", post_to_topic_command))
    
    # ✅ FIX: group=2 जोड़ा गया ताकि नॉर्मल बैच अपना काम कर सके
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO | filters.PHOTO | (filters.TEXT & ~filters.COMMAND)), pm_file_listener), group=2)

    # -----------------------------------------------------------
    # 4. GENRE & GROUP HANDLERS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler("genres", show_genre_selection))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, handle_group_message))

    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))
      
    # -----------------------------------------------------------
    # 5. NOTIFICATION & STATS
    # -----------------------------------------------------------
    application.add_handler(CommandHandler("notifyuser", notify_user_by_username))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("schedulenotify", schedule_notification))
    application.add_handler(CommandHandler("notifyuserwithmedia", notify_user_with_media))
    application.add_handler(CommandHandler("qnotify", quick_notify))
    application.add_handler(CommandHandler("forwardto", forward_to_user))
    application.add_handler(CommandHandler("broadcastmedia", broadcast_with_media))

    application.add_handler(CommandHandler("userinfo", get_user_info))
    application.add_handler(CommandHandler("listusers", list_all_users))
    application.add_handler(CommandHandler("adminhelp", admin_help))
    application.add_handler(CommandHandler("stats", get_bot_stats))

    # Error Handler
    application.add_error_handler(error_handler)


async def main():
    """Main function to run MULTIPLE bots concurrently"""
    logger.info("🚀 Starting Multi-Bot System...")

    # =================================================================
    # 1. Flask Server FIRST (Render timeout se bachao)
    # =================================================================
    flask_thread        = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    logger.info("🌐 Flask server started.")

    # =================================================================
    # 2. Database Setup
    # =================================================================
    try:
        setup_database()
        migrate_add_imdb_columns()
        migrate_content_type_for_restore()
        migrate_channel_posts_v2()
        fix_channel_posts_constraint()
        fix_movies_unique_constraint()
        fix_movies_title_constraint()
    except Exception as e:
        logger.error(f"❌ DB Setup Error: {e}")  # ← YE LINE ZAROORI HAI

    # =================================================================
    # 3. Get Tokens from ENV
    # =================================================================
    tokens = [
        os.environ.get("TELEGRAM_BOT_TOKEN"),  # Bot 1
        os.environ.get("BOT_TOKEN_2"),          # Bot 2
        os.environ.get("BOT_TOKEN_3")           # Bot 3
    ]

    # Khali tokens filter karo
    tokens = [t for t in tokens if t]

    if not tokens:
        logger.error("❌ No tokens found! Check Environment Variables.")
        return

    # =================================================================
    # 4. Initialize & Start All Bots
    # =================================================================
    apps = []
    logger.info(f"🤖 Found {len(tokens)} tokens. Initializing bots...")

    for i, token in enumerate(tokens):
        try:
            logger.info(f"🔹 Initializing Bot {i+1}...")

            app = (
                Application.builder()
                .token(token)
                .read_timeout(30)
                .write_timeout(30)
                .build()
            )

            register_handlers(app)

            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            asyncio.create_task(auto_delete_worker(app))
            if i == 0:
                logger.info("🚀 Starting Trending Worker for Main Bot...")
                asyncio.create_task(trending_worker_loop(app, ADMIN_USER_ID))
            


            apps.append(app)

            bot_info = await app.bot.get_me()
            logger.info(f"✅ Bot {i+1} Started: @{bot_info.username}")

        except Exception as e:
            logger.error(f"❌ Failed to start Bot {i+1}: {e}")

    if not apps:
        logger.error("❌ No bots could be started.")
        return

    # =================================================================
    # 5. Keep Script Alive
    # =================================================================
    stop_signal = asyncio.Event()
    await stop_signal.wait()

    # Cleanup
    for app in apps:
        try:
            await app.stop()
            await app.shutdown()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Critical Error: {e}")
