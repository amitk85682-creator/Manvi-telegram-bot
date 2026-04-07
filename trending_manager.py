import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta
from io import BytesIO

import aiohttp
import pytz
import requests
import psycopg2
from PIL import Image, ImageFilter
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# -------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# ENVIRONMENT VARIABLES
# -------------------------------------------------------------
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', '-1002555232489'))
BOT_INSTANCE_ID = os.environ.get('BOT_INSTANCE_ID', socket.gethostname())
ADMIN_USER_ID = int(os.environ.get('ADMIN_USER_ID', '6946322342'))

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL not set!")

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logger.error(f"❌ DB conn error: {e}")
        return None

def close_db_connection(conn):
    if conn:
        try:
            conn.close()
        except:
            pass

# -------------------------------------------------------------
# IMAGE PROCESSING (Cinematic Square Poster) – SAME AS main.py
# -------------------------------------------------------------
async def make_landscape_poster(url_or_bytes):
    """
    Portrait (vertical) poster ko Mobile+PC friendly (Square 1:1) format me convert karta hai.
    Heavily Blurred background aur center me sharp image.
    Exactly the same function as used in main.py.
    """
    try:
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

        img = Image.open(BytesIO(image_data)).convert("RGB")

        target_w, target_h = 800, 800   # Square format

        # 1. Blurred background (zoomed and heavy blur)
        bg_img = img.resize((target_w, int(img.height * (target_w / img.width))), Image.Resampling.LANCZOS)
        if bg_img.height > target_h:
            top = (bg_img.height - target_h) // 2
            bg_img = bg_img.crop((0, top, target_w, top + target_h))
        else:
            bg_img = bg_img.resize((target_w, target_h), Image.Resampling.LANCZOS)
        bg_img = bg_img.filter(ImageFilter.GaussianBlur(radius=40))

        # 2. Foreground (original poster) centred and slightly smaller
        fg_h = int(target_h * 0.95)
        fg_w = int(img.width * (fg_h / img.height))
        fg_img = img.resize((fg_w, fg_h), Image.Resampling.LANCZOS)

        paste_x = (target_w - fg_w) // 2
        paste_y = (target_h - fg_h) // 2
        bg_img.paste(fg_img, (paste_x, paste_y))

        # 3. Output as bytes
        output = BytesIO()
        output.name = "cinematic_poster.jpg"
        bg_img.save(output, format='JPEG', quality=95)
        output.seek(0)
        return output

    except Exception as e:
        logger.error(f"❌ Cinematic Conversion Error: {e}")
        return url_or_bytes

async def download_and_process_poster(url):
    """Download image from URL and apply cinematic effect (vertical poster preferred)"""
    if not url or url == 'N/A':
        return None
    try:
        return await make_landscape_poster(url)
    except Exception as e:
        logger.error(f"Poster processing failed: {e}")
        return None

# -------------------------------------------------------------
# TMDB API
# -------------------------------------------------------------
def fetch_trending(time_window="day"):
    try:
        url = f"https://api.themoviedb.org/3/trending/all/{time_window}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get('results', [])[:15]
    except Exception as e:
        logger.error(f"fetch_trending error: {e}")
        return []

def fetch_extra_details(tmdb_id, media_type):
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US"
        resp = requests.get(url, timeout=10)
        return resp.json()
    except Exception as e:
        logger.error(f"fetch_extra_details error: {e}")
        return {}

# -------------------------------------------------------------
# MESSAGE BUILDERS (unchanged from your original)
# -------------------------------------------------------------
def build_channel_post(item, extra):
    """Clean message for channel auto-post (no warning lines)"""
    title = item.get('title') or item.get('name') or "Unknown"
    vote_avg = item.get('vote_average', 0)
    release_date = item.get('release_date') or item.get('first_air_date') or "N/A"

    genre_ids = extra.get('genres', [])
    genre_names = [g['name'] for g in genre_ids if 'name' in g]
    genre_str = " | ".join(genre_names) if genre_names else "Unknown"

    lang = "Hindi + English (Dual Audio)"
    dynamic_res = "1080p | 720p | 480p"

    safe_title = str(title).replace('<', '&lt;').replace('>', '&gt;')

    text = (
        f"🎬 <b>{safe_title}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🌟 <b>#1st On 🅃🄴🄻🄴🄶🅁🄰🄼</b>\n"
        f"📅 <b>Date:</b> {release_date}\n"
        f"⭐ <b>iMDB Rating:</b> {vote_avg}/10\n"
        f"🎭 <b>Genre:</b> {genre_str}\n"
        f"🔊 <b>Language:</b> {lang}\n"
        f"<b>Quality:</b> V2 HQ-HDTC {dynamic_res}\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"<b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
        f"👇 <b>Download Below</b> 👇"
    )
    return text

def build_admin_alert(item, extra):
    """Admin alert message (includes warning lines)"""
    title = item.get('title') or item.get('name') or "Unknown"
    media_type = item.get('media_type', 'movie')
    tmdb_id = item.get('id')
    overview = item.get('overview', '')
    vote_avg = item.get('vote_average', 0)
    release_date = item.get('release_date') or item.get('first_air_date') or "N/A"

    genre_ids = extra.get('genres', [])
    genre_names = [g['name'] for g in genre_ids if 'name' in g]
    genre_str = " | ".join(genre_names) if genre_names else "Unknown"

    lang = "Hindi + English (Dual Audio)"
    dynamic_res = "1080p | 720p | 480p"

    safe_title = str(title).replace('<', '&lt;').replace('>', '&gt;')
    safe_overview = str(overview).replace('<', '&lt;').replace('>', '&gt;')
    if len(safe_overview) > 200:
        safe_overview = safe_overview[:197] + "..."

    text = (
        f"🎬 <b>{safe_title}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🌟 <b>#1st On 🅃🄴🄻🄴🄶🅁🄰🄼</b>\n"
        f"📅 <b>Date:</b> {release_date}\n"
        f"⭐ <b>iMDB Rating:</b> {vote_avg}/10\n"
        f"🎭 <b>Genre:</b> {genre_str}\n"
        f"🔊 <b>Language:</b> {lang}\n"
        f"<b>Quality:</b> V2 HQ-HDTC {dynamic_res}\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"<b>18+ Content:</b> <a href='https://t.me/+wcYoTQhIz-ZmOTY1'>Join Premium</a>\n"
        f"👇 <b>Download Below</b> 👇\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <i>Yeh movie database mein nahi hai!</i>\n"
        f"📥 <i>Add kar le before users search karein.</i>"
    )

    admin_buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 View on TMDB", url=f"https://www.themoviedb.org/{media_type}/{tmdb_id}"),
            InlineKeyboardButton("🔍 Search Google", url=f"https://www.google.com/search?q={safe_title.replace(' ', '+')}+download")
        ]
    ])

    image_url = f"https://image.tmdb.org/t/p/w780{item.get('backdrop_path')}" if item.get('backdrop_path') else None
    return text, admin_buttons, image_url

def build_summary_message(new_count, total_checked, skipped_in_db, skipped_already):
    now = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%d %b %Y, %I:%M %p IST")
    text = (
        f"📊 <b>TRENDING SUMMARY</b>\n\n"
        f"🤖 Bot: <code>{BOT_INSTANCE_ID}</code>\n"
        f"🕐 Time: <code>{now}</code>\n"
        f"🔍 Checked: <code>{total_checked}</code> items\n"
        f"🚀 Auto-Posted: <code>{skipped_in_db}</code>\n"
        f"🔁 Already Alerted: <code>{skipped_already}</code>\n"
        f"🆕 New Alerts: <code>{new_count}</code>\n\n"
    )
    if new_count == 0:
        text += "💤 <i>Sab kuch covered hai.</i>"
    else:
        text += "🔥 <i>Naya content aaya hai!</i>"
    return text

# -------------------------------------------------------------
# DATABASE SETUP (same as before)
# -------------------------------------------------------------
def setup_trending_db():
    if not DATABASE_URL:
        return False
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_history (
                tmdb_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                media_type TEXT DEFAULT 'movie',
                popularity REAL DEFAULT 0,
                vote_average REAL DEFAULT 0,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posted_by TEXT DEFAULT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP,
                locked_by TEXT DEFAULT NULL,
                lock_time TIMESTAMP DEFAULT NULL
            )
        """)

        # Add existing missing columns
        for col in ['posted_by']:
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='trending_history' AND column_name='{col}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE trending_history ADD COLUMN {col} TEXT DEFAULT NULL")

        # 👇 NAYA: Status track karne ke liye column (alerted ya auto_posted)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='trending_history' AND column_name='status'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE trending_history ADD COLUMN status TEXT DEFAULT 'alerted'")

        for col in ['locked_by', 'lock_time']:
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='trending_meta' AND column_name='{col}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE trending_meta ADD COLUMN {col} TEXT DEFAULT NULL")

        cur.execute("INSERT INTO trending_meta (id, last_check) VALUES (1, '2000-01-01') ON CONFLICT (id) DO NOTHING")
        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("✅ Trending DB ready with Status Tracking")
        return True
    except Exception as e:
        logger.error(f"setup_trending_db error: {e}")
        return False

def get_last_check_time():
    try:
        conn = get_db_connection()
        if not conn:
            return datetime(2000, 1, 1)
        cur = conn.cursor()
        cur.execute("SELECT last_check FROM trending_meta WHERE id = 1")
        row = cur.fetchone()
        cur.close()
        close_db_connection(conn)
        if row and row[0]:
            return row[0]
        return datetime(2000, 1, 1)
    except:
        return datetime(2000, 1, 1)

def update_last_check():
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("UPDATE trending_meta SET last_check = CURRENT_TIMESTAMP WHERE id = 1")
        conn.commit()
        cur.close()
        close_db_connection(conn)
    except Exception as e:
        logger.error(f"update_last_check error: {e}")

def acquire_lock():
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("SELECT locked_by, lock_time FROM trending_meta WHERE id = 1")
        row = cur.fetchone()
        if row and row[0]:
            locked_by, lock_time = row
            if lock_time and (datetime.utcnow() - lock_time).total_seconds() < 600:
                if locked_by != BOT_INSTANCE_ID:
                    cur.close()
                    close_db_connection(conn)
                    return False
        cur.execute("UPDATE trending_meta SET locked_by = %s, lock_time = CURRENT_TIMESTAMP WHERE id = 1", (BOT_INSTANCE_ID,))
        conn.commit()
        cur.close()
        close_db_connection(conn)
        return True
    except Exception as e:
        logger.error(f"acquire_lock error: {e}")
        return False

def release_lock():
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("UPDATE trending_meta SET locked_by = NULL, lock_time = NULL WHERE id = 1 AND locked_by = %s", (BOT_INSTANCE_ID,))
        conn.commit()
        cur.close()
        close_db_connection(conn)
    except Exception as e:
        logger.error(f"release_lock error: {e}")

# -------------------------------------------------------------
# MAIN TRENDING CHECK (with cinematic square poster & 4 buttons)
# -------------------------------------------------------------
async def check_and_alert_trending(app, admin_id):
    if not acquire_lock():
        return 0

    logger.info(f"🔍 Checking trending...")
    new_alerts = 0
    auto_posted = 0
    skipped_already = 0
    skipped_in_db = 0

    conn = None
    try:
        items = fetch_trending("day")
        if not items:
            release_lock()
            return 0

        conn = get_db_connection()
        if not conn:
            release_lock()
            return 0

        cur = conn.cursor()
        total = len(items)

        for item in items:
            tmdb_id = int(item.get('id', 0))
            title = item.get('title') or item.get('name')
            media_type = item.get('media_type', 'movie')
            if not title or not tmdb_id:
                continue

            # Fetch extra details to get IMDb ID
            extra = fetch_extra_details(tmdb_id, media_type)
            imdb_id = extra.get('imdb_id') # TMDB normally returns this for movies

            # 1. HISTORY CHECK
            cur.execute("SELECT status FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            hist_row = cur.fetchone()
            hist_status = hist_row[0] if hist_row else None

            # Agar already channel pe post ho chuki hai, toh kuch mat karo
            if hist_status == 'auto_posted':
                skipped_already += 1
                continue

            # 2. MAIN DB CHECK (IMDb ID First, then EXACT Title fallback)
            movie_row = None
            if imdb_id:
                cur.execute("SELECT id FROM movies WHERE imdb_id = %s LIMIT 1", (imdb_id,))
                movie_row = cur.fetchone()
            
            # Agar IMDb ID se na mile (ya ID na ho), tabhi EXACT naam se dhoondo (ILIKE %title% use nahi karna)
            if not movie_row:
                cur.execute("SELECT id FROM movies WHERE title = %s LIMIT 1", (title,))
                movie_row = cur.fetchone()

            # --- POSTING & ALERTING LOGIC ---
            if movie_row:
                # 🎬 MOVIE DATABASE MEIN MIL GAYI!
                channel_text = build_channel_post(item, extra)
                watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{movie_row[0]}"

                channel_buttons = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Download Now", url=watch_link),
                     InlineKeyboardButton("Download Now", url=watch_link)],
                    [InlineKeyboardButton("⚡ Download Now", url=watch_link)],
                    [InlineKeyboardButton("📢 Join Channel", url=os.environ.get('FILMFYBOX_CHANNEL_URL', 'https://t.me/FlimfyBox'))]
                ])

                # Poster process karo
                image_url = f"https://image.tmdb.org/t/p/original{item['poster_path']}" if item.get('poster_path') else None
                if not image_url and item.get('backdrop_path'):
                    image_url = f"https://image.tmdb.org/t/p/original{item['backdrop_path']}"
                
                processed_image = await download_and_process_poster(image_url) if image_url else None

                # Channel me bhejo
                if processed_image:
                    await app.bot.send_photo(chat_id=CHANNEL_ID, photo=processed_image, caption=channel_text, parse_mode='HTML', reply_markup=channel_buttons)
                else:
                    await app.bot.send_message(chat_id=CHANNEL_ID, text=channel_text, parse_mode='HTML', reply_markup=channel_buttons)
                
                auto_posted += 1
                skipped_in_db += 1
                logger.info(f"Auto-posted (Found in DB): {title}")

                # History update karo
                if hist_status == 'alerted':
                    cur.execute("UPDATE trending_history SET status = 'auto_posted' WHERE tmdb_id = %s", (tmdb_id,))
                else:
                    cur.execute("""
                        INSERT INTO trending_history (tmdb_id, title, media_type, popularity, vote_average, posted_by, status)
                        VALUES (%s, %s, %s, %s, %s, %s, 'auto_posted')
                    """, (tmdb_id, title, media_type, item.get('popularity', 0), item.get('vote_average', 0), BOT_INSTANCE_ID))
                conn.commit()

            else:
                # 🚫 MOVIE DATABASE MEIN NAHI HAI
                if hist_status == 'alerted':
                    # Admin ko pehle hi bata diya tha, par usne abhi tak add nahi ki. Spam mat karo.
                    skipped_already += 1
                    continue
                
                # Nayi trending movie hai, Admin ko Alert karo!
                admin_text, admin_buttons, admin_image_url = build_admin_alert(item, extra)
                
                image_url = f"https://image.tmdb.org/t/p/original{item['poster_path']}" if item.get('poster_path') else None
                processed_image = await download_and_process_poster(image_url) if image_url else None

                if processed_image:
                    await app.bot.send_photo(chat_id=admin_id, photo=processed_image, caption=admin_text, parse_mode='HTML', reply_markup=admin_buttons)
                elif admin_image_url:
                    await app.bot.send_photo(chat_id=admin_id, photo=admin_image_url, caption=admin_text, parse_mode='HTML', reply_markup=admin_buttons)
                else:
                    await app.bot.send_message(chat_id=admin_id, text=admin_text, parse_mode='HTML', reply_markup=admin_buttons)
                
                new_alerts += 1
                logger.info(f"New Admin alert: {title}")

                # History me 'alerted' mark karke save kar do
                cur.execute("""
                    INSERT INTO trending_history (tmdb_id, title, media_type, popularity, vote_average, posted_by, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'alerted')
                """, (tmdb_id, title, media_type, item.get('popularity', 0), item.get('vote_average', 0), BOT_INSTANCE_ID))
                conn.commit()

            await asyncio.sleep(3) # Flood control

        # Summary
        summary = build_summary_message(new_alerts, total, skipped_in_db, skipped_already)
        try:
            await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Summary send failed: {e}")

        cur.close()
        update_last_check()
    except Exception as e:
        logger.error(f"check_and_alert_trending error: {e}")
    finally:
        if conn:
            close_db_connection(conn)
        release_lock()

    return new_alerts

# -------------------------------------------------------------
# SCHEDULER: Fixed IST times (00:00, 06:00, 12:00, 18:00)
# -------------------------------------------------------------
def get_next_run_time_ist(hours=[0, 6, 12, 18]):
    tz = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(tz)
    candidates = []
    for h in hours:
        candidate = now_ist.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate <= now_ist:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    next_ist = min(candidates)
    return next_ist.astimezone(pytz.UTC)

async def trending_worker_loop(app, admin_id):
    logger.info("🛠️ TRENDING WORKER STARTED (Fixed Schedule IST)")
    if not setup_trending_db():
        logger.error("DB setup failed, worker stopping")
        return

    # Send startup message
    next_run_utc = get_next_run_time_ist()
    tz = pytz.timezone('Asia/Kolkata')
    next_ist = next_run_utc.astimezone(tz)
    time_str = next_ist.strftime("%I:%M %p IST")

    startup_msg = (
        f"🚀 <b>TRENDING MONITOR ON</b>\n\n"
        f"🕒 Fixed Schedule: 00:00, 06:00, 12:00, 18:00 IST\n"
        f"⏱️ Next Check: <code>{time_str}</code>\n\n"
        f"💤 Bot will sleep until then."
    )
    try:
        await app.bot.send_message(chat_id=admin_id, text=startup_msg, parse_mode='HTML')
        logger.info(f"Startup message sent to {admin_id}")
    except Exception as e:
        logger.error(f"Failed to send startup: {e}")

    # Main loop
    while True:
        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
        next_run_utc = get_next_run_time_ist()
        sleep_seconds = (next_run_utc - now_utc).total_seconds()
        if sleep_seconds < 0:
            sleep_seconds = 0

        logger.info(f"Sleeping for {sleep_seconds/3600:.2f} hours until next scheduled time")
        await asyncio.sleep(sleep_seconds)

        logger.info("🔍 Running scheduled trending check...")
        await check_and_alert_trending(app, admin_id)
