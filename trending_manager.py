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
# IMAGE PROCESSING (Cinematic Square Poster)
# -------------------------------------------------------------
async def make_landscape_poster(url_or_bytes):
    """
    Portrait (vertical) poster ko Mobile+PC friendly (Square 1:1) format me convert karta hai.
    Heavily Blurred background aur center me sharp image.
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
# TMDB API - NOW WITH REGIONAL SUPPORT
# -------------------------------------------------------------
def fetch_trending(time_window="day", region=None):
    """
    Fetch trending from TMDB.
    If region is provided (e.g., 'IN'), uses region parameter to get country-specific trends.
    """
    try:
        base_url = f"https://api.themoviedb.org/3/trending/all/{time_window}"
        params = {
            'api_key': TMDB_API_KEY,
            'language': 'en-US'
        }
        if region:
            params['region'] = region
        resp = requests.get(base_url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get('results', [])[:15]
    except Exception as e:
        logger.error(f"fetch_trending error (region={region}): {e}")
        return []

def fetch_indian_regional():
    """Sirf Indian regional (Telugu, Tamil, etc.) movies dhoondhne ke liye"""
    try:
        # TMDB Discover API use kar rahe hain popular Indian movies ke liye
        url = f"https://api.themoviedb.org/3/discover/movie"
        params = {
            'api_key': TMDB_API_KEY,
            'region': 'IN',
            'with_origin_country': 'IN',
            'sort_by': 'popularity.desc',
            'certification_country': 'IN',
            'language': 'en-US'
        }
        resp = requests.get(url, params=params, timeout=15)
        return resp.json().get('results', [])[:10]
    except Exception as e:
        logger.error(f"Indian regional fetch error: {e}")
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
# MESSAGE BUILDERS
# -------------------------------------------------------------
def build_channel_post(item, extra, db_metadata=None):
    # Agar DB se data mila hai toh wahi use karo, varna TMDB ka default
    title = item.get('title') or item.get('name')
    date = item.get('release_date') or item.get('first_air_date')
    
    # Database metadata (Languages aur Quality)
    lang = db_metadata.get('languages', "Hindi + English") if db_metadata else "Dual Audio"
    quality = db_metadata.get('quality', "1080p | 720p | 480p") if db_metadata else "HD"

    text = (
        f"🎬 <b>{title}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"🌟 <b>#1st On 🅃🄴🄻🄴🄶🅁🄰🄼</b>\n"
        f"📅 <b>Date:</b> {date}\n"
        f"⭐ <b>iMDB Rating:</b> {item.get('vote_average')}/10\n"
        f"🎭 <b>Genre:</b> {', '.join([g['name'] for g in extra.get('genres', [])])}\n"
        f"🔊 <b>Language:</b> {lang}\n"
        f"<b>Quality:</b> {quality}\n"
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
        f"📥 <i>Add kar le before users search karein.</i>\n"
        f"🔍 <b>IMDb ID:</b> <code>{extra.get('imdb_id', 'N/A')}</code>\n"
        f"🆔 <b>TMDB ID:</b> <code>{tmdb_id}</code>"
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
        f"🌍 Checked Worldwide + India Trends\n"
        f"🔍 Total Items: <code>{total_checked}</code>\n"
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
# DATABASE SETUP
# -------------------------------------------------------------
def setup_trending_db():
    if not DATABASE_URL:
        return False
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()

        # Main trending history table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_history (
                tmdb_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                media_type TEXT DEFAULT 'movie',
                popularity REAL DEFAULT 0,
                vote_average REAL DEFAULT 0,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posted_by TEXT DEFAULT NULL,
                status TEXT DEFAULT 'alerted',
                imdb_id TEXT DEFAULT NULL
            )
        """)
        
        # Meta table for lock mechanism
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trending_meta (
                id INTEGER PRIMARY KEY,
                last_check TIMESTAMP,
                locked_by TEXT DEFAULT NULL,
                lock_time TIMESTAMP DEFAULT NULL
            )
        """)

        # Add missing columns if they don't exist
        columns_to_add = {
            'posted_by': 'TEXT DEFAULT NULL',
            'status': "TEXT DEFAULT 'alerted'",
            'imdb_id': 'TEXT DEFAULT NULL'
        }
        
        for col_name, col_def in columns_to_add.items():
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='trending_history' AND column_name='{col_name}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE trending_history ADD COLUMN {col_name} {col_def}")
                logger.info(f"✅ Added column: {col_name}")

        # Add lock columns to meta table
        for col in ['locked_by', 'lock_time']:
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='trending_meta' AND column_name='{col}'
            """)
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE trending_meta ADD COLUMN {col} {'TEXT' if col == 'locked_by' else 'TIMESTAMP'} DEFAULT NULL")

        # Initialize meta table
        cur.execute("INSERT INTO trending_meta (id, last_check) VALUES (1, '2000-01-01') ON CONFLICT (id) DO NOTHING")
        
        conn.commit()
        cur.close()
        close_db_connection(conn)
        logger.info("✅ Trending DB ready with IMDb ID + Status Tracking")
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
# MAIN TRENDING CHECK (Worldwide + India)
# -------------------------------------------------------------
async def check_and_alert_trending(app, admin_id):
    if not acquire_lock():
        logger.info("🔒 Lock already held by another instance")
        return 0

    logger.info(f"🔍 Checking trending (Worldwide + India)...")
    new_alerts = 0
    auto_posted = 0
    skipped_already = 0
    skipped_in_db = 0

    conn = None
    try:
        # 1. FETCH ALL TRENDING (Worldwide + India + Regional)
        worldwide_items = fetch_trending("day")               # Global
        india_items = fetch_trending("day", region="IN")      # India specific
        regional_items = fetch_indian_regional()              # Indian regional movies

        # Combine and deduplicate by tmdb_id
        combined_dict = {}
        for item in worldwide_items + india_items + regional_items:
            tmdb_id = item.get('id')
            if tmdb_id:
                combined_dict[tmdb_id] = item
        items = list(combined_dict.values())
        
        if not items:
            logger.warning("⚠️ No trending items found")
            release_lock()
            return 0

        logger.info(f"🌐 Worldwide: {len(worldwide_items)} | 🇮🇳 India: {len(india_items)} | Regional: {len(regional_items)} | Unique: {len(items)}")

        conn = get_db_connection()
        if not conn:
            logger.error("❌ DB connection failed")
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

            extra = fetch_extra_details(tmdb_id, media_type)
            imdb_id = extra.get('imdb_id')

            # ═══════════════════════════════════════════════════════
            # STEP 1: CHECK TRENDING HISTORY
            # ═══════════════════════════════════════════════════════
            cur.execute("SELECT status, imdb_id FROM trending_history WHERE tmdb_id = %s", (tmdb_id,))
            hist_row = cur.fetchone()
            hist_status = hist_row[0] if hist_row else None

            if hist_status == 'auto_posted':
                skipped_already += 1
                continue

            # ═══════════════════════════════════════════════════════
            # STEP 2: CHECK MAIN DATABASE (IMDb ID + Title Priority)
            # ═══════════════════════════════════════════════════════
            movie_row = None
            match_method = None

            # Priority 1: IMDb ID (Sabse accurate)
            if imdb_id:
                cur.execute("SELECT id, title, imdb_id, year, language FROM movies WHERE imdb_id = %s LIMIT 1", (imdb_id,))
                movie_row = cur.fetchone()
                if movie_row:
                    match_method = "IMDb ID"

            # Priority 2: Title Match + Year Verify
            if not movie_row:
                cur.execute("SELECT id, title, imdb_id, year, language FROM movies WHERE title ILIKE %s LIMIT 1", (title,))
                movie_row = cur.fetchone()

                if movie_row:
                    db_id, db_title, db_imdb, db_year, db_lang = movie_row
                    
                    tmdb_date = item.get('release_date') or item.get('first_air_date') or ""
                    tmdb_year = tmdb_date.split('-')[0] if '-' in tmdb_date else "0"

                    # YEAR CHECK: Agar year match nahi hota, toh reject kar do
                    if str(db_year) != str(tmdb_year) and db_imdb != imdb_id:
                        logger.warning(f"❌ Year mismatch: DB({db_year}) vs TMDB({tmdb_year}). Alerting Admin.")
                        movie_row = None 
                    else:
                        match_method = "Title + Year"

            # ═══════════════════════════════════════════════════════
            # STEP 3: POST TO CHANNEL OR ALERT ADMIN
            # ═══════════════════════════════════════════════════════
            
            if movie_row:
                # 🎬 MOVIE FOUND IN DATABASE
                db_id, db_title, db_imdb, db_year, db_lang = movie_row
                
                logger.info(f"🚀 Auto-posting to channel: {db_title} (matched by {match_method})")
                
                # Use default quality if not in DB
                metadata = {
                    'languages': db_lang or "Hindi + English", 
                    'quality': "1080p | 720p | 480p"  # Default quality
                }
                channel_text = build_channel_post(item, extra, db_metadata=metadata)
                
                watch_link = f"https://flimfybox-bot-yht0.onrender.com/watch/{db_id}"

                channel_buttons = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("Download Now", url=watch_link),
                        InlineKeyboardButton("Download Now", url=watch_link)
                    ],
                    [InlineKeyboardButton("⚡ Download Now", url=watch_link)],
                    [InlineKeyboardButton("📢 Join Channel", url=os.environ.get('FILMFYBOX_CHANNEL_URL', 'https://t.me/FlimfyBox'))]
                ])

                # Process poster (prefer vertical poster for cinematic effect)
                image_url = f"https://image.tmdb.org/t/p/original{item['poster_path']}" if item.get('poster_path') else None
                if not image_url and item.get('backdrop_path'):
                    image_url = f"https://image.tmdb.org/t/p/original{item['backdrop_path']}"
                
                processed_image = await download_and_process_poster(image_url) if image_url else None

                # Send to channel
                try:
                    if processed_image:
                        await app.bot.send_photo(
                            chat_id=CHANNEL_ID, 
                            photo=processed_image, 
                            caption=channel_text, 
                            parse_mode='HTML', 
                            reply_markup=channel_buttons
                        )
                    else:
                        await app.bot.send_message(
                            chat_id=CHANNEL_ID, 
                            text=channel_text, 
                            parse_mode='HTML', 
                            reply_markup=channel_buttons
                        )
                    
                    auto_posted += 1
                    skipped_in_db += 1
                    logger.info(f"✅ Posted successfully: {db_title}")
                    
                except Exception as e:
                    logger.error(f"❌ Channel post failed for {db_title}: {e}")

                # Update history
                if hist_status == 'alerted':
                    cur.execute(
                        "UPDATE trending_history SET status = 'auto_posted', imdb_id = %s WHERE tmdb_id = %s",
                        (imdb_id, tmdb_id)
                    )
                else:
                    cur.execute("""
                        INSERT INTO trending_history (tmdb_id, title, media_type, popularity, vote_average, posted_by, status, imdb_id)
                        VALUES (%s, %s, %s, %s, %s, %s, 'auto_posted', %s)
                    """, (tmdb_id, title, media_type, item.get('popularity', 0), item.get('vote_average', 0), BOT_INSTANCE_ID, imdb_id))
                
                conn.commit()

            else:
                # 🚫 MOVIE NOT IN DATABASE - ALERT ADMIN
                
                # Check if already alerted (don't spam admin)
                if hist_status == 'alerted':
                    logger.info(f"⏭️ Already alerted admin about: {title}")
                    skipped_already += 1
                    continue
                
                logger.info(f"🔔 Alerting admin: {title} (not in DB)")
                
                admin_text, admin_buttons, admin_image_url = build_admin_alert(item, extra)
                
                # Use poster for admin alerts too
                image_url = f"https://image.tmdb.org/t/p/original{item['poster_path']}" if item.get('poster_path') else None
                processed_image = await download_and_process_poster(image_url) if image_url else None

                try:
                    if processed_image:
                        await app.bot.send_photo(
                            chat_id=admin_id, 
                            photo=processed_image, 
                            caption=admin_text, 
                            parse_mode='HTML', 
                            reply_markup=admin_buttons
                        )
                    elif admin_image_url:
                        await app.bot.send_photo(
                            chat_id=admin_id, 
                            photo=admin_image_url, 
                            caption=admin_text, 
                            parse_mode='HTML', 
                            reply_markup=admin_buttons
                        )
                    else:
                        await app.bot.send_message(
                            chat_id=admin_id, 
                            text=admin_text, 
                            parse_mode='HTML', 
                            reply_markup=admin_buttons
                        )
                    
                    new_alerts += 1
                    logger.info(f"✅ Admin alerted: {title}")
                    
                except Exception as e:
                    logger.error(f"❌ Admin alert failed for {title}: {e}")

                # Mark as alerted in history
                cur.execute("""
                    INSERT INTO trending_history (tmdb_id, title, media_type, popularity, vote_average, posted_by, status, imdb_id)
                    VALUES (%s, %s, %s, %s, %s, %s, 'alerted', %s)
                """, (tmdb_id, title, media_type, item.get('popularity', 0), item.get('vote_average', 0), BOT_INSTANCE_ID, imdb_id))
                
                conn.commit()

            await asyncio.sleep(3)  # Flood control

        # Send summary to admin
        summary = build_summary_message(new_alerts, total, skipped_in_db, skipped_already)
        try:
            await app.bot.send_message(chat_id=admin_id, text=summary, parse_mode='HTML')
            logger.info("📊 Summary sent to admin")
        except Exception as e:
            logger.error(f"❌ Summary send failed: {e}")

        cur.close()
        update_last_check()
        
    except Exception as e:
        logger.error(f"❌ check_and_alert_trending error: {e}", exc_info=True)
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
        logger.error("❌ DB setup failed, worker stopping")
        return

    # Send startup message
    next_run_utc = get_next_run_time_ist()
    tz = pytz.timezone('Asia/Kolkata')
    next_ist = next_run_utc.astimezone(tz)
    time_str = next_ist.strftime("%I:%M %p IST")

    startup_msg = (
        f"🚀 <b>TRENDING MONITOR ACTIVE</b>\n\n"
        f"🌍 <b>Now Checking:</b> Worldwide + 🇮🇳 India Regional Trends\n"
        f"🔍 <b>Match Priority:</b>\n"
        f"   1️⃣ IMDb ID (Most Accurate)\n"
        f"   2️⃣ Exact Title Match\n\n"
        f"🕒 <b>Schedule:</b> 00:00, 06:00, 12:00, 18:00 IST\n"
        f"⏱️ <b>Next Check:</b> <code>{time_str}</code>\n\n"
        f"✅ <b>Features:</b>\n"
        f"   • No duplicate posts\n"
        f"   • IMDb verification\n"
        f"   • Cinematic posters\n"
        f"   • Smart alerts\n\n"
        f"💤 Sleeping until next scheduled time..."
    )
    
    try:
        await app.bot.send_message(chat_id=admin_id, text=startup_msg, parse_mode='HTML')
        logger.info(f"📨 Startup message sent to {admin_id}")
    except Exception as e:
        logger.error(f"❌ Failed to send startup message: {e}")

    # Main loop
    while True:
        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)
        next_run_utc = get_next_run_time_ist()
        sleep_seconds = (next_run_utc - now_utc).total_seconds()
        
        if sleep_seconds < 0:
            sleep_seconds = 0

        logger.info(f"💤 Sleeping for {sleep_seconds/3600:.2f} hours until {next_run_utc.astimezone(tz).strftime('%I:%M %p IST')}")
        await asyncio.sleep(sleep_seconds)

        logger.info("🔍 Running scheduled trending check (Worldwide + India)...")
        try:
            await check_and_alert_trending(app, admin_id)
        except Exception as e:
            logger.error(f"❌ Trending check failed: {e}", exc_info=True)
            try:
                await app.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ <b>Trending Check Error</b>\n\n<code>{str(e)}</code>",
                    parse_mode='HTML'
                )
            except:
                pass
