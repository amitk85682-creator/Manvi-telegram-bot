import os, logging, threading, psycopg2, asyncio
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from thefuzz import process

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Flask health stub (keeps Render happy) ----------
app = Flask(__name__)
@app.route("/")          def health(): return "Bot alive", 200
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

# ---------- DB ----------
def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")

# ---------- Persistent keyboard ----------
def PERSISTENT_KB():
    return ReplyKeyboardMarkup([["🔍 Search Movies", "🙋 Request Movie"]],
                               resize_keyboard=True, persistent=True)

# ---------- Handlers ----------
async def start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🎬 Use the buttons below:", reply_markup=PERSISTENT_KB())

async def search(update: Update, _: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if len(text) < 2: return

    conn = get_conn()
    with conn.cursor() as cur:
        # 1. exact
        cur.execute("SELECT title, url FROM movies WHERE lower(title)=lower(%s)", (text,))
        row = cur.fetchone()
        if row:
            title, url = row
            await update.message.reply_text(f"🎉 Found *{title}*",
                                            parse_mode="Markdown",
                                            reply_markup=InlineKeyboardMarkup(
                                                [[InlineKeyboardButton("📥 Download", url=url)]]))
            return

        # 2. fuzzy
        cur.execute("SELECT title, url FROM movies")
        all_movies = cur.fetchall()
        titles = [t for t, _ in all_movies]
        match, score = process.extractOne(text, titles) or ("", 0)
        if score >= 75:
            url = dict(all_movies)[match]
            await update.message.reply_text(f"Did you mean *{match}*?",
                                            parse_mode="Markdown",
                                            reply_markup=InlineKeyboardMarkup(
                                                [[InlineKeyboardButton("📥 Download", url=url)]]))
            return

        # 3. partial
        cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 5", (f"%{text}%",))
        rows = cur.fetchall()
        if rows:
            kb = [[InlineKeyboardButton(t, url=u)] for t, u in rows]
            await update.message.reply_text("🔍 Select:", reply_markup=InlineKeyboardMarkup(kb))
            return

        await update.message.reply_text("😔 Not found. Request it via *Request Movie* button.",
                                        parse_mode="Markdown")
    conn.close()

# ---------- Main ----------
def main():
    threading.Thread(target=run_flask, daemon=True).start()   # keep Render happy
    app = Application.builder().token(os.environ["TELEGRAM_BOT_TOKEN"]).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search))
    logger.info("Bot polling...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
