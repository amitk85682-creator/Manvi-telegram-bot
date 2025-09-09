import os, logging, threading
from flask import Flask
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application, ConversationHandler, MessageHandler,
    filters, ContextTypes, CallbackQueryHandler
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Flask health stub ----------
app = Flask(__name__)
@app.route("/")
def health():
    return "Bot alive", 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)

# ---------- States ----------
MAIN_MENU, SEARCHING, REQUESTING = range(3)

# ---------- Persistent buttons ----------
def main_menu_kb():
    return ReplyKeyboardMarkup(
        [["🔍 Search Movies", "🙋 Request Movie"]],
        resize_keyboard=True,
        persistent=True,
        one_time_keyboard=False
    )

# ---------- Handlers ----------
async def start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Use the buttons below to interact:",
        reply_markup=main_menu_kb()
    )
    return MAIN_MENU

# 🔍 Search button pressed
async def search_pressed(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Great! Send me the movie name to search:")
    return SEARCHING

# 🙋 Request button pressed
async def request_pressed(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Okay! Send me the movie name to request:")
    return REQUESTING

# Actual search logic
async def handle_search(update: Update, _: ContextTypes.DEFAULT_TYPE):
    movie = update.message.text.strip()
    # dummy reply – plug your DB here
    await update.message.reply_text(f"🔍 Searching for: {movie}")
    return MAIN_MENU

# Actual request logic
async def handle_request(update: Update, _: ContextTypes.DEFAULT_TYPE):
    movie = update.message.text.strip()
    # dummy reply – plug your DB + admin notify here
    await update.message.reply_text(f"✅ Request sent for: {movie}")
    return MAIN_MENU

# ❌ Ignore any text message that did NOT come from button press
async def silent_fallback(update: Update, _: ContextTypes.DEFAULT_TYPE):
    # completely silent – no reply at all
    return MAIN_MENU

# ---------- Main ----------
def main():
    threading.Thread(target=run_flask, daemon=True).start()

    application = Application.builder().token(os.environ["TELEGRAM_BOT_TOKEN"]).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [
                MessageHandler(filters.Regex("^🔍 Search Movies$"), search_pressed),
                MessageHandler(filters.Regex("^🙋 Request Movie$"), request_pressed),
            ],
            SEARCHING: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search)],
            REQUESTING: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_request)],
        },
        fallbacks=[MessageHandler(filters.ALL, silent_fallback)],
        allow_reentry=True,
        per_user=True,
        per_chat=True,
    )

    application.add_handler(conv_handler)
    logger.info("Bot polling (silent unless button pressed)...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
