# File: bot_handlers.py
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from ai_utils import analyze_intent
from database import search_movies, store_user_request

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🎬 Welcome to FilmFyBot! Send me a movie name.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text.strip()
    analysis = await analyze_intent(user_message)
    
    if not analysis.get("is_request") or not analysis.get("content_title"):
        return # अगर यह मूवी रिक्वेस्ट नहीं है तो अनदेखा करें

    search_query = analysis["content_title"]
    movies = await search_movies(search_query)
    
    if not movies:
        await store_user_request(update.effective_user.id, search_query)
        await update.message.reply_text(f"😔 '{search_query}' not available. I'll notify you if it's added!")
    elif len(movies) == 1:
        title, url = movies[0]
        keyboard = [[InlineKeyboardButton("📥 Download", url=url)]]
        await update.message.reply_text(f"🎉 Found '{title}'!", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        keyboard = [[InlineKeyboardButton(title, url=url)] for title, url in movies]
        await update.message.reply_text("🎬 Similar movies found:", reply_markup=InlineKeyboardMarkup(keyboard))

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"Update {update} caused error {context.error}")
