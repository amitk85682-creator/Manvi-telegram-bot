# -*- coding: utf-8 -*-
import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# Logging setup
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Token
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

async def redirect_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User kuch bhi kare, usse ye hi message milega"""
    
    offline_text = (
        "⚠️ **Bot moved to New Address** ⚠️\n\n"
        "Technical reasons ki wajah se ye bot band kar diya gaya hai.\n\n"
        "Kripya niche diye gaye hamare active bots ka use karein. "
        "Wahan aapko saari movies aur series mil jayengi: 👇"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Go to FilmfyBox Bot", url="https://t.me/FlimfyBox_Bot?start=start")],
        [InlineKeyboardButton("🔍 Go to FilmfyBox SearchBot", url="https://t.me/FlimfyBox_SearchBot?start=start")],
        [InlineKeyboardButton("⚡ Go to Movie Bot", url="https://t.me/urmoviebot?start=start")]
    ])

    if update.message:
        await update.message.reply_text(offline_text, reply_markup=keyboard, parse_mode='Markdown')
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(offline_text, reply_markup=keyboard, parse_mode='Markdown')

def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not found!")
        return

    # Bot setup
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Sirf 3 handlers jo sab kuch redirect karenge
    application.add_handler(CommandHandler("start", redirect_user))
    application.add_handler(MessageHandler(filters.ALL, redirect_user))
    application.add_handler(CallbackQueryHandler(redirect_user))

    # Bot Start
    print("Bot is running in Redirect-Only mode...")
    application.run_polling()

if __name__ == '__main__':
    main()
