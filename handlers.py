async def send_movie_file(update: Update, context: ContextTypes.DEFAULT_TYPE, title, url):
    try:
        # Check if URL is valid
        if not url.startswith(('http://', 'https://')):
            await update.message.reply_text("❌ Invalid download URL")
            return
        
        # Send as direct link instead of file
        keyboard = [[InlineKeyboardButton("📥 Direct Download", url=url)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"🎬 {title}\n\n"
            "Click the button below to download:",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"File send error: {e}")
        await update.message.reply_text(
            "❌ Download failed. Please try again later or request this movie."
        )
