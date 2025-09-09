from telegram import ReplyKeyboardMarkup

# Permanent keyboard for all messages
def get_permanent_keyboard():
    keyboard = [
        ["🔍 Search Movies", "🙋 Request Movie"],
        ["📋 Help", "⭐ Rate Us"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, persistent=True)

# Start command में permanent keyboard add करें
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to FilmFyBot!\n\nUse the buttons below to navigate:",
        reply_markup=get_permanent_keyboard()
    )
