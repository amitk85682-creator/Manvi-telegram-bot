from telegram import InlineKeyboardButton, InlineKeyboardMarkup

def create_movie_keyboard(title, url):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Watch Now", url=url)],
        [InlineKeyboardButton("⭐ Rate This Movie", callback_data=f"rate_{title}")]
    ])

def create_movie_options(movies):
    keyboard = []
    for title, url in movies:
        keyboard.append([InlineKeyboardButton(title, callback_data=f"movie_{url}")])
    return InlineKeyboardMarkup(keyboard)
