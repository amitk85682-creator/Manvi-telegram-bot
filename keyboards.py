from telegram import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton

def get_main_menu_keyboard():
    keyboard = [
        ["🔍 Search Movies"],
        ["🙋 Request Movie"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_movie_keyboard(title, url):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Download", url=url)]
    ])

def get_movie_options(movies):
    keyboard = []
    for title, url in movies:
        keyboard.append([InlineKeyboardButton(title, url=url)])
    return InlineKeyboardMarkup(keyboard)
