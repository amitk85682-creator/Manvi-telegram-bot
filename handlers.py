from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
# ... other imports for search_movies, send_movie_content etc.

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... AI intent analysis logic comes here ...
    is_request, content_title = await analyze_message_intent(update.message.text)
    
    if is_request and content_title:
        movies = await search_movies(content_title)

        if not movies:
            await update.message.reply_text(f"Sorry, '{content_title}' not found.")
        elif len(movies) == 1:
            # Only one result, send it directly
            title, url = movies[0]
            await send_movie_content(update.message, context, title, url)
        else:
            # Multiple results, show buttons
            await send_movie_options(update, movies)

async def send_movie_options(update: Update, movies: List[Tuple[str, str]]):
    """Sends a message with inline buttons for movie selection."""
    keyboard = []
    for title, url in movies:
        # The callback_data should be unique, using the URL is a good way
        keyboard.append([InlineKeyboardButton(title, callback_data=f"movie_{url}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "I found a few matches, please select one:",
        reply_markup=reply_markup
    )
