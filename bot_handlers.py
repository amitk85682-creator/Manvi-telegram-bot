from telegram import Update
from telegram.ext import ContextTypes
from ai_utils import analyze_intent
from fuzzy_search import search_movies
from keyboards import create_movie_keyboard, create_movie_options
from database import get_cursor

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 Welcome to FilmFyBot!\n\n"
        "Send me any movie name and I'll find it for you!"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text.strip()
    
    # Analyze intent
    analysis = analyze_intent(user_message)
    
    if not analysis["is_request"]:
        return  # Ignore non-movie requests
    
    # Search for movies
    search_query = analysis["content_title"] or user_message
    movies = search_movies(search_query)
    
    if not movies:
        # Store request
        user = update.effective_user
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO user_requests (user_id, username, first_name, movie_title) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (user.id, user.username, user.first_name, search_query)
            )
        
        await update.message.reply_text(
            f"😔 '{search_query}' is not available yet.\n"
            "I'll notify you when it's added! 🔔"
        )
    elif len(movies) == 1:
        title, url = movies[0]
        await update.message.reply_text(
            f"🎉 Found '{title}'!",
            reply_markup=create_movie_keyboard(title, url)
        )
    else:
        await update.message.reply_text(
            "Multiple matches found. Select one:",
            reply_markup=create_movie_options(movies)
        )

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith("movie_"):
        url = query.data.replace("movie_", "")
        await query.message.reply_text(f"🎬 Here's your movie: {url}")
