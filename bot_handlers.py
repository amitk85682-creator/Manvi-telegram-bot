import logging
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler, MessageHandler, CommandHandler, filters
from telegram.error import BadRequest

from config import Config
from database import get_cursor, log_user_request
from ai_utils import analyze_intent
from keyboards import get_main_menu_keyboard, get_movie_keyboard, get_movie_options

# Configure logging
logger = logging.getLogger(__name__)

# Conversation states
MAIN_MENU, SEARCHING, REQUESTING = range(3)

# Start command and main menu
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = """
🎬 Welcome to FilmFyBot!

I can help you find movies and request new ones.

🔍 Use "Search Movies" to find existing movies
🙋 Use "Request Movie" to ask for new movies

Choose an option below to get started!
"""
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=get_main_menu_keyboard()
    )
    return MAIN_MENU

# Handle main menu options
async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "🔍 Search Movies":
        await update.message.reply_text(
            "Great! Tell me the name of the movie you want to search for:",
            reply_markup=get_main_menu_keyboard()
        )
        return SEARCHING
        
    elif text == "🙋 Request Movie":
        await update.message.reply_text(
            "Okay! Please tell me the name of the movie you'd like to request:",
            reply_markup=get_main_menu_keyboard()
        )
        return REQUESTING
    
    return MAIN_MENU

# Handle search requests
async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    movie_query = update.message.text
    
    # Analyze intent to filter non-movie messages
    analysis = analyze_intent(movie_query)
    
    if not analysis["is_request"]:
        await update.message.reply_text(
            "That doesn't seem to be a movie title. Please provide a valid movie name to search for.",
            reply_markup=get_main_menu_keyboard()
        )
        return SEARCHING
    
    # Search for movies
    search_query = analysis["content_title"] or movie_query
    movies = search_movies(search_query)
    
    if not movies:
        await update.message.reply_text(
            f"😔 '{search_query}' is not available yet.\n\nUse the '🙋 Request Movie' button to ask for it!",
            reply_markup=get_main_menu_keyboard()
        )
    elif len(movies) == 1:
        title, url = movies[0]
        await update.message.reply_text(
            f"🎉 Found '{title}'!",
            reply_markup=get_movie_keyboard(title, url)
        )
    else:
        await update.message.reply_text(
            "Multiple matches found. Select one:",
            reply_markup=get_movie_options(movies)
        )
    
    return MAIN_MENU

# Handle movie requests
async def handle_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    movie_request = update.message.text
    
    # Analyze intent to filter non-movie messages
    analysis = analyze_intent(movie_request)
    
    if not analysis["is_request"]:
        await update.message.reply_text(
            "That doesn't seem to be a movie title. Please provide a valid movie name to request.",
            reply_markup=get_main_menu_keyboard()
        )
        return REQUESTING
    
    movie_title = analysis["content_title"] or movie_request
    
    # Log the request
    user = update.effective_user
    chat = update.effective_chat
    
    group_id = chat.id if chat.type != "private" else None
    group_name = chat.title if chat.type != "private" else None
    
    log_user_request(
        user.id, user.username, user.first_name,
        movie_title, group_id, group_name
    )
    
    # Send notification to admin channel
    await notify_admin(user, movie_title, chat)
    
    await update.message.reply_text(
        f"✅ Got it! Your request for '{movie_title}' has been sent to the admin. "
        "Thanks for helping improve our collection!",
        reply_markup=get_main_menu_keyboard()
    )
    
    return MAIN_MENU

# Search movies in database
def search_movies(query, limit=5):
    with get_cursor() as cur:
        # Try exact match first
        cur.execute(
            "SELECT title, url FROM movies WHERE LOWER(title) = LOWER(%s) LIMIT 1",
            (query,)
        )
        exact_match = cur.fetchone()
        if exact_match:
            return [exact_match]
        
        # Try partial match
        cur.execute(
            "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
            (f'%{query}%', limit)
        )
        return cur.fetchall()

# Notify admin about new request
async def notify_admin(user, movie_title, chat):
    if not Config.ADMIN_CHANNEL_ID:
        return
    
    try:
        message = f"""
🎬 New Movie Request!

📽️ Movie: {movie_title}
👤 User: {user.first_name} (@{user.username})
💬 From: {'Group: ' + chat.title if chat.type != 'private' else 'Private Chat'}
🕒 Time: {update.message.date.strftime('%Y-%m-%d %I:%M %p')}
"""
        
        await context.bot.send_message(
            chat_id=Config.ADMIN_CHANNEL_ID,
            text=message
        )
    except BadRequest:
        logger.warning("Admin channel not configured properly")

# Cancel conversation
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Operation cancelled.",
        reply_markup=get_main_menu_keyboard()
    )
    return MAIN_MENU

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}")
    
    if update and update.message:
        await update.message.reply_text(
            "Sorry, something went wrong. Please try again.",
            reply_markup=get_main_menu_keyboard()
        )
    
    return MAIN_MENU
