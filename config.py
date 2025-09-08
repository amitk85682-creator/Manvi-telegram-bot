import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    DATABASE_URL = os.environ.get('DATABASE_URL')
    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
    BLOGGER_API_KEY = os.environ.get('BLOGGER_API_KEY')
    BLOG_ID = os.environ.get('BLOG_ID')
    ADMIN_ID = int(os.environ.get('ADMIN_USER_ID', 0))
    GROUP_CHAT_ID = os.environ.get('GROUP_CHAT_ID')
    UPDATE_SECRET = os.environ.get('UPDATE_SECRET_CODE', 'default_secret_123')
