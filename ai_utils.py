import asyncio
import re
import json
from typing import Tuple, Optional
import google.generativeai as genai
from .config import Config

gemini_model = None

def init_gemini():
    """Initializes the Gemini AI model correctly."""
    global gemini_model
    if Config.GEMINI_API_KEY:
        genai.configure(api_key=Config.GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-1.5-flash')
        print("Gemini AI model initialized.")

async def analyze_message_intent(message_text: str) -> Tuple[bool, Optional[str]]:
    """Uses Gemini to analyze the user's intent and extract the movie title."""
    if not gemini_model:
        return False, None

    prompt = f"""
    You are a 'Request Analyzer' for a Telegram bot. Your ONLY purpose is to provide MOVIES and WEB SERIES.
    Analyze the user's message to determine if they are asking for a movie or web series.
    - If it's a request, respond with: {{"is_request": true, "content_title": "Name of the Movie"}}
    - If it's anything else (greeting, question, article), respond with: {{"is_request": false, "content_title": null}}
    Respond ONLY with the JSON object.

    User's Message: "{message_text}"
    """
    try:
        # Run the blocking API call in a separate thread
        response = await asyncio.to_thread(gemini_model.generate_content, prompt)
        
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return result.get('is_request', False), result.get('content_title')
        return False, None
    except Exception as e:
        print(f"AI analysis error: {e}")
        return False, None
