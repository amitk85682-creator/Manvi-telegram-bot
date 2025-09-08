import google.generativeai as genai
import json
import re
from config import Config

if Config.GEMINI_API_KEY:
    genai.configure(api_key=Config.GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-pro')
else:
    model = None

def analyze_intent(message_text):
    if not model:
        return fallback_analysis(message_text)
    
    prompt = f"""
    You are a 'Request Analyzer' for a Telegram bot. Your only purpose is to analyze user messages 
    and determine if they are requesting a movie or web series.
    
    Respond ONLY with a JSON object in this format:
    {{
        "is_request": boolean,
        "content_title": string or null
    }}
    
    Rules:
    1. If the user is asking for a movie/series, set "is_request": true and extract the title in "content_title"
    2. If it's general conversation, greetings, or other topics, set "is_request": false and "content_title": null
    3. Never explain your reasoning. Only return the JSON.
    
    User's Message: "{message_text}"
    """
    
    try:
        response = model.generate_content(prompt)
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"AI analysis error: {e}")
    
    return fallback_analysis(message_text)

def fallback_analysis(message_text):
    movie_keywords = ['movie', 'film', 'series', 'download', 'watch', 'see']
    if any(keyword in message_text.lower() for keyword in movie_keywords):
        return {"is_request": True, "content_title": message_text}
    return {"is_request": False, "content_title": None}
