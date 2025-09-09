# File: ai_utils.py
import google.generativeai as genai
import json
import re
import asyncio
from config import Config

model = None

def init_gemini():
    """Gemini AI मॉडल को शुरू करता है।"""
    global model
    if Config.GEMINI_API_KEY:
        try:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            # इस काम के लिए Flash मॉडल तेज़ और सस्ता है
            model = genai.GenerativeModel('gemini-1.5-flash')
            print("Gemini AI model initialized.")
        except Exception as e:
            print(f"Gemini initialization failed: {e}")

async def analyze_intent(message_text: str):
    """AI का उपयोग करके यूज़र के इरादे का विश्लेषण करता है।"""
    if not model:
        return {"is_request": True, "content_title": message_text} # AI न होने पर फॉलबैक

    prompt = f"""
    You are a 'Request Analyzer' for a movie bot. Your goal is to determine if a user is asking for a movie/series.
    - If it's a clear request (e.g., "Kalki movie," "download Sholay"), respond with: {{"is_request": true, "content_title": "Name of Movie"}}
    - If it's a greeting ("hi", "hello manvi"), general chat ("how are you"), or spam, respond with: {{"is_request": false, "content_title": null}}
    - If the user says something complex like "शाहरुख खान की नई वाली पिक्चर चाहिए", extract the likely movie title.
    Respond ONLY with the JSON.
    User Message: "{message_text}"
    """
    try:
        # API कॉल को अलग थ्रेड में चलाकर बॉट को फ्रीज होने से बचाएं
        response = await asyncio.to_thread(model.generate_content, prompt)
        clean_text = response.text.strip().replace("```json", "").replace("```", "")
        json_match = re.search(r'\{.*\}', clean_text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"AI analysis error: {e}")
    
    # AI फेल होने पर फॉलबैक
    return {"is_request": True, "content_title": message_text}
