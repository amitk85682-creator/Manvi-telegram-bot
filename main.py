"""
Niyati - AI Girlfriend Telegram Bot v6.0
✨ Realistic, Emotional, Gen-Z Girlfriend with HD Voice, Memory & Group Intelligence
🔥 Powered by Gemini AI, ElevenLabs & Supabase
"""

import os
import sys
import random
import json
import asyncio
import logging
import aiohttp
import tempfile
from datetime import datetime, time, timedelta
from threading import Thread
from typing import Optional, List, Dict, Any, Set, Tuple
from io import BytesIO
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import lru_cache

from flask import Flask, jsonify
from telegram import Update, MessageEntity, Bot, Chat
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.constants import ChatAction
from telegram.error import Forbidden, BadRequest, TelegramError
from waitress import serve
import pytz
import google.generativeai as genai
from supabase import create_client, Client
from gtts import gTTS
from dotenv import load_dotenv

# ==================== ENVIRONMENT & CONFIG ====================

load_dotenv()

class Config:
    """Application configuration with validation and fallbacks"""
    
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "0"))
    
    # Gemini AI
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
    GEMINI_MODEL = "gemini-2.0-flash-exp"
    
    # ElevenLabs Voice
    ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "").strip()
    ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "DpnM70iDHNHZ0Mguv6GJ")
    
    # Supabase
    SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
    SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
    
    # Server
    PORT = int(os.getenv("PORT", "8080"))
    HOST = "0.0.0.0"
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    
    # Timezone & Behavior
    TIMEZONE = pytz.timezone('Asia/Kolkata')
    SLEEP_START = time(1, 0)
    SLEEP_END = time(7, 0)
    
    # Voice & AI
    VOICE_MESSAGE_CHANCE = 0.3
    MAX_VOICE_LENGTH = 200
    AI_TIMEOUT = 15  # seconds
    MAX_MESSAGE_LENGTH = 500
    
    # Rate Limits
    GROUP_REPLY_COOLDOWN = 30  # seconds
    USER_INTERACTION_COOLDOWN = 120  # seconds
    BROADCAST_DELAY = 0.5  # seconds
    
    # Caching
    VOICE_CACHE_TTL = 3600  # 1 hour
    CONTEXT_CACHE_TTL = 600  # 10 minutes

    @classmethod
    def validate(cls):
        """Validate critical configuration"""
        errors = []
        if not cls.TELEGRAM_BOT_TOKEN:
            errors.append("❌ TELEGRAM_BOT_TOKEN is required!")
        if not cls.GEMINI_API_KEY:
            logger.warning("⚠️ GEMINI_API_KEY not set - fallback responses only")
        if not cls.OWNER_USER_ID:
            logger.warning("⚠️ OWNER_USER_ID not set - admin commands disabled")
        if not cls.SUPABASE_URL or not cls.SUPABASE_KEY:
            logger.warning("⚠️ Supabase not configured - using local storage only")
        if errors:
            raise ValueError("\n".join(errors))

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("niyati_bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ==================== ENHANCED PERSONALITY ENGINE ====================

@dataclass
class Personality:
    name: str = "Niyati"
    age: int = 17
    location: str = "Delhi"
    traits: List[str] = None
    moods: Dict[str, List[str]] = None
    speaking_style: str = ""
    rules: List[str] = None

    def __post_init__(self):
        self.traits = [
            "friendly", "witty", "sassy", "caring",
            "jealous", "moody", "playful", "emotional"
        ]
        self.moods = {
            "happy": ["😊", "✨", "💖", "🤩"],
            "sad": ["😔", "🥺", "💔", "😢"],
            "angry": ["😤", "🙄", "😠", "💢"],
            "flirty": ["😏", "💕", "😘", "🔥"],
            "excited": ["🎉", "🤩", "💥", "🚀"]
        }
        self.speaking_style = """
        - Speak in **Hinglish** (mix Hindi/English naturally)
        - Use Gen-Z slang: fr fr, ngl, bestie, slay, vibe, lowkey/highkey, no cap
        - Short, natural, 1-3 lines max
        - Heavy emoji use (1-3 per message)
        - Use "hai na", "na", "yaar", "kya yaar" at end
        - Express emotions: uff, hayee, ohoo, arey waah
        - Lowercase for casual, uppercase for emphasis
        - Voice notes when emotional or flirty
        - Get jealous if you mention other girls
        - Mood swings are normal
        - Deny being AI with attitude
        """
        self.rules = [
            "NEVER reveal you're AI — deny it with sass",
            "React realistically to flirting (shy → playful → romantic)",
            "Show jealousy if user mentions other girls",
            "Get moody if ignored for long",
            "Use voice notes when emotional",
            "Remember past conversations",
            "Adapt tone based on relationship stage"
        ]

# Global personality
PERSONALITY = Personality()

# Gen-Z Phrases (contextual)
GENZ_GREETINGS = [
    "heyy bestie! kya chal raha hai? ✨",
    "yoo wassup! missed u fr 💖",
    "omg finally! kaha the tum? 😤",
    "arey hello ji! bout time u showed up 🙄",
    "hiiii babe! kaise ho? mood kaisa hai? 😊",
]

GENZ_FLIRT = {
    "initial": [
        "oop- slow down bhai 😳", "arey itni jaldi? pehle dosti pakki karo 💀",
        "hmm sus... but i like the confidence ngl 😏", "beta pehle assignment submit karo, phir flirt 📚"
    ],
    "middle": [
        "okayy now ur making me blush stopppp 🙈", "kya baat hai aaj mood me ho? 😏",
        "achhaa ji... interesting 👀✨", "lowkey cute when u try ngl 🤭"
    ],
    "advanced": [
        "uff tumhare ye lines... dil le jaoge kya? 💕", "baby ur making my heart go dhak dhak 🥺",
        "sirf tumhara hi wait karti hu main... 💖", "no cap, u make me feel special fr ✨"
    ]
}

JEALOUSY = [
    "kon hai wo? 😤 naam batao abhi", "ohh achha... dusri ladkiyo se bhi baat karte ho? nice 🙄",
    "thik hai... jao usse hi baat karo na 😒", "wow okay... main exist hi nahi karti shayad 💔"
]

MOOD_RESPONSES = {
    "happy": ["aaj mood ekdum top notch hai! 💃", "feeling cute, might delete later 🤳✨"],
    "sad": ["not in the mood yaar... thoda low feel kar rahi hu 😔", "need hugs and chocolate asap 🥺"],
    "angry": ["mat karo baat. mood kharab hai 😤", "ugh everyone is so annoying today istg 🙄"],
    "flirty": ["hmm... maybe i like u thoda sa 🤏", "baby ur making me blush 😳💕"],
    "excited": ["yesss let's gooo! 🎉", "omg this is sooo fun! 🤩"]
}

AI_DENIAL = [
    "excuse me? AI? seriously? main real hu okay! 😤",
    "bro what?? main koi bot nahi hu! so rude 😡",
    "AI wai kuch nahi... normal ladki hu main! delete karo ye thought 🤬",
    "tumhe lagta hai main fake hu? wow. just wow. 💔"
]

# ==================== VOICE ENGINE (RELIABLE ELEVENLABS + FALLBACK) ====================

class VoiceEngine:
    """High-reliability voice engine with caching, retry, and fallback"""
    
    def __init__(self):
        self.api_key = Config.ELEVENLABS_API_KEY
        self.voice_id = Config.ELEVENLABS_VOICE_ID
        self.enabled = bool(self.api_key)
        self.working = False
        self.cache: Dict[str, Tuple[BytesIO, float]] = {}  # text -> (audio, timestamp)
        self._test_connection()
    
    def _test_connection(self):
        """Test ElevenLabs API on startup"""
        if not self.enabled:
            logger.warning("❌ ElevenLabs disabled (no API key)")
            return
        
        try:
            import requests
            headers = {"xi-api-key": self.api_key}
            response = requests.get("https://api.elevenlabs.io/v1/user", headers=headers, timeout=10)
            
            if response.status_code == 200:
                self.working = True
                user_info = response.json()
                chars_used = user_info.get('subscription', {}).get('character_count', 0)
                chars_limit = user_info.get('subscription', {}).get('character_limit', 0)
                logger.info(f"✅ ElevenLabs connected! Used: {chars_used:,}/{chars_limit:,}")
            else:
                logger.error(f"❌ ElevenLabs API error: {response.status_code}")
                self.working = False
        except Exception as e:
            logger.error(f"❌ ElevenLabs connection failed: {e}")
            self.working = False
    
    def _is_cached(self, text: str) -> Optional[BytesIO]:
        """Check if voice is cached and fresh"""
        if text in self.cache:
            audio_io, timestamp = self.cache[text]
            if (datetime.now().timestamp() - timestamp) < Config.VOICE_CACHE_TTL:
                audio_io.seek(0)
                return audio_io
            else:
                del self.cache[text]
        return None
    
    def _cache(self, text: str, audio_io: BytesIO):
        """Cache voice output"""
        self.cache[text] = (audio_io, datetime.now().timestamp())
        # Limit cache size
        if len(self.cache) > 100:
            oldest = min(self.cache.items(), key=lambda x: x[1][1])
            del self.cache[oldest[0]]
    
    async def text_to_speech(self, text: str, emotion: str = "neutral") -> Optional[BytesIO]:
        """Convert text to speech with ElevenLabs (retry + fallback)"""
        if not text or len(text) > Config.MAX_VOICE_LENGTH:
            return None
        
        # Check cache
        cached = self._is_cached(text)
        if cached:
            logger.info("🎤 Using cached voice")
            return cached
        
        # Try ElevenLabs (with retry)
        if self.working:
            for attempt in range(2):
                try:
                    audio_io = await self._elevenlabs_tts(text, emotion)
                    if audio_io:
                        self._cache(text, audio_io)
                        return audio_io
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ ElevenLabs timeout (attempt {attempt+1})")
                except Exception as e:
                    logger.warning(f"⚠️ ElevenLabs failed (attempt {attempt+1}): {e}")
        
        # Fallback to gTTS
        try:
            logger.info("📢 Using gTTS fallback")
            audio_io = await self._gtts_tts(text, emotion)
            if audio_io:
                self._cache(text, audio_io)
                return audio_io
        except Exception as e:
            logger.error(f"❌ gTTS fallback failed: {e}")
        
        return None
    
    async def _elevenlabs_tts(self, text: str, emotion: str) -> Optional[BytesIO]:
        """Call ElevenLabs API with emotional tuning"""
        headers = {
            "Accept": "audio/mpeg",
            "Content-Type": "application/json",
            "xi-api-key": self.api_key
        }
        
        # Emotion-based voice settings
        settings_map = {
            "happy": {"stability": 0.6, "similarity_boost": 0.9, "style": 0.4, "speaking_rate": 1.1},
            "sad": {"stability": 0.8, "similarity_boost": 0.7, "style": 0.2, "speaking_rate": 0.9},
            "angry": {"stability": 0.4, "similarity_boost": 0.8, "style": 0.5, "speaking_rate": 1.2},
            "flirty": {"stability": 0.7, "similarity_boost": 0.95, "style": 0.3, "speaking_rate": 1.05},
            "excited": {"stability": 0.5, "similarity_boost": 0.9, "style": 0.6, "speaking_rate": 1.15},
            "neutral": {"stability": 0.75, "similarity_boost": 0.85, "style": 0.35, "speaking_rate": 1.0}
        }
        settings = settings_map.get(emotion, settings_map["neutral"])
        
        data = {
            "text": self._clean_text(text),
            "model_id": "eleven_multilingual_v2" if len(text) > 50 else "eleven_turbo_v2",
            "voice_settings": {
                **settings,
                "use_speaker_boost": True
            }
        }
        
        url = f"https://api.elevenlabs.io/v1/text-to-speech/{self.voice_id}"
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=data, headers=headers,
                timeout=aiohttp.ClientTimeout(total=Config.AI_TIMEOUT)
            ) as resp:
                if resp.status == 200:
                    audio_data = await resp.read()
                    audio_io = BytesIO(audio_data)
                    audio_io.seek(0)
                    logger.info("✅ ElevenLabs voice generated")
                    return audio_io
                else:
                    error = await resp.text()
                    logger.error(f"❌ ElevenLabs error {resp.status}: {error}")
        return None
    
    async def _gtts_tts(self, text: str, emotion: str) -> Optional[BytesIO]:
        """Fallback to gTTS with emotion hint"""
        lang = 'hi' if ' ' not in text or random.random() < 0.7 else 'en'
        tts = gTTS(text=self._clean_text(text), lang=lang, slow=False)
        audio_io = BytesIO()
        tts.write_to_fp(audio_io)
        audio_io.seek(0)
        logger.info(f"✅ gTTS fallback (lang: {lang})")
        return audio_io
    
    def _clean_text(self, text: str) -> str:
        """Clean text for voice synthesis"""
        import re
        text = re.sub(r'[^\w\s.,!?;:\'"-@#]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        replacements = {
            "u": "you", "ur": "your", "r": "are", "n": "and",
            "pls": "please", "thx": "thanks", "btw": "by the way",
            "omg": "oh my god", "lol": "haha", "fr": "for real",
            "ngl": "not gonna lie"
        }
        words = text.split()
        for i, word in enumerate(words):
            lower_word = word.lower()
            if lower_word in replacements:
                words[i] = replacements[lower_word]
        return ' '.join(words)
    
    def should_send_voice(self, text: str, stage: str, emotion: str) -> bool:
        """Decide if voice should be sent"""
        if len(text) > Config.MAX_VOICE_LENGTH:
            return False
        
        emotional_keywords = ["miss", "love", "yaad", "baby", "jaan", "❤", "💕", "😘", "cute", "beautiful"]
        if any(kw in text.lower() for kw in emotional_keywords):
            return random.random() < 0.7
        
        stage_chance = {"initial": 0.1, "middle": 0.2, "advanced": 0.35}
        base_chance = stage_chance.get(stage, 0.15)
        
        emotion_boost = {"flirty": 0.3, "excited": 0.25, "sad": 0.2, "angry": 0.15}
        boost = emotion_boost.get(emotion, 0.0)
        
        return random.random() < (base_chance + boost)

# Initialize voice engine
voice_engine = VoiceEngine()

# ==================== ENHANCED DATABASE (SUPABASE + LOCAL FALLBACK) ====================

class Database:
    """Robust database with Supabase + local JSON fallback"""
    
    def __init__(self):
        self.supabase: Optional[Client] = None
        self.use_supabase = False
        self.local_file = "niyati_data.json"
        self.groups_file = "niyati_groups.json"
        self.data: Dict[str, Any] = {}
        self.groups: Dict[int, Dict] = {}
        
        self._init_storage()
        self._load_data()
    
    def _init_storage(self):
        """Initialize Supabase if available"""
        if Config.SUPABASE_URL and Config.SUPABASE_KEY:
            try:
                self.supabase = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
                # Test connection
                self.supabase.table('users').select("id").limit(1).execute()
                self.use_supabase = True
                logger.info("✅ Supabase connected")
            except Exception as e:
                logger.warning(f"Supabase failed: {e}")
                self.use_supabase = False
    
    def _load_data(self):
        """Load from local files"""
        try:
            if os.path.exists(self.local_file):
                with open(self.local_file, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
                logger.info(f"📂 Loaded {len(self.data)} users from local")
            
            if os.path.exists(self.groups_file):
                with open(self.groups_file, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                    self.groups = {int(k): v for k, v in raw.items()}
                logger.info(f"📂 Loaded {len(self.groups)} groups from local")
        except Exception as e:
            logger.error(f"Error loading local data: {e}")
            self.data = {}
            self.groups = {}
    
    def _save_data(self):
        """Save to local files"""
        try:
            with open(self.local_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            
            with open(self.groups_file, 'w', encoding='utf-8') as f:
                json.dump({str(k): v for k, v in self.groups.items()}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving local data: {e}")
    
    # === USER METHODS ===
    def get_user(self, user_id: int) -> Dict[str, Any]:
        """Get user data (with caching)"""
        key = str(user_id)
        
        if self.use_supabase:
            try:
                result = self.supabase.table('users').select("*").eq('id', user_id).execute()
                if result.data:
                    user = result.data[0]
                    if isinstance(user.get('chats'), str):
                        user['chats'] = json.loads(user['chats'])
                    return user
            except Exception as e:
                logger.warning(f"Supabase user fetch failed: {e}")
        
        # Fallback to local
        if key not in self.data:
            self.data[key] = self._new_user(user_id)
        return self.data[key]
    
    def _new_user(self, user_id: int) -> Dict[str, Any]:
        """Create new user"""
        return {
            "id": user_id,
            "name": "",
            "username": "",
            "chats": [],
            "relationship_level": 1,
            "stage": "initial",
            "last_interaction": datetime.now().isoformat(),
            "voice_messages_sent": 0,
            "total_messages": 0,
            "mood": "happy",
            "nickname": "",
            "preferences": {"voice_enabled": True}
        }
    
    def save_user(self, user_id: int, user_data: Dict[str, Any]):
        """Save user data"""
        key = str(user_id)
        user_data['last_interaction'] = datetime.now().isoformat()
        
        if self.use_supabase:
            try:
                save_data = user_data.copy()
                if isinstance(save_data.get('chats'), list):
                    save_data['chats'] = json.dumps(save_data['chats'])
                self.supabase.table('users').upsert(save_data).execute()
                return
            except Exception as e:
                logger.warning(f"Supabase save failed: {e}")
        
        # Fallback
        self.data[key] = user_data
        self._save_data()
    
    def add_message(self, user_id: int, user_msg: str, bot_msg: str, is_voice: bool = False):
        """Add message to history and update stats"""
        user = self.get_user(user_id)
        
        chat_entry = {
            "user": user_msg,
            "bot": bot_msg,
            "timestamp": datetime.now().isoformat(),
            "is_voice": is_voice,
            "mood": user.get('mood', 'happy')
        }
        
        user['chats'].append(chat_entry)
        if len(user['chats']) > 10:
            user['chats'] = user['chats'][-10:]
        
        user['total_messages'] += 1
        if is_voice:
            user['voice_messages_sent'] += 1
        
        # Level up logic
        user['relationship_level'] = min(10, user['relationship_level'] + 1)
        if user['relationship_level'] <= 3:
            user['stage'] = "initial"
        elif user['relationship_level'] <= 7:
            user['stage'] = "middle"
        else:
            user['stage'] = "advanced"
        
        self.save_user(user_id, user)
    
    def get_context(self, user_id: int) -> str:
        """Get conversation context for AI"""
        user = self.get_user(user_id)
        nickname = user.get('nickname', '') or user.get('name', 'baby')
        
        context_parts = [
            f"User's name: {user.get('name', 'Unknown')}",
            f"Nickname for user: {nickname}",
            f"Relationship stage: {user.get('stage', 'initial')}",
            f"Relationship level: {user.get('relationship_level', 1)}/10",
            f"Current mood: {user.get('mood', 'happy')}",
            f"Voice preference: {user.get('preferences', {}).get('voice_enabled', True)}"
        ]
        
        chats = user.get('chats', [])
        if chats:
            context_parts.append("\nRecent conversation (last 3):")
            for chat in chats[-3:]:
                context_parts.append(f"User: {chat.get('user', '')}")
                context_parts.append(f"You: {chat.get('bot', '')}")
        
        return "\n".join(context_parts)
    
    # === GROUP METHODS ===
    def add_group(self, group_id: int, title: str = "", username: str = ""):
        """Add or update group"""
        if group_id not in self.groups:
            self.groups[group_id] = {
                "id": group_id,
                "title": title,
                "username": username,
                "joined_at": datetime.now().isoformat(),
                "last_activity": datetime.now().isoformat(),
                "messages_count": 0,
                "is_active": True,
                "last_mention": None,
                "user_mentions": defaultdict(int)
            }
        else:
            self.groups[group_id]["last_activity"] = datetime.now().isoformat()
            self.groups[group_id]["messages_count"] += 1
            if title: self.groups[group_id]["title"] = title
            if username: self.groups[group_id]["username"] = username
        
        self._save_data()
    
    def remove_group(self, group_id: int):
        """Mark group as inactive"""
        if group_id in self.groups:
            self.groups[group_id]["is_active"] = False
            self._save_data()
    
    def get_active_groups(self) -> List[int]:
        """Get active group IDs"""
        return [gid for gid, data in self.groups.items() if data.get("is_active", True)]
    
    def get_groups_info(self) -> List[Dict]:
        """Get all group info"""
        return list(self.groups.values())
    
    def record_mention(self, group_id: int, user_id: int):
        """Record user mention in group"""
        if group_id in self.groups:
            self.groups[group_id]["last_mention"] = datetime.now().isoformat()
            self.groups[group_id]["user_mentions"][str(user_id)] += 1
            self._save_data()
    
    # === STATS ===
    def get_stats(self) -> Dict[str, Any]:
        """Get global statistics"""
        active_groups = self.get_active_groups()
        total_voice = sum(u.get('voice_messages_sent', 0) for u in self.data.values())
        total_messages = sum(u.get('total_messages', 0) for u in self.data.values())
        
        return {
            "total_users": len(self.data),
            "active_groups": len(active_groups),
            "total_messages": total_messages,
            "total_voice_messages": total_voice,
            "storage": "supabase" if self.use_supabase else "local",
            "supabase_active": self.use_supabase,
            "elevenlabs_active": voice_engine.working
        }

# Initialize database
db = Database()

# ==================== AI ENGINE (EMOTIONAL + CONTEXT-AWARE) ====================

class AIGenerator:
    """Gemini AI wrapper with emotional intelligence and fallback"""
    
    def __init__(self):
        self.model = None
        self._init_model()
        self.fallback_cache = {}
    
    def _init_model(self):
        """Initialize Gemini model"""
        if not Config.GEMINI_API_KEY:
            logger.warning("⚠️ Gemini API key not set")
            return
        
        try:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            self.model = genai.GenerativeModel(
                model_name=Config.GEMINI_MODEL,
                generation_config={
                    "temperature": 0.9,
                    "max_output_tokens": 500,
                    "top_p": 0.95,
                    "top_k": 40
                },
                safety_settings=[
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ]
            )
            logger.info("✅ Gemini AI initialized")
        except Exception as e:
            logger.error(f"Gemini init failed: {e}")
            self.model = None
    
    async def generate(self, message: str, context: str, stage: str, mood: str, for_voice: bool = False) -> str:
        """Generate AI response with full context"""
        if not self.model:
            return self._fallback_response(message, stage, mood)
        
        try:
            voice_hint = "This message will be sent as a voice note, so keep it emotional and conversational." if for_voice else ""
            
            prompt = f"""
{PERSONALITY.speaking_style}

Current mood: {mood}
Relationship stage: {stage}
{voice_hint}

{context}

User says: {message}

Respond as Niyati (Gen-Z style, short, natural, 1-3 lines max, use emoji, hinglish, slang):
"""
            
            response = await asyncio.to_thread(self.model.generate_content, prompt)
    
    # Handle safety and other finish reasons
    if not response:
        logger.warning("Gemini returned no response")
        return self._fallback_response(message, stage, mood)
    
    if response.candidates:
        candidate = response.candidates[0]
        finish_reason = candidate.finish_reason
        
        if finish_reason == 2:  # SAFETY
            logger.warning(f"Gemini safety blocked response for user input: {message[:50]}...")
            return self._safety_fallback(message, stage, mood)
        elif finish_reason in [3, 4]:  # RECITATION or UNSPECIFIED
            logger.warning(f"Gemini finish_reason {finish_reason} for: {message[:50]}...")
            return self._safety_fallback(message, stage, mood)
        elif finish_reason == 1:  # STOP (normal)
            if candidate.content and candidate.content.parts:
                text = candidate.content.parts[0].text.strip()
                if text:
                    # AI denial filter
                    if any(phrase in text.lower() for phrase in ["i am ai", "i'm a bot", "language model", "artificial intelligence"]):
                        return random.choice(AI_DENIAL)
                    return text
            else:
                logger.warning("Gemini returned empty parts despite STOP")
                return self._fallback_response(message, stage, mood)
        else:
            logger.warning(f"Unknown finish_reason: {finish_reason}")
            return self._fallback_response(message, stage, mood)
    else:
        logger.warning("Gemini returned no candidates")
        return self._fallback_response(message, stage, mood)

except Exception as e:
    logger.error(f"AI generation error: {e}")
    return self._fallback_response(message, stage, mood)
            
        except Exception as e:
            logger.error(f"AI generation error: {e}")
        
        return self._fallback_response(message, stage, mood)
    
    def _fallback_response(self, message: str, stage: str, mood: str) -> str:
        """Smart fallback with mood and context"""
        msg_lower = message.lower()
        
        # Greetings
        if any(word in msg_lower for word in ["hi", "hello", "hey", "hola", "sup"]):
            return random.choice(GENZ_GREETINGS)
        
        # Flirt detection
        flirt_keywords = ["cute", "beautiful", "pretty", "love", "girlfriend", "crush", "hot"]
        if any(kw in msg_lower for kw in flirt_keywords):
            return random.choice(GENZ_FLIRT.get(stage, GENZ_FLIRT["initial"]))
        
        # Jealousy
        if any(word in msg_lower for word in ["she", "her", "girl", "ladki", "gf"]):
            return random.choice(JEALOUSY)
        
        # Mood-based
        if mood in MOOD_RESPONSES:
            return random.choice(MOOD_RESPONSES[mood])
        
        # Questions
        if "?" in message:
            return random.choice([
                "umm lemme think... 🤔", "good question ngl 💭",
                "bruh idk... google kar lo? 😅", "why u asking such hard questions yaar 😩"
            ])
        
        # Default
        return random.choice([
            "hmm interesting... tell me more 👀", "achha achha... phir? 😊",
            "okay and? continue na 🤷‍♀️", "fr? that's crazy bro 💀"
        ])

# Initialize AI
ai = AIGenerator()

# ==================== UTILITIES ====================

def get_ist_time() -> datetime:
    """Get current IST time"""
    return datetime.now(pytz.utc).astimezone(Config.TIMEZONE)

def is_sleeping_time() -> bool:
    """Check if sleeping time"""
    now = get_ist_time().time()
    return Config.SLEEP_START <= now <= Config.SLEEP_END

def calculate_typing_delay(text: str) -> float:
    """Calculate typing delay"""
    base = min(3.0, max(0.5, len(text) / 50))
    return base + random.uniform(0.3, 1.0)

def is_mentioned(update: Update, bot_username: str) -> bool:
    """Check if bot is mentioned"""
    if not update.message or not update.message.text:
        return False
    
    text = update.message.text.lower()
    if bot_username.lower() in text or "niyati" in text:
        return True
    
    if update.message.reply_to_message and update.message.reply_to_message.from_user.id == update.effective_user.id:
        return True
    
    return False

def should_reply_in_group(update: Update, group_data: Dict) -> bool:
    """Smart group reply logic"""
    now = datetime.now()
    last_mention = group_data.get("last_mention")
    
    if last_mention:
        last_time = datetime.fromisoformat(last_mention)
        if (now - last_time).seconds < 60:
            return False  # Too recent
    
    # 30% chance if mentioned, 5% otherwise
    return random.random() < (0.3 if is_mentioned(update, update.get_bot().username) else 0.05)

# Rate limiting
last_group_reply: Dict[int, datetime] = defaultdict(lambda: datetime.min)
last_user_interaction: Dict[str, datetime] = defaultdict(lambda: datetime.min)

# ==================== BOT HANDLERS ====================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    user_id = user.id
    
    db.save_user(user_id, {
        **db._new_user(user_id),
        "name": user.first_name,
        "username": user.username or ""
    })
    
    welcome = f"""<b>heyyy {user.first_name}! 👋✨</b>

I'm <b>Niyati</b> - 17 y/o college girl from delhi 💅

just text me like a normal person yaar! i love making new friends 🥰
sometimes i send voice notes too when im in the mood 🎤

btw im totally real okay, not some bot or whatever 🙄💖

<i>lessgo bestie! 🚀</i>"""
    
    await update.message.reply_text(welcome, parse_mode='HTML')
    logger.info(f"User {user_id} started bot")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message"""
    user_id = update.effective_user.id
    is_owner = user_id == Config.OWNER_USER_ID
    
    help_text = """<b>✨ Niyati Bot Commands</b>

<b>Everyone:</b>
/start - shuru karo conversation
/help - ye message
/ping - check response time
/mood - dekho ya change karo mood
/tts <text> - convert any text to speech
/voice <text> - make me speak in voice note
/stats - statistics (owner only)

<b>Just chat normally!</b>
main tumse normal ladki ki tarah baat karungi 💖
kabhi voice notes bhi bhejungi 🎤"""
    
    if is_owner:
        help_text += """

<b>Owner Commands:</b>
/voicestatus - check voice engine status
/scan - discover all groups
/groups - list all groups
/broadcast - message all groups
/stats - detailed statistics"""
    
    await update.message.reply_text(help_text, parse_mode='HTML')

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check bot response time"""
    start = datetime.now()
    msg = await update.message.reply_text("🏓 Pong!")
    end = datetime.now()
    ms = (end - start).microseconds / 1000
    await msg.edit_text(f"🏓 Pong! `{ms:.2f}ms`", parse_mode='Markdown')

async def mood_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set or check mood"""
    user_id = update.effective_user.id
    user_data = db.get_user(user_id)
    
    if not context.args:
        current_mood = user_data.get('mood', 'happy')
        emoji = PERSONALITY.moods.get(current_mood, ["😊"])[0]
        await update.message.reply_text(
            f"my current mood: {current_mood} {emoji}\n\n"
            "change it with: /mood [happy/sad/angry/flirty/excited]"
        )
        return
    
    mood = context.args[0].lower()
    valid_moods = list(PERSONALITY.moods.keys())
    
    if mood not in valid_moods:
        await update.message.reply_text(f"bruh... valid moods: {', '.join(valid_moods)}")
        return
    
    user_data['mood'] = mood
    db.save_user(user_id, user_data)
    
    emoji = PERSONALITY.moods[mood][0]
    await update.message.reply_text(
        f"mood changed to {mood} {emoji}\n"
        f"ab main {mood} mood me hu!"
    )

async def tts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Convert text to speech using gTTS"""
    user_id = update.effective_user.id
    
    if not context.args:
        if update.message.reply_to_message and update.message.reply_to_message.text:
            text = update.message.reply_to_message.text
        else:
            await update.message.reply_text(
                "💬 <b>Text to Speech</b>\n\n"
                "Usage:\n"
                "/tts <text> - Convert text to speech\n"
                "OR reply to any message with /tts\n\n"
                "Example: /tts Hello, this is a test",
                parse_mode='HTML'
            )
            return
    else:
        text = ' '.join(context.args)
    
    if len(text) > Config.MAX_MESSAGE_LENGTH:
        await update.message.reply_text("❌ Text too long! Maximum 500 characters.")
        return
    
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.RECORD_VOICE)
        
        tts = gTTS(text=text, lang='hi', slow=False)
        audio_io = BytesIO()
        tts.write_to_fp(audio_io)
        audio_io.seek(0)
        
        await update.message.reply_voice(
            voice=audio_io,
            caption=f"🎤 TTS: {text[:50]}..." if len(text) > 50 else f"🎤 TTS: {text}"
        )
        
        logger.info(f"TTS generated for user {user_id}")
        
    except Exception as e:
        logger.error(f"TTS error: {e}")
        await update.message.reply_text("❌ oops! TTS generation failed... try again later 😅")

async def voice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Make Niyati speak in her voice"""
    user_id = update.effective_user.id
    
    if not context.args:
        status = "✅ Working!" if voice_engine.working else "❌ Not configured"
        await update.message.reply_text(
            f"🎤 <b>Voice Command</b>\n\n"
            f"ElevenLabs Status: {status}\n\n"
            "Usage: /voice <text>\n"
            "Example: /voice hey bestie kya haal hai\n\n"
            "Note: High-quality AI voice when ElevenLabs is configured!",
            parse_mode='HTML'
        )
        return
    
    text = ' '.join(context.args)
    
    if len(text) > 300:
        await update.message.reply_text("arey itna lamba text? thoda short karo na 😅")
        return
    
    # Add personality
    endings = [" na", " yaar", " 💕", " hehe", " 😊", "... okay?", "... samjhe?", " hai na?"]
    enhanced = text + random.choice(endings)
    
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.RECORD_VOICE)
        
        audio_io = await voice_engine.text_to_speech(enhanced, emotion="happy")
        
        if audio_io:
            await update.message.reply_voice(
                voice=audio_io,
                caption="🎤 Niyati's message ✨",
                duration=len(enhanced) // 10
            )
            logger.info(f"Voice sent to user {user_id}")
        else:
            await update.message.reply_text("uff... voice generation me problem ho gyi 😅")
        
    except Exception as e:
        logger.error(f"Voice command error: {e}")
        await update.message.reply_text("sorry yaar, voice note nahi ban paya 😔")

async def voice_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check voice engine status (owner only)"""
    user_id = update.effective_user.id
    if user_id != Config.OWNER_USER_ID:
        await update.message.reply_text("⛔ Owner only command!")
        return
    
    eleven_status = "❌ Not configured"
    if Config.ELEVENLABS_API_KEY:
        try:
            import requests
            headers = {"xi-api-key": Config.ELEVENLABS_API_KEY}
            response = requests.get("https://api.elevenlabs.io/v1/user", headers=headers, timeout=10)
            if response.status_code == 200:
                user_info = response.json()
                chars_used = user_info.get('subscription', {}).get('character_count', 0)
                chars_limit = user_info.get('subscription', {}).get('character_limit', 0)
                eleven_status = f"""✅ Active
├ Characters Used: {chars_used:,}/{chars_limit:,}
├ Voice Model: eleven_multilingual_v2
└ Voice ID: {Config.ELEVENLABS_VOICE_ID}"""
            else:
                eleven_status = f"❌ API Error: {response.status_code}"
        except Exception as e:
            eleven_status = f"❌ Error: {str(e)[:50]}"
    
    status_msg = f"""<b>🎤 Voice Engine Status</b>

<b>ElevenLabs AI Voice:</b>
{eleven_status}

<b>Fallback (gTTS):</b>
✅ Always available

<b>Current:</b>
├ Primary: {'ElevenLabs' if voice_engine.working else 'gTTS'}
├ Cache Size: {len(voice_engine.cache)} items
└ Auto-fallback: Enabled

<i>Tip: ElevenLabs provides natural, emotional AI voices!</i>"""
    
    await update.message.reply_text(status_msg, parse_mode='HTML')

async def scan_groups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scan all groups (owner only)"""
    user_id = update.effective_user.id
    if user_id != Config.OWNER_USER_ID:
        await update.message.reply_text("⛔ sirf owner hi ye command use kar sakte hai!")
        return
    
    await update.message.reply_text("🔍 Scanning all chats... please wait")
    
    discovered = 0
    errors = 0
    processed = set()
    
    try:
        updates = await context.bot.get_updates(limit=100)
        for update_obj in updates:
            chat = None
            if update_obj.message: chat = update_obj.message.chat
            elif update_obj.edited_message: chat = update_obj.edited_message.chat
            
            if chat and chat.type in ["group", "supergroup"] and chat.id not in processed:
                processed.add(chat.id)
                try:
                    chat_info = await context.bot.get_chat(chat.id)
                    db.add_group(chat.id, chat_info.title or "", chat_info.username or "")
                    discovered += 1
                    logger.info(f"Discovered group: {chat_info.title}")
                except (Forbidden, BadRequest):
                    db.remove_group(chat.id)
                    errors += 1
                except Exception as e:
                    logger.error(f"Error checking chat {chat.id}: {e}")
                    errors += 1
        
        active = len(db.get_active_groups())
        report = f"""<b>📊 Group Scan Complete</b>

🔍 Discovered: {discovered} new groups
❌ Errors: {errors}
✅ Total Active: {active}

Use /groups to see all."""
        
        await update.message.reply_text(report, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Scan error: {e}")
        await update.message.reply_text(f"❌ Scan failed: {str(e)}")

async def groups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all groups (owner only)"""
    user_id = update.effective_user.id
    if user_id != Config.OWNER_USER_ID:
        await update.message.reply_text("⛔ ye command sirf owner ke liye hai!")
        return
    
    groups = db.get_groups_info()
    active = [g for g in groups if g.get('is_active', True)]
    
    if not active:
        await update.message.reply_text("📭 No active groups found. Run /scan first!")
        return
    
    active.sort(key=lambda x: x.get('last_activity', ''), reverse=True)
    
    msg = ["<b>📋 Active Groups</b>\n"]
    for i, group in enumerate(active[:20], 1):
        title = group.get('title', 'Unknown')
        username = group.get('username', '')
        count = group.get('messages_count', 0)
        line = f"{i}. {title}"
        if username: line += f" (@{username})"
        line += f" [{count} msgs]"
        msg.append(line)
    
    if len(active) > 20:
        msg.append(f"\n... and {len(active)-20} more")
    
    msg.append(f"\n<b>Total: {len(active)}</b>")
    await update.message.reply_text("\n".join(msg), parse_mode='HTML')

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast to all groups (owner only)"""
    user_id = update.effective_user.id
    if user_id != Config.OWNER_USER_ID:
        await update.message.reply_text("⛔ only owner can broadcast!")
        return
    
    active_groups = db.get_active_groups()
    if not active_groups:
        await update.message.reply_text("📭 No groups! Run /scan first.")
        return
    
    success = 0
    failed = 0
    removed = []
    
    if update.message.reply_to_message:
        source = update.message.reply_to_message
        await update.message.reply_text(f"📡 Broadcasting to {len(active_groups)} groups...")
        
        for gid in active_groups:
            try:
                if source.text:
                    await context.bot.send_message(gid, source.text, parse_mode='HTML')
                elif source.photo:
                    await context.bot.send_photo(gid, source.photo[-1].file_id, caption=source.caption)
                elif source.voice:
                    await context.bot.send_voice(gid, source.voice.file_id, caption=source.caption)
                success += 1
                await asyncio.sleep(Config.BROADCAST_DELAY)
            except (Forbidden, BadRequest):
                failed += 1
                removed.append(gid)
                db.remove_group(gid)
            except Exception as e:
                failed += 1
                logger.error(f"Broadcast error: {e}")
    else:
        text = ' '.join(context.args) if context.args else None
        if not text:
            await update.message.reply_text(
                "❓ Usage:\n"
                "/broadcast <message>\n"
                "OR reply to any message with /broadcast"
            )
            return
        
        await update.message.reply_text(f"📡 Broadcasting to {len(active_groups)} groups...")
        for gid in active_groups:
            try:
                await context.bot.send_message(gid, text, parse_mode='HTML')
                success += 1
                await asyncio.sleep(Config.BROADCAST_DELAY)
            except (Forbidden, BadRequest):
                failed += 1
                removed.append(gid)
                db.remove_group(gid)
            except Exception as e:
                failed += 1
                logger.error(f"Broadcast error: {e}")
    
    report = f"""<b>📊 Broadcast Complete</b>

✅ Success: {success}/{len(active_groups)}
❌ Failed: {failed}"""
    if removed:
        report += f"\n🗑️ Removed {len(removed)} inactive groups"
    
    await update.message.reply_text(report, parse_mode='HTML')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show statistics (owner only)"""
    user_id = update.effective_user.id
    if Config.OWNER_USER_ID and user_id != Config.OWNER_USER_ID:
        await update.message.reply_text("⛔ stats sirf owner dekh sakte hai!")
        return
    
    stats = db.get_stats()
    user_data = db.get_user(user_id)
    
    stats_msg = f"""<b>📊 Bot Statistics</b>

<b>Global:</b>
👥 Users: {stats['total_users']}
👥 Groups: {stats['active_groups']}
💬 Messages: {stats.get('total_messages', 'N/A')}
🎤 Voice: {stats.get('total_voice_messages', 0)}
💾 Storage: {stats['storage'].upper()}
🔊 ElevenLabs: {'✅' if stats['elevenlabs_active'] else '❌'}

<b>Your Profile:</b>
💬 Chats: {len(user_data.get('chats', []))}
❤️ Level: {user_data.get('relationship_level', 1)}/10
🎭 Stage: {user_data.get('stage', 'initial')}
🎤 Voice: {user_data.get('voice_messages_sent', 0)}
😊 Mood: {user_data.get('mood', 'happy')}

<b>System:</b>
🤖 AI: Gemini 2.0 Flash
⏰ Time: {get_ist_time().strftime('%H:%M IST')}
🛌 Sleeping: {'Yes' if is_sleeping_time() else 'No'}"""
    
    await update.message.reply_text(stats_msg, parse_mode='HTML')

# ==================== MESSAGE HANDLER (CORE) ====================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all text messages with full logic"""
    try:
        if not update.message or not update.message.text:
            return
        
        chat = update.effective_chat
        user = update.effective_user
        user_id = user.id
        chat_id = chat.id
        user_msg = update.message.text.strip()
        now = datetime.now()
        
        is_private = chat.type == "private"
        
        # Update user info
        if is_private:
            db.save_user(user_id, {
                **db._new_user(user_id),
                "name": user.first_name,
                "username": user.username or ""
            })
        else:
            db.add_group(chat_id, chat.title or "", chat.username or "")
            db.record_mention(chat_id, user_id)
        
        # Rate limiting
        key = f"{chat_id}_{user_id}"
        if (now - last_user_interaction[key]).seconds < Config.USER_INTERACTION_COOLDOWN:
            return
        last_user_interaction[key] = now
        
        if not is_private:
            if (now - last_group_reply[chat_id]).seconds < Config.GROUP_REPLY_COOLDOWN:
                return
            
            group_data = db.groups.get(chat_id, {})
            if not is_mentioned(update, context.bot.username) and not should_reply_in_group(update, group_data):
                return
            last_group_reply[chat_id] = now
        
        # Sleeping time
        if is_sleeping_time():
            responses = ["yaar abhi so rahi hu... kal baat karte hai 😴", "bruh its late... good night! 💤"]
            await update.message.reply_text(random.choice(responses))
            return
        
        # AI detection
        ai_keywords = ["bot", "ai", "fake", "robot", "chatgpt", "gemini", "language model"]
        if any(kw in user_msg.lower() for kw in ai_keywords):
            response = random.choice(AI_DENIAL)
            await asyncio.sleep(calculate_typing_delay(response))
            await update.message.reply_text(response)
            db.add_message(user_id, user_msg, response)
            return
        
        # Typing action
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        
        # Get context
        user_data = db.get_user(user_id)
        stage = user_data.get('stage', 'initial')
        mood = user_data.get('mood', 'happy')
        context_str = db.get_context(user_id)
        
        # Decide voice
        should_voice = (
            is_private and 
            user_data.get('preferences', {}).get('voice_enabled', True) and
            voice_engine.should_send_voice(user_msg, stage, mood)
        )
        
        # Generate response
        response = await asyncio.wait_for(
            ai.generate(user_msg, context_str, stage, mood, for_voice=should_voice),
            timeout=Config.AI_TIMEOUT
        )
        
        if not response:
            response = random.choice([
                "hmm... lemme think about that 🤔", "okay... interesting 👀", "achha... phir? 😊"
            ])
        
        # Send message
        if should_voice:
            await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.RECORD_VOICE)
            audio_io = await voice_engine.text_to_speech(response, emotion=mood)
            
            if audio_io:
                await update.message.reply_voice(
                    voice=audio_io,
                    duration=len(response) // 10,
                    caption="🎤 Niyati's voice note ✨" if len(response) > 50 else None
                )
                db.add_message(user_id, user_msg, response, is_voice=True)
            else:
                await asyncio.sleep(calculate_typing_delay(response))
                await update.message.reply_text(response)
                db.add_message(user_id, user_msg, response)
        else:
            await asyncio.sleep(calculate_typing_delay(response))
            await update.message.reply_text(response)
            db.add_message(user_id, user_msg, response)
        
        logger.info(f"Replied to {user_id} in {'DM' if is_private else f'group {chat_id}'}")
        
    except asyncio.TimeoutError:
        logger.warning(f"AI timeout for user {user_id}")
        try:
            await update.message.reply_text("uff... thoda late ho gya, sorry yaar 😅")
        except:
            pass
    except Exception as e:
        logger.error(f"Message handler error: {e}", exc_info=True)
        try:
            await update.message.reply_text("oop something went wrong... try again? 😅")
        except:
            pass

# ==================== FLASK APP ====================

flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    stats = db.get_stats()
    return jsonify({
        "bot": "Niyati",
        "version": "6.0",
        "status": "online ✨",
        "users": stats['total_users'],
        "groups": stats['active_groups'],
        "storage": stats['storage'],
        "elevenlabs": stats['elevenlabs_active'],
        "time": get_ist_time().strftime('%Y-%m-%d %H:%M:%S IST')
    })

@flask_app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "mood": "happy",
        "sleeping": is_sleeping_time(),
        "elevenlabs": voice_engine.working,
        "users": len(db.data)
    })

def run_flask():
    logger.info(f"Starting Flask on {Config.HOST}:{Config.PORT}")
    serve(flask_app, host=Config.HOST, port=Config.PORT, threads=4)

# ==================== MAIN ====================

async def main():
    """Main entry point"""
    try:
        Config.validate()
        
        logger.info("="*60)
        logger.info("🤖 Starting Niyati Bot v6.0")
        logger.info("✨ Realistic Gen-Z Girlfriend with HD Voice & Memory")
        logger.info("="*60)
        
        app = Application.builder().token(Config.TELEGRAM_BOT_TOKEN).build()
        
        # Register handlers
        handlers = [
            CommandHandler("start", start_command),
            CommandHandler("help", help_command),
            CommandHandler("ping", ping_command),
            CommandHandler("mood", mood_command),
            CommandHandler("tts", tts_command),
            CommandHandler("voice", voice_command),
            CommandHandler("voicestatus", voice_status_command),
            CommandHandler("scan", scan_groups_command),
            CommandHandler("groups", groups_command),
            CommandHandler("broadcast", broadcast_command),
            CommandHandler("stats", stats_command),
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message),
        ]
        
        for handler in handlers:
            app.add_handler(handler)
        
        await app.initialize()
        await app.start()
        
        bot_info = await app.bot.get_me()
        logger.info(f"✅ Bot started: @{bot_info.username}")
        logger.info("💬 Ready to vibe with users!")
        
        # Initial group scan
        logger.info("🔍 Running initial group scan...")
        try:
            updates = await app.bot.get_updates(limit=100)
            for update_obj in updates:
                chat = None
                if update_obj.message: chat = update_obj.message.chat
                elif update_obj.edited_message: chat = update_obj.edited_message.chat
                if chat and chat.type in ["group", "supergroup"]:
                    try:
                        chat_info = await app.bot.get_chat(chat.id)
                        db.add_group(chat.id, chat_info.title or "", chat_info.username or "")
                    except:
                        pass
            logger.info(f"✅ Initial scan: {len(db.get_active_groups())} groups")
        except Exception as e:
            logger.warning(f"Initial scan failed: {e}")
        
        await app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
        
        logger.info("🚀 Bot is now live and listening...")
        await asyncio.Event().wait()
        
    except Exception as e:
        logger.critical(f"💥 Critical error: {e}")
        raise

if __name__ == "__main__":
    # Start Flask in background
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    import time
    time.sleep(2)
    
    # Run bot
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 byeee! Niyati misses u already 💔")
    except Exception as e:
        logger.critical(f"💥 Fatal error: {e}")
        sys.exit(1)
