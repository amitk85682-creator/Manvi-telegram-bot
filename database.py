# File: database.py
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from typing import List, Tuple
import asyncio
from config import Config

db_pool = None

def init_db():
    """बॉट के शुरू होने पर डेटाबेस कनेक्शन पूल बनाता है।"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, dsn=Config.DATABASE_URL)
        with get_cursor() as cur:
            # ज़रूरी टेबल्स बनाएँ
            cur.execute("CREATE TABLE IF NOT EXISTS movies (id SERIAL PRIMARY KEY, title TEXT NOT NULL UNIQUE, url TEXT NOT NULL)")
            cur.execute("CREATE TABLE IF NOT EXISTS user_requests (id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, movie_title TEXT NOT NULL, UNIQUE(user_id, movie_title))")
            # pg_trgm एक्सटेंशन को चालू करने की कोशिश करें
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        print("Database pool and tables initialized.")
    except Exception as e:
        print(f"Database initialization failed: {e}")
        db_pool = None

@contextmanager
def get_cursor():
    """पूल से एक कनेक्शन सुरक्षित रूप से लेता और लौटाता है।"""
    if not db_pool:
        raise ConnectionError("Database pool is not initialized.")
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        db_pool.putconn(conn)

def _search_movies_blocking(query: str) -> List[Tuple[str, str]]:
    """डेटाबेस में मूवी खोजने का असली (धीमा) काम करता है।"""
    with get_cursor() as cur:
        # PostgreSQL की SIMILARITY फंक्शन का उपयोग करके स्मार्ट सर्च
        cur.execute("""
            SELECT title, url FROM movies 
            WHERE SIMILARITY(title, %s) > %s
            ORDER BY SIMILARITY(title, %s) DESC LIMIT 5
        """, (query, Config.FUZZY_MATCH_THRESHOLD / 100.0, query))
        results = cur.fetchall()
        
        # अगर स्मार्ट सर्च से कुछ नहीं मिलता, तो पुराने तरीके (ILIKE) से कोशिश करें
        if not results:
            cur.execute("SELECT title, url FROM movies WHERE title ILIKE %s LIMIT 5", (f'%{query}%',))
            results = cur.fetchall()
        return results

async def search_movies(query: str) -> List[Tuple[str, str]]:
    """बॉट को फ्रीज किए बिना मूवी खोजता है।"""
    return await asyncio.to_thread(_search_movies_blocking, query)

def _store_request_blocking(user_id: int, title: str):
    """यूज़र की रिक्वेस्ट को डेटाबेस में स्टोर करता है।"""
    with get_cursor() as cur:
        cur.execute("INSERT INTO user_requests (user_id, movie_title) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, title))

async def store_user_request(user_id: int, title: str):
    """बॉट को फ्रीज किए बिना यूज़र की रिक्वेस्ट स्टोर करता है।"""
    await asyncio.to_thread(_store_request_blocking, user_id, title)
