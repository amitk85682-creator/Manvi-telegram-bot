import asyncio
from typing import List, Tuple
from .database import get_db_cursor # Assuming get_db_cursor is in a database utility file
from .config import Config # Assuming a Config class for settings

# This is the slow, blocking function
def _search_movies_blocking(query: str) -> List[Tuple[str, str]]:
    """Performs the actual database search using similarity."""
    with get_db_cursor() as cur:
        try:
            # First, try similarity search with pg_trgm
            cur.execute("""
                SELECT title, url FROM movies 
                WHERE SIMILARITY(title, %s) > %s
                ORDER BY SIMILARITY(title, %s) DESC LIMIT %s
            """, (query, Config.FUZZY_MATCH_THRESHOLD / 100.0, query, Config.MAX_SEARCH_RESULTS))
            results = cur.fetchall()
            if results:
                return results
        except Exception:
            # Fallback to simple ILIKE search if the extension isn't enabled
            cur.execute(
                "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
                (f'%{query}%', Config.MAX_SEARCH_RESULTS)
            )
            return cur.fetchall()
    return []

# This is the async wrapper your bot will call
async def search_movies(query: str) -> List[Tuple[str, str]]:
    """Asynchronously searches for movies without freezing the bot."""
    return await asyncio.to_thread(_search_movies_blocking, query)
