from database import get_cursor
from thefuzz import fuzz

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
        
        # Try fuzzy matching with pg_trgm
        try:
            cur.execute("""
                SELECT title, url, SIMILARITY(title, %s) as similarity
                FROM movies 
                WHERE title % %s
                ORDER BY similarity DESC
                LIMIT %s
            """, (query, query, limit))
            
            results = cur.fetchall()
            if results:
                return [(title, url) for title, url, similarity in results if similarity >= 0.3]
        except:
            pass
        
        # Fallback to ILIKE
        cur.execute(
            "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
            (f'%{query}%', limit)
        )
        return cur.fetchall()
