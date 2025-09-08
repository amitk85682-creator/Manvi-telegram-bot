from database import get_cursor
from thefuzz import process

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
        
        # Try ILIKE search
        cur.execute(
            "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
            (f'%{query}%', limit)
        )
        results = cur.fetchall()
        
        if len(results) >= 3:
            return results
            
        # Fallback to fuzzy matching
        cur.execute("SELECT title, url FROM movies")
        all_movies = cur.fetchall()
        
        # Use thefuzz for fuzzy matching
        movie_titles = [movie[0] for movie in all_movies]
        matches = process.extract(query, movie_titles, limit=limit, scorer=process.partial_ratio)
        
        # Filter matches with score >= 80
        filtered_matches = [match for match, score in matches if score >= 80]
        
        # Get full movie data for matches
        if filtered_matches:
            placeholders = ','.join(['%s'] * len(filtered_matches))
            cur.execute(
                f"SELECT title, url FROM movies WHERE title IN ({placeholders}) LIMIT %s",
                filtered_matches + [limit]
            )
            return cur.fetchall()
            
        return results
