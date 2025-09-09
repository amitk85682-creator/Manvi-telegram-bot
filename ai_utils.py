from thefuzz import process

def correct_spelling(query, movie_list):
    """Correct spelling using fuzzy matching"""
    matches = process.extract(query, movie_list, limit=3)
    if matches and matches[0][1] > 80:  # 80% similarity threshold
        return matches[0][0], matches[1:]  # Return best match and alternatives
    return query, []

# Search function में use करें
def search_movies(query, limit=5):
    with get_cursor() as cur:
        # Get all movie titles for spelling correction
        cur.execute("SELECT title FROM movies")
        all_titles = [row[0] for row in cur.fetchall()]
        
        # Correct spelling
        corrected_query, alternatives = correct_spelling(query, all_titles)
        
        # Search with corrected query
        cur.execute(
            "SELECT title, url FROM movies WHERE title ILIKE %s LIMIT %s",
            (f'%{corrected_query}%', limit)
        )
        results = cur.fetchall()
        
        return results, corrected_query, alternatives
