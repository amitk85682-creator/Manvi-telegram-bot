import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from config import Config

db_pool = None

def init_db():
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, Config.DATABASE_URL)
        
        with get_cursor() as cur:
            # Create tables
            cur.execute('''
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL UNIQUE,
                    url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cur.execute('''
                CREATE TABLE IF NOT EXISTS user_requests (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    movie_title TEXT NOT NULL,
                    group_id BIGINT,
                    group_name TEXT,
                    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified BOOLEAN DEFAULT FALSE,
                    UNIQUE(user_id, movie_title)
                )
            ''')
            
    except Exception as e:
        print(f"Database initialization error: {e}")
        raise

@contextmanager
def get_cursor():
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

def log_user_request(user_id, username, first_name, movie_title, group_id=None, group_name=None):
    with get_cursor() as cur:
        cur.execute('''
            INSERT INTO user_requests 
            (user_id, username, first_name, movie_title, group_id, group_name) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, movie_title) DO NOTHING
        ''', (user_id, username, first_name, movie_title, group_id, group_name))
