import psycopg2
from psycopg2 import pool
import os

# Database connection pool
db_pool = None

def init_db():
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, os.environ.get('DATABASE_URL'),
            sslmode='require'  # SSL connection for Render
        )
        
        # Test connection
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT version();")
        conn.commit()
        cur.close()
        db_pool.putconn(conn)
        
        logger.info("Database connected successfully")
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
