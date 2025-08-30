import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Get database connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "road_db"),
        user=os.getenv("DB_USER", "road_user"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", "5432")),
    )
