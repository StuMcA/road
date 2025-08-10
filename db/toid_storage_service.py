import psycopg2
from psycopg2.extras import execute_values
from db.db_config import get_connection

def init_toid_table():
    create_query = """
    CREATE EXTENSION IF NOT EXISTS postgis;

    CREATE TABLE IF NOT EXISTS toid_points (
        toid TEXT PRIMARY KEY,
        version_date DATE,
        source_product TEXT,
        geom geometry(Point, 4326),
        longtitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION
    );
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(create_query)
        conn.commit()
    conn.close()

def save_toids_to_db(features):
    connection = get_connection()
    cursor = connection.cursor()

    insert_query = """
        INSERT INTO toid_points (toid, version_date, source_product, geom, longtitude, latitude)
        VALUES %s
        ON CONFLICT (toid) DO NOTHING;
    """

    if features:
        execute_values(
            cursor,
            insert_query,
            features,
            template="(%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)"
        )
        connection.commit()

    cursor.close()
    connection.close()
