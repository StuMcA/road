import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="road_db",
        user="road_user",
        password="my_password",
        port=5432
    )
