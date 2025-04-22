import pg8000.native
import os


def create_conn():
    return pg8000.native.Connection(
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE"),
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
    )


def close_db_connection(conn):
    conn.close()
