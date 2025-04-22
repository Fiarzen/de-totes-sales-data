import pg8000.native
import os


def connect_to_test_db():
    return pg8000.native.Connection(
        user=os.getenv("TEST_USER"),
        password=os.getenv("TEST_PASSWORD"),
        database=os.getenv("TEST_DATABASE"),
        host=os.getenv("TEST_HOST"),
        port=int(os.getenv("TEST_PORT"))
    )
