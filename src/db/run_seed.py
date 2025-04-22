from seed import seed_db

try:
    seed_db()
except Exception as e:
    print(e)
    raise e
