import random
import time
from datetime import datetime

import psycopg

conn_params = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "mydb",
    "user": "postgres",
    "password": "postgrespw",
}

def setup_db():
    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id INT,
                    amount NUMERIC(10,2),
                    card_type VARCHAR(20),
                    merchant_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

def generate_data(batch_size=1000):
    insert_sql = """
        INSERT INTO transactions
        (user_id, amount, card_type, merchant_id)
        VALUES (%s, %s, %s, %s)
    """

    with psycopg.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            while True:
                data = [
                    (
                        random.randint(1000, 9999),
                        round(random.uniform(5.0, 5000.0), 2),
                        random.choice(["VISA", "MASTERCARD", "AMEX"]),
                        random.randint(1, 500),
                    )
                    for _ in range(batch_size)
                ]

                cur.executemany(insert_sql, data)
                conn.commit()

                print(f"{datetime.now()} - Inserted {batch_size} transactions")
                time.sleep(0.5)

if __name__ == "__main__":
    setup_db()
    generate_data()
