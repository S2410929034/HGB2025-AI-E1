import os
import subprocess
import sys
import random
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import extras
# Connection config
conn_params = "host=localhost port=5432 dbname=mydb user=postgres password=postgrespw"

def setup_db():
    conn = psycopg2.connect(conn_params)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id INT,
            amount DECIMAL(10,2),
            card_type VARCHAR(20),
            merchant_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def generate_data(batch_size=1000):
    conn = psycopg2.connect(conn_params)
    cur = conn.cursor()
    
    while True:
        data = [
            (random.randint(1000, 9999), 
             round(random.uniform(5.0, 5000.0), 2), 
             random.choice(['VISA', 'MASTERCARD', 'AMEX']),
             random.randint(1, 500)) 
            for _ in range(batch_size)
        ]
        
        query = "INSERT INTO transactions (user_id, amount, card_type, merchant_id) VALUES %s"
        extras.execute_values(cur, query, data)
        conn.commit()
        print(f"Inserted {batch_size} transactions...")
        time.sleep(0.5) # Adjust sleep to hit your specific "thousands/sec" target

if __name__ == "__main__":
    setup_db()
    generate_data()