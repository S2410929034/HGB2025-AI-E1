import time
import random
from datetime import datetime

import psycopg
from psycopg import sql

DB_NAME = "office_db"
DB_USER = "postgres"
DB_PASSWORD = "postgrespw"
DB_HOST = "localhost"
DB_PORT = 5432

# Step 1: Connect to default database
with psycopg.connect(
    dbname="postgres",
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    autocommit=True
) as conn:

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_NAME,)
        )
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}")
                .format(sql.Identifier(DB_NAME))
            )
            print(f"Database {DB_NAME} created.")
        else:
            print(f"Database {DB_NAME} already exists.")

# Step 2: Connect to the target database
with psycopg.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
) as conn:

    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS temperature_readings (
            id SERIAL PRIMARY KEY,
            sensor_id VARCHAR(50),
            temperature FLOAT,
            recorded_at TIMESTAMP DEFAULT NOW()
        )
        """)
        conn.commit()
        print("Table ready.")

        sensor_id = "sensor_1"

        try:
            while True:
                temp = round(random.uniform(18.0, 30.0), 2)
                cursor.execute(
                    """
                    INSERT INTO temperature_readings
                    (sensor_id, temperature, recorded_at)
                    VALUES (%s, %s, %s)
                    """,
                    (sensor_id, temp, datetime.now())
                )
                conn.commit()
                print(f"{datetime.now()} - Inserted temperature: {temp} °C")
                time.sleep(60)

        except KeyboardInterrupt:
            print("Stopped producing data.")
