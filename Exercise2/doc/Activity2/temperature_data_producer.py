import os
import subprocess
import sys
import random
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql

DB_NAME = "office_db"
DB_USER = "postgres"
DB_PASSWORD = "postgrespw"
DB_HOST = "localhost"
DB_PORT = 5432

# Step 1: Connect to default database
conn = psycopg2.connect(
    dbname="postgres",  # default DB
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
conn.autocommit = True  # required to create database
cursor = conn.cursor()

# Step 2: Create target database if it doesn't exist
cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [DB_NAME])
exists = cursor.fetchone()
if not exists:
    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
    print(f"Database {DB_NAME} created.")
else:
    print(f"Database {DB_NAME} already exists.")

cursor.close()
conn.close()

# Step 3: Connect to the newly created database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# Step 4: Create table if it doesn't exist
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

# Step 5: Produce sample data
import random, time
from datetime import datetime

sensor_id = "sensor_1"

try:
    while True:
        temp = round(random.uniform(18.0, 30), 2)
        cursor.execute(
            "INSERT INTO temperature_readings (sensor_id, temperature, recorded_at) VALUES (%s, %s, %s)",
            (sensor_id, temp, datetime.now())
        )
        conn.commit()
        print(f"{datetime.now()} - Inserted temperature: {temp} °C")
        time.sleep(60)
except KeyboardInterrupt:
    print("Stopped producing data.")
finally:
    cursor.close()
    conn.close()