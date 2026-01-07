#This agent uses a sliding window (simulated) to perform velocity checks and score the transaction
import json
from collections import deque
import time
import base64
from kafka import KafkaConsumer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
KAFKA_TOPIC = 'fraud-detection.public.transactions'
CONSUMER_GROUP = 'fraud-detection-agent2'

# Simulated In-Memory State for Velocity Checks.
user_history = {} 

def decode_decimal(encoded_bytes):
    """Decode Debezium DECIMAL bytes to float"""
    if isinstance(encoded_bytes, str):
        try:
            # Try base64 decode first
            decoded = base64.b64decode(encoded_bytes)
            # Convert bytes to integer (big-endian)
            value = int.from_bytes(decoded, byteorder='big', signed=True)
            # Apply scale of 2 (DECIMAL(10,2))
            return float(value) / 100
        except:
            return None
    elif isinstance(encoded_bytes, (int, float)):
        return float(encoded_bytes)
    return None

def analyze_fraud(transaction):
    user_id = transaction['user_id']
    amount = decode_decimal(transaction['amount'])
    
    # 1. Velocity Check (Recent transaction count)
    now = time.time()
    if user_id not in user_history:
        user_history[user_id] = deque()
    
    # Keep only last 60 seconds of history
    user_history[user_id].append(now)
    while user_history[user_id] and user_history[user_id][0] < now - 60:
        user_history[user_id].popleft()

    velocity = len(user_history[user_id])
    
    # 2. Heuristic Fraud Scoring
    score = 0
    if velocity > 5: score += 40  # Too many transactions in a minute
    if amount > 4000: score += 50 # High value transaction
    
    # 3. Simulate ML Model Hand-off
    # model.predict([[velocity, amount]])
    
    return score

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Agent started. Listening for CDC events...")
for message in consumer:  #consumer has to be implemented before!
    # Debezium wraps data in an 'after' block
    payload = message.value.get('payload', {})
    data = payload.get('after')
    
    if data:
        fraud_score = analyze_fraud(data)
        if fraud_score > 70:
            print(f"⚠️ HIGH FRAUD ALERT: User {data['user_id']} | Score: {fraud_score} | Amt: {decode_decimal(data['amount'])}")
        else:
            print(f"✅ Transaction OK: {data['id']} (Score: {fraud_score})")