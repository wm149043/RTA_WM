from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', 
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

print("Rozpoczęto zliczanie transakcji per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    # Zapisywanie 
    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    # Co 10 wiadomości podsumowanie
    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (Przerobiono wiadomości: {msg_count}) ---")
        for s in store_counts:
            print(f"Sklep: {s:<10} | Liczba transakcji: {store_counts[s]:<5} | Suma: {total_amount[s]:.2f} PLN")