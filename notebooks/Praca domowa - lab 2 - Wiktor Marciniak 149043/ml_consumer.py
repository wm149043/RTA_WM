from kafka import KafkaConsumer, KafkaProducer
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

print("Konsument uruchomiony, czekam na transakcje...\n")

for msg in consumer:
    tx = msg.value
    # Zamieniamy kategorię na cyfrę dla modelu
    features = {
        "amount": tx.get('amount', 0),
        "is_electronics": 1 if tx.get('category') == 'elektronika' else 0,
        "tx_per_minute": 5 
    }

    try:
        r = requests.post(API_URL, json=features, timeout=2)
        result = r.json()
        
        if result.get('is_fraud'):
            alert_producer.send('alerts', value=tx)
            print(f" ALERT | id={tx.get('tx_id')} | prob={result.get('fraud_probability')}")
        else:
            print(f" OK    | id={tx.get('tx_id')} | prob={result.get('fraud_probability')}")
    except Exception as e:
        print(f"Błąd połączenia z API: {e}")
