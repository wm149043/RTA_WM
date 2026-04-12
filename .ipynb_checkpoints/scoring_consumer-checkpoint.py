from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

# 4.1
def score_transaction(tx):
    score = 0
    rules = []
    
    # R1: kwota > 3000 (+3 punkty)
    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1')
        
    # R2: elektronika i kwota > 1500 (+2 punkty)
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2')
        
    # R3: godzina < 6 (noc) (+2 punkty)
    # Wyciągamy godzinę z naszego pola 'hour' lub z 'timestamp' (jak w teście wykładowcy)
    hour = tx.get('hour')
    if hour is None:
        try:
            hour = datetime.fromisoformat(tx['timestamp']).hour
        except:
            hour = 12 # bezpieczny domyślny środek dnia
            
    if hour < 6:
        score += 2
        rules.append('R3')
        
    return score, rules

# Test wykładowcy (wyświetli się na samej górze przy uruchomieniu)
test_tx = {'tx_id': 'TX999', 'amount': 4500.0, 'category': 'elektronika', 'timestamp': '2026-04-01T03:15:00'}
print(f"Wynik testu prowadzącego: {score_transaction(test_tx)}\n") 


# 4.2
consumer = KafkaConsumer(
    'transactions', 
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', 
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Rozpoczęto sprawdzanie oszustw. Czekam na transakcje...")

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    # Jeśli suma >= 3 -> PODEJRZANA
    if score >= 3:
        print(f" ALERT! Transakcja: {tx['tx_id']} | Punkty: {score} | Złamane reguły: {rules}")
        # Wyślij do nowego tematu 'alerts'
        alert_producer.send('alerts', value=tx)