from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

# 4.1 Logika punktacji
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
    hour = tx.get('hour')
    if hour is None:
        try:
            # Próba wyciągnięcia godziny z timestampu, jeśli nie ma pola 'hour'
            hour = datetime.fromisoformat(tx['timestamp']).hour
        except:
            hour = 12 
            
    if hour < 6:
        score += 2
        rules.append('R3')
        
    return score, rules

# Test prowadzącego
test_tx = {'tx_id': 'TX999', 'amount': 4500.0, 'category': 'elektronika', 'timestamp': '2026-04-01T03:15:00'}
print(f"Wynik testu prowadzącego: {score_transaction(test_tx)}\n") 


# 4.2 KONSUMENT - Zmiana tematu na 'enriched_transactions'
consumer = KafkaConsumer(
    'enriched_transactions',  # <-- TUTAJ BYŁA ZMIANA (czytamy z etapu po dodaniu ryzyka)
    bootstrap_servers='localhost:9092', # Zmienione na localhost, żeby pasowało do reszty Twoich plików
    auto_offset_reset='earliest', 
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Rozpoczęto sprawdzanie oszustw. Czekam na transakcje z poziomu ENRICH...")

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    # Jeśli suma >= 3 -> PODEJRZANA
    if score >= 3:
        # Dodajemy informację o punktach do transakcji przed wysłaniem alertu
        tx['fraud_score'] = score
        tx['violated_rules'] = rules
        
        print(f"!!! ALERT !!! Transakcja: {tx['tx_id']} | Punkty: {score} | Ryzyko: {tx.get('risk_level')} | Reguły: {rules}")
        
        # Wyślij do tematu 'alerts'
        alert_producer.send('alerts', value=tx)
