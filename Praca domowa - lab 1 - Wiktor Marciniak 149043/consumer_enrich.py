from kafka import KafkaConsumer, KafkaProducer
import json


consumer = KafkaConsumer(
    'filtered_transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Enrichment Consumer (Risk Levels) started...")

for message in consumer:
    transaction = message.value
    amount = transaction.get('amount', 0)
    
    
    if amount > 3000:
        transaction['risk_level'] = 'HIGH'
    elif amount > 1000:
        transaction['risk_level'] = 'MEDIUM'
    else:
        transaction['risk_level'] = 'LOW'
    
    producer.send('enriched_transactions', value=transaction)
    print(f"Processed: Amount {amount} -> Risk {transaction['risk_level']}")
