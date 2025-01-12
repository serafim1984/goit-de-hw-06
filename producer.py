from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "Serafymovych"
topic_name_1 = f'building_sensors_{my_name}'

for i in range(30):
    # Відправлення повідомлення в топік
    try:
        data = {
            "ID": 1,  # Часова мітка
            "timestamp": time.time(),  # Часова мітка
            "temperature": random.randint(25, 45),  # Випадкове значення
            "humidity": random.randint(15, 85)  # Випадкове значення
        }
        producer.send(topic_name_1, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name_1}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer

