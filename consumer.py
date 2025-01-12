from kafka import KafkaConsumer
from configs import kafka_config
from kafka import KafkaProducer
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

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    #group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

# Назва топіку для споживання данних з датчика
my_name = "Serafymovych"
topic_name_1 = f'building_sensors_{my_name}'

# Назва топіку для запису Temperature alerts
my_name = "Serafymovych"
topic_name_2 = f'temperature_alerts_{my_name}'

# Назва топіку для запису Humidity alerts
my_name = "Serafymovych"
topic_name_3 = f'humidity_alerts_{my_name}'

# Підписка на тему
consumer.subscribe([topic_name_1])

print(f"Subscribed to topic '{topic_name_1}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")

        if message.value["temperature"] > 40:
            print(f'Temperature alert >40 {message.value["temperature"]}, ID: {message.value["ID"]}, Time: {message.value["timestamp"]}')
            try:
                data = {
                    "ID": message.value["ID"],  # ID датчика
                    "timestamp": message.value["timestamp"],  # Часова мітка
                    "temperature": message.value["temperature"],  # Значення що виходить за межі
                }
                producer.send(topic_name_2, key=str(uuid.uuid4()), value=data)
                producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
                print(f"Temperature alert sent to topic '{topic_name_2}' successfully.")
                time.sleep(2)

            except Exception as e:
                print(f"An error occurred while sending temperature alert: {e}")

        if message.value["humidity"] > 80 or message.value["humidity"] < 20:
            print(f'Humidity alert <20 or >80 {message.value["humidity"]}, ID: {message.value["ID"]}, Time: {message.value["timestamp"]}')
            try:
                data = {
                    "ID": message.value["ID"],  # ID датчика
                    "timestamp": message.value["timestamp"],  # Часова мітка
                    "humidity": message.value["humidity"],  # Значення що виходить за межі
                }
                producer.send(topic_name_2, key=str(uuid.uuid4()), value=data)
                producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
                print(f"Humidity alert sent to topic '{topic_name_3}' successfully.")
                time.sleep(2)

            except Exception as e:
                print(f"An error occurred while sending humidity alert: {e}")

except KeyboardInterrupt:
    print("Processing interrupted by user.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer
