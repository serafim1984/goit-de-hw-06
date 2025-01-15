from kafka import KafkaProducer
from configs import kafka_config, MY_NAME
import json
import uuid
import time
import random

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = f'Sensor_[{uuid.uuid4()}]'
sensor_topic_name = f'{MY_NAME}_building_sensors'
print(sensor_id)

for i in range(20):
    try:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(15, 50),
            "humidity": random.randint(10, 90)
        }
        producer.send(sensor_topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # waiting till all messages are sent
        print(f"Message {i} sent to topic '{sensor_topic_name}' successfully.")
        time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Close producer