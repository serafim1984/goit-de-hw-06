from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нового топіку
my_name = "Serafymovych"
topic_name_1 = f'building_sensors_{my_name}'
topic_name_2 = f'temperature_alerts_{my_name}'
topic_name_3 = f'humidity_alerts_{my_name}'
num_partitions = 2
replication_factor = 1

new_topic_1 = NewTopic(name=topic_name_1, num_partitions=num_partitions, replication_factor=replication_factor)
new_topic_2 = NewTopic(name=topic_name_2, num_partitions=num_partitions, replication_factor=replication_factor)
new_topic_3 = NewTopic(name=topic_name_3, num_partitions=num_partitions, replication_factor=replication_factor)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[new_topic_1], validate_only=False)
    print(f"Topic '{topic_name_1}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

try:
    admin_client.create_topics(new_topics=[new_topic_2], validate_only=False)
    print(f"Topic '{topic_name_2}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

try:
    admin_client.create_topics(new_topics=[new_topic_3], validate_only=False)
    print(f"Topic '{topic_name_3}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
print(admin_client.list_topics())

# Виводимо свої топіки (ті, що містять `my_name`)
existing_topics = admin_client.list_topics()
my_topics = [topic for topic in existing_topics if my_name in topic]
print("My topics:")
for topic in my_topics:
    print(topic)

# Закриття зв'язку з клієнтом
admin_client.close()




