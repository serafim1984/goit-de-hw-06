from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, MY_NAME

# Create Kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

topic_name_1 = f'{MY_NAME}_building_sensors'
topic_name_2 = f'{MY_NAME}_alert_Kafka_topic'

num_partitions = 1
replication_factor = 1

new_topic_1 = NewTopic(name=topic_name_1, num_partitions=num_partitions, replication_factor=replication_factor)
new_topic_2 = NewTopic(name=topic_name_2, num_partitions=num_partitions, replication_factor=replication_factor)
try:
    admin_client.create_topics(new_topics=[new_topic_1, new_topic_2], validate_only=False)
    #admin_client.create_topics(new_topics=[new_topic_1], validate_only=False)
    print(f"Topics '{topic_name_1}', '{topic_name_2}' created successfully.")
    #print(f"Topics '{topic_name_1}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

for topic in admin_client.list_topics():
    if "MY_NAME" in topic:
        print(topic)

# Close client
admin_client.close()
