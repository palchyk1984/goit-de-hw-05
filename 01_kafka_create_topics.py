from kafka.admin import KafkaAdminClient, NewTopic
from kafka_config import kafka_config
import sys

TOPICS = [
    "building_sensors_hellcat_topic",
    "temperature_alerts_hellcat_topic",
    "humidity_alerts_hellcat_topic"
]

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
except Exception as e:
    print(f"❌ Помилка підключення до Kafka: {e}")
    sys.exit(1)

existing_topics = set(admin_client.list_topics())
new_topics = [NewTopic(name=topic, num_partitions=2, replication_factor=1) for topic in TOPICS if topic not in existing_topics]

if new_topics:
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"✅ Створені топіки: {[topic.name for topic in new_topics]}")
    except Exception as e:
        print(f"❌ Помилка створення топіків: {e}")
else:
    print("⚠️ Всі топіки вже існують.")

admin_client.close()