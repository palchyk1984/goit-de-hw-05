from kafka.admin import KafkaAdminClient
from kafka_config import kafka_config

TOPICS_TO_DELETE = [
    "building_sensors_hellcat_topic",
    "temperature_alerts_hellcat_topic",
    "humidity_alerts_hellcat_topic"
]

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"]
    )

    existing_topics = admin_client.list_topics()
    topics_to_delete = [topic for topic in TOPICS_TO_DELETE if topic in existing_topics]

    if topics_to_delete:
        admin_client.delete_topics(topics_to_delete)
        print(f"✅ Видалені топіки: {topics_to_delete}")
    else:
        print("⚠️ Вказані топіки не існують.")

except Exception as e:
    print(f"❌ Помилка: {e}")

finally:
    admin_client.close()
