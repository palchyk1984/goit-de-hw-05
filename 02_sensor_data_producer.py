from kafka import KafkaProducer
from kafka_config import kafka_config
import json
import uuid
import time
import random

SENSOR_ID = str(uuid.uuid4())
TOPIC_NAME = "building_sensors_hellcat_topic"

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

try:
    # Перевіряємо доступність Kafka
    producer.send(TOPIC_NAME, key=SENSOR_ID, value={"test": "connection check"})
    print(f"✅ Підключено до Kafka. Починаємо відправку даних...")
    
    while True:
        data = {
            "sensor_id": SENSOR_ID,
            "timestamp": int(time.time() * 1000),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)
        }

        producer.send(TOPIC_NAME, key=SENSOR_ID, value=data)
        print(f"📤 Відправлено: {data}")

        time.sleep(2)
except KeyboardInterrupt:
    print("🛑 Відправка даних зупинена.")
finally:
    producer.close()
