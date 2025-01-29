from kafka import KafkaConsumer, KafkaProducer
from kafka_config import kafka_config
import json

INPUT_TOPIC = "building_sensors_hellcat_topic"
OUTPUT_TOPICS = {
    "temperature": "temperature_alerts_hellcat_topic",
    "humidity": "humidity_alerts_hellcat_topic"
}

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='sensor_processor_group'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

print("📡 Запущено обробку даних з датчиків...")

for message in consumer:
    data = message.value
    print(f"📥 Отримано повідомлення: {data}")  # Дебаг

    if not isinstance(data, dict):
        print(f"❌ Некоректний формат повідомлення: {data}")
        continue

    if "temperature" not in data or "humidity" not in data:
        print(f"⚠️ Відсутні очікувані ключі в повідомленні: {data}")
        continue

    alerts = []

    if data["temperature"] > 40:
        alerts.append((OUTPUT_TOPICS["temperature"], {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "value": data["temperature"],
            "message": "🚨 Критична температура!"
        }))

    if data["humidity"] > 80 or data["humidity"] < 20:
        alerts.append((OUTPUT_TOPICS["humidity"], {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "value": data["humidity"],
            "message": "🚨 Аномальна вологість!"
        }))

    for topic, alert in alerts:
        producer.send(topic, key=data["sensor_id"], value=alert)
        print(f"⚠️ Сповіщення: {alert}")

    consumer.commit()

