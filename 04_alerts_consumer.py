from kafka import KafkaConsumer
from kafka_config import kafka_config
import json
import sys
import time

TOPICS = ["temperature_alerts_hellcat_topic", "humidity_alerts_hellcat_topic"]

def create_consumer(topics, group_id='alerts_consumer_group'):
    """
    Створює Kafka Consumer для заданих топіків.

    :param topics: Список топіків для підписки.
    :param group_id: Ідентифікатор групи споживачів.
    :return: Ініціалізований KafkaConsumer.
    """
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
            key_deserializer=lambda v: v.decode('utf-8') if v else None,
            auto_offset_reset='earliest',  # Читаємо всі повідомлення з початку
            enable_auto_commit=False,  # Керуємо комітами вручну
            group_id=group_id
        )
        return consumer
    except Exception as e:
        print(f"❌ Не вдалося створити Kafka Consumer: {e}")
        sys.exit(1)

def format_timestamp(ts):
    """Форматує часову мітку у читабельний вигляд."""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts / 1000))

def main():
    consumer = create_consumer(TOPICS)

    print(f"📡 Підписано на топіки: {', '.join(TOPICS)}. Очікування алертів...")

    try:
        for message in consumer:
            if message.value is None:
                print(f"⚠️ Отримано порожнє повідомлення в топіку `{message.topic}`. Пропускаємо.")
                continue

            try:
                alert = message.value
                alert_topic = message.topic
                sensor_id = alert.get("sensor_id", "Unknown")
                timestamp = alert.get("timestamp", 0)
                value = alert.get("value", "N/A")
                message_text = alert.get("message", "Немає повідомлення")

                print(f"\n⚠️  АЛЕРТ [{alert_topic.upper()}]")
                print(f"   📍 Датчик: {sensor_id[:8]}...")
                print(f"   🕒 Час: {format_timestamp(timestamp)}")
                print(f"   📊 Значення: {value}")
                print(f"   📝 Повідомлення: {message_text}")
                print("=" * 40)

                # Підтверджуємо отримання повідомлення
                consumer.commit()

            except json.JSONDecodeError:
                print(f"❌ Помилка декодування JSON у топіку `{message.topic}`: {message.value}")
                continue

    except KeyboardInterrupt:
        print("\n🛑 Моніторинг зупинено.")
    except Exception as e:
        print(f"❌ Помилка під час обробки алертів: {e}")
    finally:
        consumer.close()
        print("🔌 З'єднання з Kafka закрито.")

if __name__ == "__main__":
    main()
