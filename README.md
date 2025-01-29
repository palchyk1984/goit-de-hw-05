# 🚀 Kafka Sensor Data Processing

This project implements a real-time sensor data processing system using Apache Kafka.  
A producer generates sensor data, which is processed and stored in dedicated topics.

## 📦 **Project Structure**
```bash
├── kafka_config.py          # Kafka configuration (server, username, password)
├── 02_kafka_create_topics.py   # Script to create Kafka topics
├── 03_sensor_data_producer.py  # Producer that generates sensor data
├── 03_sensor_alert_processor.py  # Processor that analyzes sensor data and generates alerts
├── 04_alerts_consumer.py       # Consumer that listens for temperature and humidity alerts
├── kafka_connection_test.py      # Script to check connection with the Server
├── delete_topics.py      # Reset Kafka Topics (if needed)
```


📌 Features:

✅ Sends simulated sensor data to Kafka
✅ Detects critical temperature and humidity values
✅ Generates alerts when threshold values are exceeded
✅ Listens for alerts via a consumer

## Check connection with the Server 

``` bash
python3 kafka_connection_test.py
```

## How to run

1) Create Kafka topics

``` bash
python3 01_kafka_create_topics.py
```

❌ Reset Kafka Topics (if needed)

``` bash
python3 delete_topics.py
```

2) 🔵 Start the producer (simulating sensor data)

``` bash
python3 02_sensor_data_producer.py
```

3) 🟢 Start the data processor (generates alerts)

``` bash
python3 03_sensor_data_producer.py
```

4) 🟠 Start the alerts consumer

``` bash
python3 04_alerts_consumer.py
```

