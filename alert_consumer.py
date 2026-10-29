
import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"

TEMPERATURE_TOPIC = "anna_malkova_temperature_alerts"
HUMIDITY_TOPIC = "anna_malkova_humidity_alerts"

consumer = KafkaConsumer(
    TEMPERATURE_TOPIC,
    HUMIDITY_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="anna_malkova_hw05_final_consumer",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

print("Alert consumer started")
print("Listening to temperature and humidity alerts")

try:
    for message in consumer:
        alert = message.value

        print("\n" + "=" * 55)
        print(f"Topic: {message.topic}")
        print(f"Sensor ID: {alert['sensor_id']}")
        print(f"Timestamp: {alert['timestamp']}")
        print(f"Temperature: {alert['temperature']}°C")
        print(f"Humidity: {alert['humidity']}%")
        print(f"Alert type: {alert['alert_type']}")
        print(f"Message: {alert['message']}")
        print("=" * 55, flush=True)

except KeyboardInterrupt:
    print("\nAlert consumer stopped.")

finally:
    consumer.close()