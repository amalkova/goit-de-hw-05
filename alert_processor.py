
import json

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

SENSORS_TOPIC = "anna_malkova_building_sensors"
TEMPERATURE_TOPIC = "anna_malkova_temperature_alerts"
HUMIDITY_TOPIC = "anna_malkova_humidity_alerts"

consumer = KafkaConsumer(
    SENSORS_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    group_id="anna_malkova_alert_processor",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Alert processor started")
print(f"Listening to: {SENSORS_TOPIC}")

try:
    for message in consumer:
        data = message.value

        sensor_id = data["sensor_id"]
        timestamp = data["timestamp"]
        temperature = data["temperature"]
        humidity = data["humidity"]

        print(f"\nReceived: {data}", flush=True)

        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "alert_type": "temperature",
                "message": f"Temperature exceeded 40°C: {temperature}°C"
            }

            producer.send(
                TEMPERATURE_TOPIC, value=alert
            ).get(timeout=10)

            print(f"TEMPERATURE ALERT: {alert}", flush=True)
            print(f"Sent to: {TEMPERATURE_TOPIC}", flush=True)

        if humidity > 80 or humidity < 20:
            condition = (
                "above 80%" if humidity > 80 else "below 20%"
            )

            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "alert_type": "humidity",
                "message": f"Humidity {condition}: {humidity}%"
            }

            producer.send(
                HUMIDITY_TOPIC, value=alert
            ).get(timeout=10)

            print(f"HUMIDITY ALERT: {alert}", flush=True)
            print(f"Sent to: {HUMIDITY_TOPIC}", flush=True)

except KeyboardInterrupt:
    print("\nAlert processor stopped.")

finally:
    producer.flush()
    producer.close()
    consumer.close()