# Data Engineering HW05 — Apache Kafka

IoT sensor monitoring system using Python and Apache Kafka.

## Overview

The application simulates IoT sensors that continuously send
temperature and humidity measurements to Kafka.

A separate processor analyzes incoming measurements and
publishes alerts when predefined thresholds are exceeded.

## Components

- docker-compose.yml — Kafka broker configuration
- check_topics.py — Kafka topic verification
- sensor_producer.py — IoT sensor data generation
- alert_processor.py — data filtering and alert generation
- alert_consumer.py — reading and displaying alerts

## Kafka Topics

- anna_malkova_building_sensors
- anna_malkova_temperature_alerts
- anna_malkova_humidity_alerts

## Alert Conditions

- Temperature > 40°C
- Humidity > 80%
- Humidity < 20%

## Technologies

- Python 3.13
- Apache Kafka 4.1.2
- Docker Compose
- kafka-python 3.0.12

## Running

Start Kafka:

    docker compose up -d

Install dependencies:

    pip install -r requirements.txt

Verify topics:

    python check_topics.py

Start sensor producers in separate terminals:

    python sensor_producer.py

Start alert processing:

    python alert_processor.py

Start alert consumer:

    python alert_consumer.py

## Results

Execution screenshots for all five homework stages
are available in the screenshots directory.
