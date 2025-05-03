#!/usr/bin/with-contenv bashio

MQTT_HOST=$(bashio::services mqtt "host")
MQTT_USER=$(bashio::services mqtt "username")
MQTT_PASSWORD=$(bashio::services mqtt "password")

/app/venv/bin/python3 main.py pair-only

/app/venv/bin/python3 --version
id
ls -la /dev/ttyUSB0

MQTT_HOST=$MQTT_HOST MQTT_USER=$MQTT_USER MQTT_PASSWORD=$MQTT_PASSWORD \
  /app/venv/bin/python3 main.py

