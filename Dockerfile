ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base-python:3.13-alpine3.23
FROM ${BUILD_FROM}

COPY bmslib bmslib
COPY main.py .
COPY run.sh .
COPY mqtt_util.py .
COPY requirements.txt .
RUN \
    pip install -r requirements.txt \
    && chmod a+x /run.sh

CMD [ "/run.sh" ]
