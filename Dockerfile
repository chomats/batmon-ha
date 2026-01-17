ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base-python:3.13-alpine3.23
FROM ${BUILD_FROM}

COPY bmslib .
COPY main.py .
COPY run.sh .

RUN \
    pip install \
        --no-cache-dir \
        --prefer-binary \
        paho-mqtt==2.1.0 \
        crcmod~=1.7 \
        pyserial~=3.5 \
        numpy~=2.3.2 \
    && chmod a+x /run.sh

CMD [ "/run.sh" ]
