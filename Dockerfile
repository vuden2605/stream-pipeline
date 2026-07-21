FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libgomp1 libgl1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    ultralytics \
    opencv-python-headless \
    numpy \
    pillow \
    requests \
    redis \
    python-dotenv \
    urllib3 \
    firebase-admin

COPY . .
