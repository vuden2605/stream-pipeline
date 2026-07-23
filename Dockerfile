FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
# Container mặc định chạy UTC, không phải giờ VN — ảnh hưởng timestamp log
# (logging.basicConfig dùng time.localtime()) của cả 3 service dùng chung
# image này. tzdata đã có sẵn trong base image nên chỉ cần set TZ.
ENV TZ=Asia/Ho_Chi_Minh

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
    aiohttp

COPY . .
