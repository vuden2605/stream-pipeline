import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

AWS_REGION     = os.getenv("AWS_REGION")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME    = os.getenv("BUCKET_NAME")

KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_RAW_IMAGES = os.getenv("TOPIC_RAW",    "raw_images")
TRAFFIC_EVENT_TOPIC  = os.getenv("TRAFFIC_EVENT", "traffic_events")
VEHICLE_COUNT_AGG_TOPIC = os.getenv("TOPIC_AGG", "vehicle_count_agg")
TOPIC_CANONICAL = os.getenv("TOPIC_CANONICAL", "traffic_events")

DB_URL        = os.getenv("DB_URL")
MODEL_PATH    = os.getenv("MODEL_PATH", "model.pt")
CONFIDENCE    = float(os.getenv("CONFIDENCE", 0.3))

INTERVAL_SECONDS = 10

# Load danh sách camera từ file cameras.json (chỉ id + url)
_CAMERAS_FILE = Path(__file__).parent / "cameras.json"
with open(_CAMERAS_FILE, encoding="utf-8") as _f:
    CAMERAS = json.load(_f)

# CLASS_NAMES được import và dùng trong consumer_ai.py
# Key = class ID từ YOLO model, Value = tên hiển thị / lưu DB
CLASS_NAMES = {
    0: "motorcycle",
    1: "car",
    2: "bus",
    3: "large_vehicle",
}   
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close"  # BẮT BUỘC ĐỔI THÀNH CLOSE
}