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
CONFIDENCE    = float(os.getenv("CONFIDENCE", 0.3))

# Model YOLO dùng bởi thư mục AI/ (thay cho detect trực tiếp trong consumer_ai.py cũ)
AI_MODEL_PATH = os.getenv("AI_MODEL_PATH", str(Path(__file__).parent / "AI" / "weights" / "best.pt"))

# speed_kmh = TWF (Traffic Weight Factor, 0-1, xem AI/src/core/metrics.py) x tốc độ free-flow giả định
TWF_MAX_SPEED_KMH = float(os.getenv("TWF_MAX_SPEED_KMH", "50"))

INTERVAL_SECONDS = 10

# Load danh sách camera + edge_id thật (đã map-match với Valhalla graph) từ
# cameras_with_zones_merged.json — thay cho cameras.json cũ (edge_id giả).
# CAMERA_LIMIT (tuỳ chọn qua env) giới hạn số camera bật; để trống = bật hết.
_CAMERAS_FILE = Path(__file__).parent / "cameras_with_zones_merged.json"
with open(_CAMERAS_FILE, encoding="utf-8") as _f:
    _camera_configs = json.load(_f)

_CAMERA_LIMIT = os.getenv("CAMERA_LIMIT")
_CAMERA_LIMIT = int(_CAMERA_LIMIT) if _CAMERA_LIMIT else None

CAMERAS = [
    {
        "id": cam["cam_id"],
        "url": (
            "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx"
            f"?id={cam['cam_id']}&bg=black&w=500&h=500"
        ),
        "edges": [
            {
                "edge_id":   str(zone["edge_id"]),
                "way_id":    zone.get("way_id"),
                "direction": zone.get("side"),
            }
            for zone in cam.get("zones", {}).values()
        ],
    }
    for cam in _camera_configs
    if cam.get("zones")
][:_CAMERA_LIMIT]

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close"  # BẮT BUỘC ĐỔI THÀNH CLOSE
}