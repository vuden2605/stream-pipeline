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
# [LEGACY] chỉ còn dùng bởi consumer_ai.py/jobs/ (giữ lại, không phải pipeline
# đang chạy — xem worker.py cho pipeline Redis-queue hiện hành).
TWF_MAX_SPEED_KMH = float(os.getenv("TWF_MAX_SPEED_KMH", "50"))

INTERVAL_SECONDS = 10

# ── Redis (worker.py / queue_feeder.py — pipeline hiện hành) ─────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

REDIS_QUEUE_KEY      = os.getenv("REDIS_QUEUE_KEY", "camera_queue")
REDIS_SPEEDS_KEY     = os.getenv("REDIS_SPEEDS_KEY", "traffic:speeds")

# Tốc độ free-flow mặc định khi edge không có trong default_traffic.json
# (dùng làm free_flow[edge] cho công thức Greenshields bên dưới).
FREE_FLOW_DEFAULT_KMH = float(os.getenv("FREE_FLOW_DEFAULT_KMH", "50"))

# raw_speed = free_flow[edge] x (1 - density^n) — Greenshields tổng quát hoá,
# n=1 = Greenshields gốc (quan hệ speed-density tuyến tính).
GREENSHIELDS_N = float(os.getenv("GREENSHIELDS_N", "1.0"))
MIN_SPEED_KMH  = float(os.getenv("MIN_SPEED_KMH", "3.0"))

# MEU tối đa để chuẩn hoá density = min(MEU/MEU_max, 1.0). Thiết kế gốc muốn
# MEU_max theo từng (camera, giờ trong ngày), nhưng camera_thresholds.json
# hiện chỉ có hiệu chuẩn phẳng theo camera (meuMax, không tách theo giờ) và
# chỉ phủ camera_001 — nên MEU_MAX_DEFAULT là fallback cho 610/611 camera còn
# lại chưa hiệu chuẩn (xem worker.py:get_meu_max).
MEU_MAX_DEFAULT = float(os.getenv("MEU_MAX_DEFAULT", "200.0"))

# TTL cho từng field trong traffic:speeds (HEXPIRE, Redis >= 7.4).
# Không ảnh hưởng tính đúng đắn — valhalla-traffic-daemon (bên
# valhalla-hcm-traffic, ngoài repo này) đã tự coi field hết hạn dựa vào
# timestamp nhúng trong value (speed_ttl_seconds=900) bất kể field còn tồn
# tại trong Redis hay không. TTL ở đây chỉ để dọn bộ nhớ cho camera chết hẳn
# (không còn worker.py nào ghi nữa). PHẢI >= speed_ttl_seconds bên daemon đó
# để không bao giờ xoá field sớm hơn ngưỡng daemon tự áp dụng — chọn dư
# 300s làm đệm.
REDIS_FIELD_TTL_SECONDS = int(os.getenv("REDIS_FIELD_TTL_SECONDS", "1200"))

# Load danh sách camera + edge_id thật (đã map-match với Valhalla graph) từ
# cameras_with_zones_merged.json — thay cho cameras.json cũ (edge_id giả).
# CAMERA_LIMIT (tuỳ chọn qua env) giới hạn số camera bật; để trống = bật hết.
_CAMERAS_FILE = Path(__file__).parent / "cameras_with_zones_merged.json"
with open(_CAMERAS_FILE, encoding="utf-8") as _f:
    _camera_configs = json.load(_f)

_CAMERA_LIMIT = os.getenv("CAMERA_LIMIT")
_CAMERA_LIMIT = int(_CAMERA_LIMIT) if _CAMERA_LIMIT else None


def _extract_edge_id(zone: dict) -> str | None:
    # Lấy GraphId đầy đủ của Valhalla từ zone["mapped_edge"]["edge_id"]["value"]
    # — đây là edge do Valhalla map-matching trả về, nguồn đáng tin cậy duy
    # nhất, luôn có mặt và luôn đồng nhất 1 schema ở mọi zone (khác với
    # zone["edge_id"]/zone["edge_id_full"] ở cấp trên, vốn không đồng nhất
    # giữa các zone). Dùng "value" (GraphId toàn cục = tile_id+level+id),
    # KHÔNG dùng "id" ngắn (chỉ là số thứ tự cục bộ trong 1 tile bản đồ, không
    # xác định duy nhất 1 con đường trên toàn thành phố) — "value" mới khớp
    # đúng định dạng default_traffic.json và Valhalla graph thật dùng.
    mapped = zone.get("mapped_edge") or {}
    eid = mapped.get("edge_id") or {}
    value = eid.get("value")
    return str(value) if value is not None else None


def _build_edges(cam: dict) -> list[dict]:
    edges = []
    for zone in cam.get("zones", {}).values():
        edge_id = _extract_edge_id(zone)
        if edge_id is None:
            continue
        edges.append({
            "edge_id":   edge_id,
            "way_id":    zone.get("way_id"),
            "direction": zone.get("side"),
        })
    return edges


CAMERAS = [
    {
        "id": cam["cam_id"],
        "url": (
            "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx"
            f"?id={cam['cam_id']}&bg=black&w=500&h=500"
        ),
        "edges": _build_edges(cam),
    }
    for cam in _camera_configs
    if cam.get("zones")
][:_CAMERA_LIMIT]

# Tốc độ mặc định (free-flow) từng edge, theo GraphId đầy đủ — dùng làm cơ sở
# nhân với TWF thay vì hardcode 50km/h. Nếu edge không có trong file này,
# consumer_ai.py tự fallback về TWF_MAX_SPEED_KMH.
_DEFAULT_TRAFFIC_FILE = Path(__file__).parent / "default_traffic.json"
with open(_DEFAULT_TRAFFIC_FILE, encoding="utf-8") as _f:
    _default_traffic = json.load(_f)

DEFAULT_EDGE_SPEED_KMH = {
    str(entry["edge_id"]): entry["speed_kph"]
    for entry in _default_traffic
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close"  # BẮT BUỘC ĐỔI THÀNH CLOSE
}