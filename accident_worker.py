import os
import sys
import json
import time
import logging
from io import BytesIO
from pathlib import Path

import numpy as np
import redis
import requests
import urllib3
from PIL import Image

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Thư mục AI/ có package riêng (src/core) import kiểu tương đối — cần thêm
# AI/ vào sys.path để dùng được từ accident_worker.py chạy ở root repo.
sys.path.insert(0, str(Path(__file__).parent / "AI"))
from src.core.accident_detector import AccidentDetector, INCIDENT_CLASSES

# Worker này CHỦ ĐÍCH tách biệt hoàn toàn khỏi worker.py/queue_feeder.py
# (pipeline traffic hiện hành) — chỉ import CAMERAS/BROWSER_HEADERS/REDIS_*
# (đọc, không sửa config.py), không dùng chung camera_queue hay bất kỳ Redis
# key nào của traffic pipeline, để không ảnh hưởng tới stream cũ.
from config import CAMERAS, BROWSER_HEADERS, REDIS_HOST, REDIS_PORT, REDIS_DB

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("accident_worker")

ACCIDENT_MODEL_PATH = os.getenv(
    "ACCIDENT_MODEL_PATH",
    str(Path(__file__).parent / "AI" / "accident-detection" / "weights" / "best.pt"),
)
ACCIDENT_CONFIDENCE = float(os.getenv("ACCIDENT_CONFIDENCE", "0.8"))
ACCIDENT_INTERVAL_SECONDS = int(os.getenv("ACCIDENT_INTERVAL_SECONDS", "10"))

# Số chu kỳ liên tiếp phải thấy incident trên cùng 1 camera trước khi gửi
# thông báo — chống báo giả vì 1 frame nhiễu/occlusion. Quan trọng hơn trước
# vì giờ gửi thẳng cho user thật, không còn admin duyệt lại như bản trước.
ACCIDENT_STREAK_THRESHOLD = int(os.getenv("ACCIDENT_STREAK_THRESHOLD", "3"))

# Sau khi đã gửi thông báo cho 1 camera, tạm ngưng gửi thêm cho chính camera
# đó trong khoảng này — tránh spam nhiều thông báo trùng cho cùng 1 vụ còn
# đang trong khung hình.
ACCIDENT_COOLDOWN_SECONDS = int(os.getenv("ACCIDENT_COOLDOWN_SECONDS", "300"))

# ── Redis keys riêng cho accident pipeline — không trùng key nào của
# worker.py/queue_feeder.py (camera_queue, traffic:speeds).
ACCIDENT_STREAK_KEY_PREFIX = "accident:streak:"
ACCIDENT_COOLDOWN_KEY_PREFIX = "accident:cooldown:"

# List Redis mà BE (smart-transport) poll để lấy user theo edge + gửi FCM —
# worker này CHỈ detect + đẩy message, không tự gọi BE API hay Firebase nữa
# (xem AccidentAlertScheduler.java bên BE). Cùng 1 Redis instance với BE
# (REDIS_HOST ở .env.prod trỏ thẳng vào Redis đã deploy cùng BE).
ACCIDENT_ALERT_QUEUE_KEY = "accident:alerts"

COOKIE_REFRESH_INTERVAL_SECONDS = 3600

# Lưu lại ảnh mỗi khi có nghi vấn (kể cả chưa đủ streak) để test model khác
# offline — không liên quan tới luồng gửi FCM, chỉ để thu thập dữ liệu.
TEST_SNAPSHOT_DIR = Path(os.getenv("TEST_SNAPSHOT_DIR", str(Path(__file__).parent / "test")))
TEST_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

# ── cam_id -> tên địa điểm dễ đọc (VD "Võ Trần Chí - Kênh 10 (2)"), đọc
# thẳng từ cameras_with_zones_merged.json (giống config.py, nhưng đọc độc
# lập — không sửa config.py vì CAMERAS ở đó không có field "description").
_CAMERAS_FILE = Path(__file__).parent / "cameras_with_zones_merged.json"
with open(_CAMERAS_FILE, encoding="utf-8") as _f:
    _CAMERA_DESCRIPTIONS = {cam["cam_id"]: cam.get("description", "") for cam in json.load(_f)}

detector = AccidentDetector(ACCIDENT_MODEL_PATH)
log.info("Accident model loaded: %s", ACCIDENT_MODEL_PATH)


def push_accident_alert(r: redis.Redis, cam_id: str, edge_ids: list[int], class_name: str, conf: float, ts: int) -> None:
    payload = {
        "camId": cam_id,
        "edgeIds": edge_ids,
        "className": class_name,
        "confidence": round(conf, 3),
        "ts": ts,
        "location": _CAMERA_DESCRIPTIONS.get(cam_id, cam_id),
    }
    try:
        r.rpush(ACCIDENT_ALERT_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
        log.warning("[%s] Đã đẩy cảnh báo tai nạn vào Redis queue: %s", cam_id, payload)
    except Exception as e:
        log.error("[%s] Đẩy cảnh báo vào Redis thất bại: %r", cam_id, e)


def save_test_snapshot(image_data: bytes, cam_id: str, class_name: str, conf: float) -> None:
    ts = int(time.time())
    filename = f"{cam_id}_{ts}_{class_name}_{conf:.2f}.jpg"
    try:
        (TEST_SNAPSHOT_DIR / filename).write_bytes(image_data)
    except Exception as e:
        log.error("[%s] Lưu snapshot test thất bại: %r", cam_id, e)


def is_valid_jpeg(data: bytes) -> bool:
    return len(data) > 5_000 and data[:2] == b"\xff\xd8" and data[-2:] == b"\xff\xd9"


def refresh_cookie(session: requests.Session) -> None:
    try:
        resp = session.get(
            "https://giaothong.hochiminhcity.gov.vn/",
            headers=BROWSER_HEADERS, timeout=15, verify=False,
        )
        log.info("Session cookie refreshed (HTTP %d)", resp.status_code)
    except Exception as e:
        log.warning("Cookie refresh failed: %r", e)


def fetch_snapshot(session: requests.Session, camera: dict) -> bytes | None:
    url = f"{camera['url']}&t={int(time.time() * 1000)}"
    try:
        resp = session.get(url, headers=BROWSER_HEADERS, timeout=10, verify=False)
    except Exception as e:
        log.warning("[%s] fetch lỗi: %r", camera["id"], e)
        return None

    if resp.status_code != 200:
        log.warning("[%s] HTTP %d", camera["id"], resp.status_code)
        return None

    data = resp.content
    if not is_valid_jpeg(data):
        log.warning("[%s] Not JPEG", camera["id"])
        return None
    return data


def process_camera(session: requests.Session, r: redis.Redis, camera: dict) -> None:
    cam_id = camera["id"]

    # Không map được edge nào -> không biết gửi cho ai, bỏ qua ngay từ đầu
    # (đỡ tốn compute chạy YOLO vô ích) — giống cách worker.py (traffic) bỏ
    # qua camera không có edges.
    edge_ids = [int(e["edge_id"]) for e in camera.get("edges", []) if e.get("edge_id") is not None]
    if not edge_ids:
        return

    image_data = fetch_snapshot(session, camera)
    if image_data is None:
        return

    image = np.array(Image.open(BytesIO(image_data)).convert("RGB"))
    result = detector.predict(image, conf=ACCIDENT_CONFIDENCE, save=False)
    incidents = detector.extractIncidentBoxes(result, INCIDENT_CLASSES)

    streak_key = f"{ACCIDENT_STREAK_KEY_PREFIX}{cam_id}"

    if not incidents:
        r.delete(streak_key)
        return

    streak = r.incr(streak_key)
    r.expire(streak_key, ACCIDENT_INTERVAL_SECONDS * (ACCIDENT_STREAK_THRESHOLD + 2))

    best = max(incidents, key=lambda b: b["conf"])
    log.info(
        "[%s] nghi vấn: %s conf=%.2f streak=%d/%d",
        cam_id, best["className"], best["conf"], streak, ACCIDENT_STREAK_THRESHOLD,
    )
    save_test_snapshot(image_data, cam_id, best["className"], best["conf"])

    if streak < ACCIDENT_STREAK_THRESHOLD:
        return

    # Khoá cooldown theo camera — nếu đã gửi thông báo gần đây thì bỏ qua,
    # chỉ log để theo dõi chứ không gửi thêm thông báo trùng.
    cooldown_key = f"{ACCIDENT_COOLDOWN_KEY_PREFIX}{cam_id}"
    if not r.set(cooldown_key, "1", nx=True, ex=ACCIDENT_COOLDOWN_SECONDS):
        return

    push_accident_alert(r, cam_id, edge_ids, best["className"], best["conf"], int(time.time()))


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    session = requests.Session()
    refresh_cookie(session)
    last_cookie_refresh = time.time()

    log.info("Accident worker started — %d camera | chu kỳ %ds", len(CAMERAS), ACCIDENT_INTERVAL_SECONDS)

    while True:
        t0 = time.time()

        if time.time() - last_cookie_refresh > COOKIE_REFRESH_INTERVAL_SECONDS:
            refresh_cookie(session)
            last_cookie_refresh = time.time()

        for camera in CAMERAS:
            try:
                process_camera(session, r, camera)
            except Exception as e:
                log.error("[%s] lỗi xử lý: %r", camera["id"], e)

        elapsed = time.time() - t0
        time.sleep(max(0, ACCIDENT_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
