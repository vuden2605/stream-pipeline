import os
import sys
import json
import time
import base64
import logging
import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
from pathlib import Path

import numpy as np
import redis
import requests
import urllib3
import firebase_admin
from firebase_admin import credentials, messaging
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
ACCIDENT_CONFIDENCE = float(os.getenv("ACCIDENT_CONFIDENCE", "0.4"))
ACCIDENT_INTERVAL_SECONDS = int(os.getenv("ACCIDENT_INTERVAL_SECONDS", "10"))

# Số chu kỳ liên tiếp phải thấy incident trên cùng 1 camera trước khi gửi
# thông báo — chống báo giả vì 1 frame nhiễu/occlusion. Quan trọng hơn trước
# vì giờ gửi thẳng cho user thật, không còn admin duyệt lại như bản trước.
ACCIDENT_STREAK_THRESHOLD = int(os.getenv("ACCIDENT_STREAK_THRESHOLD", "3"))

# Sau khi đã gửi thông báo cho 1 camera, tạm ngưng gửi thêm cho chính camera
# đó trong khoảng này — tránh spam nhiều thông báo trùng cho cùng 1 vụ còn
# đang trong khung hình.
ACCIDENT_COOLDOWN_SECONDS = int(os.getenv("ACCIDENT_COOLDOWN_SECONDS", "300"))

# ── Redis keys riêng cho accident pipeline (chỉ dùng để chống báo giả/spam,
# không lưu sự kiện/ảnh nữa — không còn dashboard nào đọc lại) — không trùng
# key nào của worker.py/queue_feeder.py (camera_queue, traffic:speeds).
ACCIDENT_STREAK_KEY_PREFIX = "accident:streak:"
ACCIDENT_COOLDOWN_KEY_PREFIX = "accident:cooldown:"

COOKIE_REFRESH_INTERVAL_SECONDS = 3600

# ── Firebase Cloud Messaging — gửi theo TỪNG token thiết bị (không còn
# broadcast topic) — chỉ user có edge_id của camera này trong route hay đi
# mới nhận được. FIREBASE_SERVICE_ACCOUNT_B64 là service account key gốc
# (dạng JSON) đã base64-encode — không lưu file .json thật trong repo.
_FIREBASE_CREDS_B64 = os.getenv("FIREBASE_SERVICE_ACCOUNT_B64", "")

# ── API nội bộ bên BE (smart-transport) trả về danh sách fcm_token của user
# có edge_id nằm trong route hay đi — xem BE:
# controller/FavoriteRouteController.java#getFcmTokensByEdges.
BE_USERS_BY_EDGES_URL = os.getenv(
    "BE_USERS_BY_EDGES_URL",
    "http://167.172.80.252:8080/api/v1/favorite-routes/internal/users-by-edges",
)
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")

# Token của tài khoản admin — nhận MỌI cảnh báo thật trên toàn bản đồ, không
# lọc theo edge (khác với user thường, chỉ nhận khi route hay đi trùng đúng
# chỗ tai nạn). Nhiều token phân cách bởi dấu phẩy. Không cần đổi gì bên BE.
ADMIN_FCM_TOKENS = [t.strip() for t in os.getenv("ADMIN_FCM_TOKENS", "").split(",") if t.strip()]

_firebase_ready = False
if _FIREBASE_CREDS_B64:
    try:
        _service_account_info = json.loads(base64.b64decode(_FIREBASE_CREDS_B64))
        firebase_admin.initialize_app(credentials.Certificate(_service_account_info))
        _firebase_ready = True
        log.info("Firebase Admin SDK đã khởi tạo — project_id=%s", _service_account_info.get("project_id"))
    except Exception as e:
        log.error("Khởi tạo Firebase Admin SDK thất bại: %r", e)
else:
    log.warning("Chưa cấu hình FIREBASE_SERVICE_ACCOUNT_B64 — sẽ không gửi được FCM")

# ── cam_id -> tên địa điểm dễ đọc (VD "Võ Trần Chí - Kênh 10 (2)"), đọc
# thẳng từ cameras_with_zones_merged.json (giống config.py, nhưng đọc độc
# lập — không sửa config.py vì CAMERAS ở đó không có field "description").
_CAMERAS_FILE = Path(__file__).parent / "cameras_with_zones_merged.json"
with open(_CAMERAS_FILE, encoding="utf-8") as _f:
    _CAMERA_DESCRIPTIONS = {cam["cam_id"]: cam.get("description", "") for cam in json.load(_f)}

detector = AccidentDetector(ACCIDENT_MODEL_PATH)
log.info("Accident model loaded: %s", ACCIDENT_MODEL_PATH)


def get_fcm_tokens_for_edges(edge_ids: list[int]) -> list[str]:
    if not INTERNAL_API_KEY:
        log.warning("Chưa cấu hình INTERNAL_API_KEY — không gọi được BE để lấy user theo edge")
        return []

    try:
        resp = requests.post(
            BE_USERS_BY_EDGES_URL,
            json={"edgeIds": edge_ids},
            headers={"X-Internal-Api-Key": INTERNAL_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("data") or []
    except Exception as e:
        log.error("Gọi BE lấy user theo edge_ids=%s thất bại: %r", edge_ids, e)
        return []


def send_fcm_to_tokens(tokens: list[str], cam_id: str, class_name: str, conf: float, ts: int) -> None:
    if not _firebase_ready:
        log.info("[%s] Bỏ qua gửi FCM (Firebase chưa sẵn sàng)", cam_id)
        return
    if not tokens:
        log.info("[%s] Không có user nào đang đi qua đoạn đường này — không gửi FCM", cam_id)
        return

    location = _CAMERA_DESCRIPTIONS.get(cam_id, cam_id)
    # time.localtime() dùng giờ hệ thống — container Docker mặc định chạy UTC
    # (khác giờ Windows host), nên PHẢI ép rõ timezone VN ở đây, không được
    # dựa vào "giờ local" của máy đang chạy worker (đã gặp bug lệch 7h thật).
    vn_time = datetime.datetime.fromtimestamp(ts, tz=ZoneInfo("Asia/Ho_Chi_Minh"))
    time_str = vn_time.strftime("%H:%M %d/%m/%Y")

    messages = [
        messaging.Message(
            notification=messaging.Notification(
                title="Cảnh báo tai nạn giao thông",
                body=f"Nghi vấn tai nạn tại {location} lúc {time_str}",
            ),
            data={
                "type": "accident",
                "cam_id": cam_id,
                "class_name": class_name,
                "confidence": f"{conf:.3f}",
                "ts": str(ts),
            },
            token=token,
        )
        for token in tokens
    ]

    try:
        response = messaging.send_each(messages)
        log.warning(
            "[%s] Đã gửi FCM tới %d user (%d thành công, %d lỗi): %s",
            cam_id, len(tokens), response.success_count, response.failure_count, location,
        )
    except Exception as e:
        log.error("[%s] Gửi FCM thất bại: %r", cam_id, e)


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

    if streak < ACCIDENT_STREAK_THRESHOLD:
        return

    # Khoá cooldown theo camera — nếu đã gửi thông báo gần đây thì bỏ qua,
    # chỉ log để theo dõi chứ không gửi thêm thông báo trùng.
    cooldown_key = f"{ACCIDENT_COOLDOWN_KEY_PREFIX}{cam_id}"
    if not r.set(cooldown_key, "1", nx=True, ex=ACCIDENT_COOLDOWN_SECONDS):
        return

    tokens = get_fcm_tokens_for_edges(edge_ids)
    # Admin nhận mọi cảnh báo thật, không cần route trùng edge — gộp thêm
    # vào danh sách, khử trùng phòng khi admin cũng match qua edge thường.
    all_tokens = list(dict.fromkeys(tokens + ADMIN_FCM_TOKENS))
    send_fcm_to_tokens(all_tokens, cam_id, best["className"], best["conf"], int(time.time()))


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
