import os
import sys
import time
import uuid
import logging
import smtplib
from email.mime.text import MIMEText
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
ACCIDENT_CONFIDENCE = float(os.getenv("ACCIDENT_CONFIDENCE", "0.4"))
ACCIDENT_INTERVAL_SECONDS = int(os.getenv("ACCIDENT_INTERVAL_SECONDS", "10"))

# Số chu kỳ liên tiếp phải thấy incident trên cùng 1 camera trước khi tạo sự
# kiện chờ duyệt — chống báo giả vì 1 frame nhiễu/occlusion.
ACCIDENT_STREAK_THRESHOLD = int(os.getenv("ACCIDENT_STREAK_THRESHOLD", "3"))

# Sau khi đã tạo 1 sự kiện cho 1 camera, tạm ngưng tạo thêm sự kiện mới cho
# chính camera đó trong khoảng này — tránh spam admin nhiều sự kiện trùng
# cho cùng 1 vụ tai nạn còn đang trong khung hình.
ACCIDENT_COOLDOWN_SECONDS = int(os.getenv("ACCIDENT_COOLDOWN_SECONDS", "300"))

ACCIDENT_IMAGE_TTL_SECONDS = int(os.getenv("ACCIDENT_IMAGE_TTL_SECONDS", str(48 * 3600)))
ACCIDENT_META_TTL_SECONDS = int(os.getenv("ACCIDENT_META_TTL_SECONDS", str(7 * 24 * 3600)))

# ── Redis keys riêng cho accident pipeline — không trùng với key nào của
# worker.py/queue_feeder.py (camera_queue, traffic:speeds, traffic:events).
REDIS_ACCIDENT_PENDING_KEY = os.getenv("REDIS_ACCIDENT_PENDING_KEY", "accident:pending")
ACCIDENT_STREAK_KEY_PREFIX = "accident:streak:"
ACCIDENT_IMAGE_KEY_PREFIX = "accident:image:"
ACCIDENT_META_KEY_PREFIX = "accident:meta:"
ACCIDENT_COOLDOWN_KEY_PREFIX = "accident:cooldown:"

COOKIE_REFRESH_INTERVAL_SECONDS = 3600

# ── Email báo admin khi có sự kiện mới — optional, tự bỏ qua nếu chưa cấu
# hình (không làm crash worker vì đây chỉ là thông báo, không phải core logic
# phát hiện). Đã có debounce tự nhiên qua streak+cooldown ở process_camera()
# nên mỗi sự kiện chỉ gửi đúng 1 email, không cần chống spam thêm ở đây.
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
ADMIN_EMAILS = [e.strip() for e in os.getenv("ADMIN_EMAILS", "").split(",") if e.strip()]
DASHBOARD_BASE_URL = os.getenv("DASHBOARD_BASE_URL", "http://localhost:8080")

detector = AccidentDetector(ACCIDENT_MODEL_PATH)
log.info("Accident model loaded: %s", ACCIDENT_MODEL_PATH)


def send_accident_email(event_id: str, cam_id: str, class_name: str, conf: float, ts: int) -> None:
    if not SMTP_HOST or not ADMIN_EMAILS:
        log.info("Bỏ qua gửi email (chưa cấu hình SMTP_HOST/ADMIN_EMAILS)")
        return

    link = f"{DASHBOARD_BASE_URL.rstrip('/')}/?event={event_id}"
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    body = (
        f"Phát hiện nghi vấn tai nạn giao thông.\n\n"
        f"Camera: {cam_id}\n"
        f"Loại: {class_name} (conf {conf:.2f})\n"
        f"Thời gian: {time_str}\n\n"
        f"Xem và duyệt tại: {link}\n"
    )
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = f"[Cảnh báo tai nạn] Camera {cam_id}"
    msg["From"] = SMTP_FROM
    msg["To"] = ", ".join(ADMIN_EMAILS)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.starttls()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, ADMIN_EMAILS, msg.as_string())
        log.info("[%s] Đã gửi email báo sự kiện %s tới %s", cam_id, event_id, ADMIN_EMAILS)
    except Exception as e:
        log.error("[%s] Gửi email thất bại: %r", cam_id, e)


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

    # Khoá cooldown theo camera — nếu đã tạo sự kiện gần đây thì bỏ qua, chỉ
    # log để theo dõi chứ không tạo thêm sự kiện trùng.
    cooldown_key = f"{ACCIDENT_COOLDOWN_KEY_PREFIX}{cam_id}"
    if not r.set(cooldown_key, "1", nx=True, ex=ACCIDENT_COOLDOWN_SECONDS):
        return

    event_id = uuid.uuid4().hex
    ts = int(time.time())

    pipe = r.pipeline()
    pipe.set(f"{ACCIDENT_IMAGE_KEY_PREFIX}{event_id}", image_data, ex=ACCIDENT_IMAGE_TTL_SECONDS)
    pipe.hset(f"{ACCIDENT_META_KEY_PREFIX}{event_id}", mapping={
        "cam_id": cam_id,
        "ts": ts,
        "class_name": best["className"],
        "confidence": f"{best['conf']:.3f}",
        "status": "PENDING",
    })
    pipe.expire(f"{ACCIDENT_META_KEY_PREFIX}{event_id}", ACCIDENT_META_TTL_SECONDS)
    pipe.zadd(REDIS_ACCIDENT_PENDING_KEY, {event_id: ts})
    pipe.execute()

    log.warning("[%s] TẠO SỰ KIỆN TAI NẠN event_id=%s class=%s — chờ admin duyệt",
                cam_id, event_id, best["className"])

    send_accident_email(event_id, cam_id, best["className"], best["conf"], ts)


def main():
    # decode_responses=False (khác worker.py/queue_feeder.py) vì cần ghi JPEG
    # bytes thô vào Redis (accident:image:<id>) — kết nối này chỉ dùng cho
    # accident pipeline nên không ảnh hưởng tới các client Redis khác.
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)
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
