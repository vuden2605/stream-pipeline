import sys
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

# Thư mục AI/ có package riêng (src/core, src/data) import kiểu tương đối —
# cần thêm AI/ vào sys.path để dùng được từ worker.py chạy ở root repo.
sys.path.insert(0, str(Path(__file__).parent / "AI"))
from src.core.detector import TrafficDetector
from src.core import metrics
from src.data import config_loader

from config import (
    CAMERAS, CONFIDENCE, AI_MODEL_PATH, BROWSER_HEADERS,
    DEFAULT_EDGE_SPEED_KMH, FREE_FLOW_DEFAULT_KMH,
    GREENSHIELDS_N, MIN_SPEED_KMH, MEU_MAX_DEFAULT,
    PUBLISH_DEBOUNCE_SECONDS,
    REDIS_FIELD_TTL_SECONDS,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    REDIS_QUEUE_KEY, REDIS_SPEEDS_KEY, REDIS_CHANNEL,
)

REDIS_PUBLISH_LOCK_KEY = "traffic:publish_lock"

# Site camera dùng chứng chỉ không hợp lệ (giống hành vi ssl=False của
# ingestion.py cũ) — tắt cảnh báo lặp lại của urllib3 cho mỗi request.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

CAMERA_BY_ID = {cam["id"]: cam for cam in CAMERAS}

detector = TrafficDetector(AI_MODEL_PATH)
log.info("AI model loaded: %s", AI_MODEL_PATH)

_MEU_COEFFICIENTS = config_loader.getMeuCoefficients()
_CAMERA_THRESHOLDS = config_loader.loadJsonConfig("camera_thresholds.json")

COOKIE_REFRESH_INTERVAL_SECONDS = 3600


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
    """Bước 2: GET snapshot trực tiếp, giữ bytes trong RAM — không upload S3."""
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


def get_meu_max(cam_id: str) -> float:
    """
    Bước 5 cần MEU_max[cam][hour] — nhưng camera_thresholds.json hiện chỉ có
    hiệu chuẩn phẳng theo camera (không tách giờ) và chỉ phủ camera_001, nên
    610/611 camera còn lại dùng MEU_MAX_DEFAULT làm fallback.
    """
    return _CAMERA_THRESHOLDS.get(cam_id, {}).get("meuMax", MEU_MAX_DEFAULT)


def process_camera(session: requests.Session, r: redis.Redis, camera: dict) -> None:
    cam_id = camera["id"]
    edges = camera.get("edges", [])
    if not edges:
        return

    image_data = fetch_snapshot(session, camera)
    if image_data is None:
        return

    image = np.array(Image.open(BytesIO(image_data)).convert("RGB"))

    # Bước 3: YOLO inference
    result = detector.predict(image, conf=CONFIDENCE, save=False)

    # Bước 4: TWF (Traffic Weight Factor) — occupancy đo trực tiếp từ ảnh làm gatekeeper,
    # nên meuMax hiệu chuẩn thấp hơn sức chứa thật không còn gây báo kẹt giả (occupancy
    # thấp -> TWF = 0 dù meuMax nhỏ). TWF dùng thẳng làm density cho Greenshields.
    precise_occupancy = metrics.calculatePreciseOccupancyRatio(result, cam_id)
    camera_thresholds = {"meuMax": get_meu_max(cam_id)}
    density = metrics.calculateTrafficWeightFactor(
        precise_occupancy, result, cam_id, _MEU_COEFFICIENTS, camera_thresholds,
    )

    ts = int(time.time())

    for edge in edges:
        edge_id = edge["edge_id"]

        # Bước 5: Greenshields → raw_speed
        free_flow = DEFAULT_EDGE_SPEED_KMH.get(edge_id, FREE_FLOW_DEFAULT_KMH)
        raw_speed = free_flow * (1 - density ** GREENSHIELDS_N)
        raw_speed = max(raw_speed, MIN_SPEED_KMH)

        # Bước 8: Ghi Redis — kèm HEXPIRE để tự dọn field nếu camera chết hẳn
        # (không worker.py nào ghi lại nữa). TTL >= speed_ttl_seconds bên
        # valhalla-traffic-daemon nên không ảnh hưởng tính đúng đắn, chỉ dọn
        # bộ nhớ (xem config.py:REDIS_FIELD_TTL_SECONDS).
        pipe = r.pipeline()
        pipe.hset(REDIS_SPEEDS_KEY, edge_id, f"{raw_speed:.1f}:{ts}")
        pipe.hexpire(REDIS_SPEEDS_KEY, REDIS_FIELD_TTL_SECONDS, edge_id)
        pipe.execute()

        log.info(
            "[%s] edge=%s occupancy=%.1f%% TWF=%.2f speed=%.1fkm/h",
            cam_id, edge_id, precise_occupancy, density, raw_speed,
        )

    # Daemon Valhalla đang dùng (/app/traffic_updater.py) tự poll theo đồng hồ
    # cố định, không nghe Pub/Sub — PUBLISH này hiện không ai lắng nghe, giữ
    # lại (đã debounce qua SET NX EX) phòng khi sau này có consumer
    # event-driven khác cần (xem README Phần 2, mục 3.2).
    if r.set(REDIS_PUBLISH_LOCK_KEY, "1", nx=True, ex=PUBLISH_DEBOUNCE_SECONDS):
        r.publish(REDIS_CHANNEL, "update")


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    session = requests.Session()
    refresh_cookie(session)
    last_cookie_refresh = time.time()

    log.info("Worker started — %d camera trong config | chờ queue '%s'", len(CAMERAS), REDIS_QUEUE_KEY)

    while True:
        if time.time() - last_cookie_refresh > COOKIE_REFRESH_INTERVAL_SECONDS:
            refresh_cookie(session)
            last_cookie_refresh = time.time()

        # Bước 1: BLPOP cam_id từ Redis queue (chờ tối đa 5s rồi lặp lại để
        # vẫn kiểm tra refresh cookie định kỳ dù queue rỗng lâu)
        item = r.blpop(REDIS_QUEUE_KEY, timeout=5)
        if item is None:
            continue
        _, cam_id = item

        camera = CAMERA_BY_ID.get(cam_id)
        if camera is None:
            log.warning("cam_id=%s không có trong config.CAMERAS — bỏ qua", cam_id)
            continue

        try:
            process_camera(session, r, camera)
        except Exception as e:
            log.error("[%s] lỗi xử lý: %r", cam_id, e)


if __name__ == "__main__":
    main()
