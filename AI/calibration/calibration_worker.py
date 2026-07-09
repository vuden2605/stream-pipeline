import sys
import time
import logging
from io import BytesIO
from pathlib import Path

import numpy as np
import redis
import requests
from PIL import Image

sys.path.insert(0, str(Path(__file__).parent))
from calibration_config import (
    CAMERAS, CONFIDENCE, AI_MODEL_PATH,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    CALIBRATION_QUEUE_KEY, CALIBRATION_MEU_MAX_KEY, CALIBRATION_FRAME_COUNT_KEY,
)
import camera_client

# AI/ có package riêng (src/core, src/data) import kiểu tương đối — cần thêm
# AI/ vào sys.path để dùng được từ AI/calibration/ (giống worker.py ở root).
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core.detector import TrafficDetector
from src.core import metrics
from src.data import config_loader

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("calibration_worker")

CAMERA_BY_ID = {cam["id"]: cam for cam in CAMERAS}

detector = TrafficDetector(AI_MODEL_PATH)
log.info("AI model loaded: %s", AI_MODEL_PATH)

_MEU_COEFFICIENTS = config_loader.getMeuCoefficients()

COOKIE_REFRESH_INTERVAL_SECONDS = 3600


def process_camera(session: requests.Session, r: redis.Redis, camera: dict) -> None:
    cam_id = camera["id"]

    image_data = camera_client.fetch_snapshot(session, camera)
    if image_data is None:
        return

    image = np.array(Image.open(BytesIO(image_data)).convert("RGB"))
    result = detector.predict(image, conf=CONFIDENCE, save=False)

    # Nhiệm vụ 1: tích lũy heatmap — ghi đè {cam_id}_heatmap.npy
    metrics.updateHeatmapByCameraId(result, cam_id)

    # Nhiệm vụ 2: meuMax — running max qua toàn bộ camera, atomic bằng ZADD GT
    # (không cần đọc-so sánh-ghi thủ công nên nhiều instance chạy song song
    # không bị race condition).
    meu = metrics.calculateMotorcycleEquivalentUnit(result, _MEU_COEFFICIENTS)
    r.zadd(CALIBRATION_MEU_MAX_KEY, {cam_id: meu}, gt=True)
    r.hincrby(CALIBRATION_FRAME_COUNT_KEY, cam_id, 1)

    log.info("[%s] MEU=%.2f frame đã tích lũy", cam_id, meu)


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    session = requests.Session()
    camera_client.refresh_cookie(session)
    last_cookie_refresh = time.time()

    log.info(
        "Calibration worker started — %d camera trong config | chờ queue '%s'",
        len(CAMERAS), CALIBRATION_QUEUE_KEY,
    )

    while True:
        if time.time() - last_cookie_refresh > COOKIE_REFRESH_INTERVAL_SECONDS:
            camera_client.refresh_cookie(session)
            last_cookie_refresh = time.time()

        item = r.blpop(CALIBRATION_QUEUE_KEY, timeout=5)
        if item is None:
            continue
        _, cam_id = item

        camera = CAMERA_BY_ID.get(cam_id)
        if camera is None:
            log.warning("cam_id=%s không có trong danh sách calibration — bỏ qua", cam_id)
            continue

        try:
            process_camera(session, r, camera)
        except Exception as e:
            log.error("[%s] lỗi xử lý: %r", cam_id, e)


if __name__ == "__main__":
    main()
