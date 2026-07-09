import sys
import os
import json
import logging
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).parent))
from calibration_config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    CALIBRATION_MEU_MAX_KEY, CALIBRATION_FRAME_COUNT_KEY,
    ROAD_THRESHOLD_RATIO, MIN_RELIABLE_FRAMES, CAMERA_THRESHOLDS_PATH,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core import metrics

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("calibration_finalize")


def _write_json_atomic(path: str, data: dict) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    frame_counts = {cam_id: int(count) for cam_id, count in r.hgetall(CALIBRATION_FRAME_COUNT_KEY).items()}
    meu_maxes = dict(r.zrange(CALIBRATION_MEU_MAX_KEY, 0, -1, withscores=True))

    if not frame_counts:
        log.warning("Không có dữ liệu calibration nào trong Redis — bỏ qua finalize (calibration_worker.py có chạy chưa?).")
        return

    log.info("Finalize %d camera có dữ liệu calibration...", len(frame_counts))

    # 1. Road Mask — threshold tương đối theo số frame thật thu được của từng
    # camera (không dùng ngưỡng cứng 100, vì 611 camera có thể có số frame
    # thành công rất khác nhau do lỗi mạng/cookie trong 4h chạy).
    road_mask_done = 0
    for cam_id, frame_count in frame_counts.items():
        if frame_count <= 0:
            continue
        threshold = max(1, round(frame_count * ROAD_THRESHOLD_RATIO))
        metrics.generateAndSaveRoadMask(cam_id, threshold=threshold)
        road_mask_done += 1
        log.info("[%s] road mask: frame_count=%d threshold=%d (%.0f%%)", cam_id, frame_count, threshold, ROAD_THRESHOLD_RATIO * 100)

    # 2. meuMax — merge vào camera_thresholds.json, chỉ ghi field "meuMax"
    # (đúng như calculateTrafficWeightFactor đọc: cameraThresholds.get("meuMax", 1.0)).
    existing = {}
    if os.path.exists(CAMERA_THRESHOLDS_PATH):
        with open(CAMERA_THRESHOLDS_PATH, encoding="utf-8") as f:
            content = f.read().strip()
            existing = json.loads(content) if content else {}

    updated = 0
    for cam_id, meu_max in meu_maxes.items():
        cam_entry = existing.get(cam_id, {})
        cam_entry["meuMax"] = round(float(meu_max), 2)
        existing[cam_id] = cam_entry
        updated += 1

    _write_json_atomic(CAMERA_THRESHOLDS_PATH, existing)

    # 3. Cảnh báo camera có quá ít frame (dữ liệu calibration không đáng tin)
    low_frame_cams = sorted(cid for cid, cnt in frame_counts.items() if cnt < MIN_RELIABLE_FRAMES)
    if low_frame_cams:
        preview = ", ".join(low_frame_cams[:20])
        suffix = "..." if len(low_frame_cams) > 20 else ""
        log.warning(
            "%d camera có frame_count < %d (dữ liệu calibration không đáng tin cậy): %s%s",
            len(low_frame_cams), MIN_RELIABLE_FRAMES, preview, suffix,
        )

    log.info(
        "Finalize xong — road mask: %d camera, meuMax cập nhật: %d camera. Đã ghi %s",
        road_mask_done, updated, CAMERA_THRESHOLDS_PATH,
    )

    # Dọn Redis state để lần chạy calibration kế tiếp sạch sẽ.
    r.delete(CALIBRATION_MEU_MAX_KEY, CALIBRATION_FRAME_COUNT_KEY)


if __name__ == "__main__":
    main()
