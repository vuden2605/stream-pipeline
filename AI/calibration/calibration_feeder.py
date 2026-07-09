import sys
import time
import logging
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).parent))
from calibration_config import (
    CAMERAS, INTERVAL_SECONDS, DURATION_SECONDS, DRAIN_TIMEOUT_SECONDS,
    BACKLOG_GUARD_RATIO,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    CALIBRATION_QUEUE_KEY, CALIBRATION_DONE_KEY,
)

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("calibration_feeder")


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    total = len(CAMERAS)
    start_time = time.time()
    end_time = start_time + DURATION_SECONDS

    r.delete(CALIBRATION_DONE_KEY)

    log.info(
        "Calibration feeder started — %d camera | chu kỳ %ds | chạy %.1fh | queue=%s",
        total, INTERVAL_SECONDS, DURATION_SECONDS / 3600, CALIBRATION_QUEUE_KEY,
    )

    while time.time() < end_time:
        t0 = time.time()
        qlen_before = r.llen(CALIBRATION_QUEUE_KEY)

        if qlen_before > total * BACKLOG_GUARD_RATIO:
            log.warning(
                "Queue còn tồn %d/%d camera từ chu kỳ trước — bỏ qua chu kỳ này để worker bắt kịp",
                qlen_before, total,
            )
        else:
            pipe = r.pipeline()
            for cam in CAMERAS:
                pipe.rpush(CALIBRATION_QUEUE_KEY, cam["id"])
            pipe.execute()
            remaining_h = max(0.0, (end_time - time.time()) / 3600)
            log.info(
                "Đã đẩy %d camera_id vào queue (tồn trước đó: %d) — còn %.2fh",
                total, qlen_before, remaining_h,
            )

        elapsed = time.time() - t0
        time.sleep(max(0, INTERVAL_SECONDS - elapsed))

    log.info(
        "Đã hết %.1fh — dừng đẩy thêm, đợi queue rỗng (timeout %ds)...",
        DURATION_SECONDS / 3600, DRAIN_TIMEOUT_SECONDS,
    )

    drain_deadline = time.time() + DRAIN_TIMEOUT_SECONDS
    while time.time() < drain_deadline:
        qlen = r.llen(CALIBRATION_QUEUE_KEY)
        if qlen == 0:
            break
        log.info("Còn %d cam_id trong queue — chờ calibration_worker.py xử lý nốt...", qlen)
        time.sleep(5)
    else:
        log.warning(
            "Hết timeout drain nhưng queue vẫn còn %d cam_id — vẫn tiến hành finalize",
            r.llen(CALIBRATION_QUEUE_KEY),
        )

    r.set(CALIBRATION_DONE_KEY, "1")
    log.info("Queue đã rỗng (hoặc hết timeout) — chạy finalize tự động...")

    import calibration_finalize
    calibration_finalize.main()


if __name__ == "__main__":
    main()
