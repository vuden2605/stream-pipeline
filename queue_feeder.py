import sys
import time
import logging

import redis

from config import (
    CAMERAS, INTERVAL_SECONDS,
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_QUEUE_KEY,
)

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("queue_feeder")

# Nếu queue vẫn còn tồn quá % này so với tổng camera từ chu kỳ trước, bỏ qua
# chu kỳ hiện tại thay vì đẩy thêm — tránh unbounded growth khi worker không
# xử lý kịp (tương đương vấn đề consumer lag từng gặp bên Kafka).
BACKLOG_GUARD_RATIO = 0.5


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    total = len(CAMERAS)
    log.info("Queue feeder started — %d camera | chu kỳ %ds | queue=%s", total, INTERVAL_SECONDS, REDIS_QUEUE_KEY)

    while True:
        t0 = time.time()
        qlen_before = r.llen(REDIS_QUEUE_KEY)

        if qlen_before > total * BACKLOG_GUARD_RATIO:
            log.warning(
                "Queue còn tồn %d/%d camera từ chu kỳ trước — bỏ qua chu kỳ này để worker bắt kịp",
                qlen_before, total,
            )
        else:
            pipe = r.pipeline()
            for cam in CAMERAS:
                pipe.rpush(REDIS_QUEUE_KEY, cam["id"])
            pipe.execute()
            log.info("Đã đẩy %d camera_id vào queue (tồn trước đó: %d)", total, qlen_before)

        elapsed = time.time() - t0
        time.sleep(max(0, INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
