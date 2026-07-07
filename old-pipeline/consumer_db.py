import asyncio, json, logging, sys
import asyncpg
from aiokafka import AIOKafkaConsumer, TopicPartition
from config import KAFKA_BROKER, TOPIC_COUNTS, DB_URL

# Console Windows mặc định dùng codepage cp125x, không encode được tiếng Việt
# có dấu trong log → crash UnicodeEncodeError. Ép UTF-8 để chạy được mọi nơi.
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer_db")


# ── Tạo bảng TimescaleDB ──────────────────────────────────────────────
SETUP_SQL = """
CREATE TABLE IF NOT EXISTS vehicle_counts (
    id         BIGSERIAL PRIMARY KEY,
    time       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    camera_id  TEXT        NOT NULL,
    counts     JSONB       NOT NULL,
    total      INT         DEFAULT 0
);


CREATE INDEX IF NOT EXISTS idx_vehicle_counts_camera_time
    ON vehicle_counts (camera_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_counts_counts
    ON vehicle_counts USING GIN (counts);
"""

# ── Query gợi ý ───────────────────────────────────────────────────────
# Heatmap (5 phút):
#   SELECT time_bucket('5 minutes', time) AS bucket, camera_id, SUM(total)
#   FROM vehicle_counts
#   WHERE time > NOW() - INTERVAL '1 hour'
#   GROUP BY bucket, camera_id ORDER BY bucket DESC;
#
# Live traffic (1 phút gần nhất):
#   SELECT camera_id, SUM(total)
#   FROM vehicle_counts
#   WHERE time > NOW() - INTERVAL '1 minute'
#   GROUP BY camera_id;
#
# Giờ cao điểm:
#   SELECT EXTRACT(HOUR FROM time) AS hour, AVG(total)
#   FROM vehicle_counts
#   GROUP BY hour ORDER BY hour;


INSERT_SQL = """
INSERT INTO vehicle_counts (time, camera_id, counts, total)
VALUES (to_timestamp($1::double precision / 1000), $2, $3::jsonb, $4)
"""


async def main():
    consumer = AIOKafkaConsumer(
        TOPIC_COUNTS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="db-writer",
        auto_offset_reset="latest",
        enable_auto_commit=False,   # commit thủ công sau khi insert thành công
    )
    await consumer.start()

    # Fix 2: dùng connection pool thay vì single connection
    # Pool tự reconnect khi connection die, hỗ trợ concurrent inserts
    pool = await asyncpg.create_pool(
        DB_URL,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )

    async with pool.acquire() as conn:
        await conn.execute(SETUP_SQL)

    log.info("Consumer DB: %s → TimescaleDB", TOPIC_COUNTS)

    try:
        async for msg in consumer:
            payload = json.loads(msg.value)
            cam_id  = payload["camera_id"]
            ts_ms   = payload["timestamp"]
            counts  = payload["counts"]
            total   = payload["total"]

            # Fix 3: error handling riêng cho từng insert
            # Lỗi DB không được làm mất message (không commit khi lỗi)
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        INSERT_SQL,
                        ts_ms, cam_id, json.dumps(counts), total,
                    )
                # Chỉ commit Kafka offset khi insert DB thành công
                await consumer.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})
                log.info("[%s] DB ✓ counts=%s total=%d", cam_id, counts, total)

            except asyncpg.PostgresError as exc:
                # Lỗi DB (constraint, connection) → không commit offset
                # → Kafka sẽ re-deliver message sau khi restart
                log.error("[%s] DB insert lỗi ts=%s: %s", cam_id, ts_ms, exc)

            except Exception as exc:
                log.error("[%s] Lỗi không xác định: %s", cam_id, exc)

    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())