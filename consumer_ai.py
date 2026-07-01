import asyncio, json, io, logging, signal
import numpy as np
import aioboto3
from PIL import Image
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from ultralytics import YOLO
from config import (
    KAFKA_BROKER, TOPIC_RAW_IMAGES, TRAFFIC_EVENT_TOPIC,
    MODEL_PATH, CONFIDENCE,
    CLASS_NAMES as LABEL_MAP,
    AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKET_NAME,
)
from pathlib import Path

CAMERA_CONFIG_PATH = Path("cameras.json")
with open(CAMERA_CONFIG_PATH, "r", encoding="utf-8") as f:
    camera_configs = json.load(f)

CAMERA_EDGE_MAP = {
    cam["id"]: [e["edge_id"] for e in cam.get("edges", [])]
    for cam in camera_configs
}

EDGE_META = {
    edge["edge_id"]: {
        "way_id":    edge.get("way_id"),
        "direction": edge.get("direction"),
    }
    for cam in camera_configs
    for edge in cam.get("edges", [])
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer_ai")

model = YOLO(MODEL_PATH)
log.info("Model loaded: %s", MODEL_PATH)
log.info("Class mapping: %s", LABEL_MAP)

CONCURRENCY = 4
_sem = asyncio.Semaphore(CONCURRENCY)


async def download_from_s3(s3_client, s3_key: str) -> bytes:
    """Tải ảnh từ S3, trả về raw bytes."""
    response = await s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    async with response["Body"] as stream:
        return await stream.read()


async def process_frame(producer, s3_client, msg) -> None:
    # Kafka message value là JSON payload từ ingestion
    payload = json.loads(msg.value.decode())
    s3_key  = payload["s3_key"]
    cam_id  = payload["cam_id"]
    ts_ms   = payload["timestamp"]

    async with _sem:
        # Tải ảnh từ S3
        image_data = await download_from_s3(s3_client, s3_key)
        image      = np.array(Image.open(io.BytesIO(image_data)).convert("RGB"))

        results = (await asyncio.to_thread(model, image, conf=CONFIDENCE, verbose=False))[0]

        counts: dict[str, int] = {}
        for box in results.boxes:
            cls_id   = int(box.cls[0])
            cls_name = LABEL_MAP.get(cls_id, f"class_{cls_id}")
            counts[cls_name] = counts.get(cls_name, 0) + 1

        total = sum(counts.values())

        # ── LOG KẾT QUẢ ĐẾM ─────────────────────────────────────────────
        count_str = ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "none"
        log.info("[%s] detected %d vehicles | %s", cam_id, total, count_str)
        # ─────────────────────────────────────────────────────────────────

        edges = CAMERA_EDGE_MAP.get(cam_id)
        if not edges:
            log.warning("[%s] no edge mapping — result discarded (total=%d)", cam_id, total)
            return

        for edge_id in edges:
            meta = EDGE_META.get(edge_id, {})
            traffic_event = {
                "event_id":       f"cam_{edge_id}_{ts_ms}",
                "source_type":    "camera",
                "edge_id":        edge_id,
                "way_id":         meta.get("way_id"),
                "timestamp":      ts_ms,
                "vehicle_counts": counts,
                "total_vehicles": total,
                "speed_kmh":      None,  # Placeholder, có thể bổ sung nếu có model đo tốc độ
                "confidence":      None
            }
            await producer.send_and_wait(
                TRAFFIC_EVENT_TOPIC,
                key=edge_id.encode("utf-8"),
                value=json.dumps(traffic_event).encode(),
            )
            log.info("[%s] -> published edge=%s  total=%d", cam_id, edge_id, total)


async def main():
    shutdown = asyncio.Event()

    def _request_shutdown(*_):
        log.info("Shutdown signal received")
        shutdown.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT,  _request_shutdown)

    consumer = AIOKafkaConsumer(
        TOPIC_RAW_IMAGES,
        bootstrap_servers=KAFKA_BROKER,
        group_id="ai-detector",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)

    await consumer.start()
    await producer.start()
    log.info("AI consumer started — [%s] -> [%s]", TOPIC_RAW_IMAGES, TRAFFIC_EVENT_TOPIC)

    boto_session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    try:
        async with boto_session.client("s3") as s3_client:
            while not shutdown.is_set():
                batch_data = await consumer.getmany(timeout_ms=1000, max_records=CONCURRENCY * 4)

                if not batch_data:
                    continue

                tasks = [
                    process_frame(producer, s3_client, msg)
                    for tp, messages in batch_data.items()
                    for msg in messages
                ]

                try:
                    await asyncio.gather(*tasks)
                    await consumer.commit()
                except Exception as e:
                    log.error("Batch processing failed, skipping commit — will retry: %s", e)

    finally:
        log.info("Shutting down AI consumer...")
        await consumer.stop()
        await producer.stop()
        log.info("AI consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())