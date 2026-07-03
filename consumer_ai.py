import asyncio, json, io, logging, signal, sys
import numpy as np
import aioboto3
from PIL import Image
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Console Windows mặc định dùng codepage cp125x, không encode được tiếng Việt
# có dấu trong log → crash UnicodeEncodeError. Ép UTF-8 để chạy được mọi nơi.
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Thư mục AI/ có package riêng (src/core, src/data) import kiểu tương đối
# ("from src.core... import ...") — cần thêm AI/ vào sys.path để dùng được
# từ consumer_ai.py chạy ở root repo.
sys.path.insert(0, str(Path(__file__).parent / "AI"))
from src.core.detector import TrafficDetector
from src.core import metrics
from src.data import config_loader

from config import (
    KAFKA_BROKER, TOPIC_RAW_IMAGES, TRAFFIC_EVENT_TOPIC,
    AI_MODEL_PATH, CONFIDENCE, TWF_MAX_SPEED_KMH,
    CAMERAS, DEFAULT_EDGE_SPEED_KMH,
    AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKET_NAME,
)

CAMERA_EDGE_MAP = {
    cam["id"]: [e["edge_id"] for e in cam.get("edges", [])]
    for cam in CAMERAS
}

EDGE_META = {
    edge["edge_id"]: {
        "way_id":    edge.get("way_id"),
        "direction": edge.get("direction"),
    }
    for cam in CAMERAS
    for edge in cam.get("edges", [])
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer_ai")

detector = TrafficDetector(AI_MODEL_PATH)
log.info("AI model loaded: %s", AI_MODEL_PATH)

# Config tĩnh (hệ số MEU, trọng số HCI) — nạp 1 lần, dùng chung cho mọi camera.
# Ngưỡng riêng từng camera (meuMax/orMax/hciMax) tra theo cam_id trong process_frame.
_MEU_COEFFICIENTS = config_loader.getMeuCoefficients()
_HCI_WEIGHTS = config_loader.getHciWeights()

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

        r0 = await asyncio.to_thread(detector.predict, image, conf=CONFIDENCE, save=False)

        # Tiền điều kiện Road Mask cần vài chu kỳ tích luỹ heatmap mới có ý nghĩa
        # (xem AI/src/core/metrics.py). "Tạm thời" gọi lại mỗi frame giống AI/main.py
        # gốc — production thật nên chuyển sang lịch định kỳ (vd hàng ngày/hàng tháng).
        await asyncio.to_thread(metrics.updateHeatmapByCameraId, r0, cam_id)
        await asyncio.to_thread(metrics.generateAndSaveRoadMask, cam_id, 1)

        preciseOccupancyRatio = metrics.calculatePreciseOccupancyRatio(r0, cam_id)
        rawOccupancyRatio     = metrics.calculateOccupancyRatio(r0)
        meu                   = metrics.calculateMotorcycleEquivalentUnit(r0, _MEU_COEFFICIENTS)

        cameraThresholds = config_loader.getCameraThresholds(cam_id)
        hci = metrics.calculateHybridCongestionIndex(
            meu, rawOccupancyRatio,
            cameraThresholds.get("meuMax", 1.0), cameraThresholds.get("orMax", 1.0),
            _HCI_WEIGHTS,
        )
        twf = metrics.calculateTrafficWeightFactor(
            preciseOccupancyRatio=preciseOccupancyRatio,
            r=r0,
            cameraId=cam_id,
            meuCoefficients=_MEU_COEFFICIENTS,
            hciWeights=_HCI_WEIGHTS,
            cameraThresholds=cameraThresholds,
        )
        log.info(
            "[%s] TWF=%.2f HCI=%.2f MEU=%.2f occupancy=%.1f%%",
            cam_id, twf, hci, meu, preciseOccupancyRatio,
        )

        edges = CAMERA_EDGE_MAP.get(cam_id)
        if not edges:
            log.warning("[%s] no edge mapping — result discarded", cam_id)
            return

        for edge_id in edges:
            meta = EDGE_META.get(edge_id, {})
            # speed_kmh = TWF x tốc độ mặc định (free-flow) của CHÍNH edge đó,
            # tra trong default_traffic.json bằng đúng edge_id (giờ edge_id đã
            # LÀ GraphId đầy đủ, khớp thẳng với key của default_traffic.json,
            # không cần trường graph_value riêng nữa — xem config.py). Mỗi edge
            # của cùng 1 camera có thể có tốc độ mặc định khác nhau (khác
            # road_class), nên phải tính riêng từng edge — không dùng chung 1
            # speed cho cả camera. Edge không có trong default_traffic.json
            # (phần lớn, ~67%) thì fallback về TWF_MAX_SPEED_KMH (mặc định 50).
            default_speed = DEFAULT_EDGE_SPEED_KMH.get(edge_id)
            base_speed_kmh = default_speed if default_speed is not None else TWF_MAX_SPEED_KMH
            speed_kmh = round(twf * base_speed_kmh, 2)

            traffic_event = {
                "event_id":    f"cam_{edge_id}_{ts_ms}",
                # Tái dùng nhánh "gps" của Flink job (EWMA speed) cho speed suy ra
                # từ camera qua TWF — không có nguồn GPS thật trong hệ thống này.
                "source_type": "gps",
                "edge_id":     edge_id,
                "way_id":      meta.get("way_id"),
                "timestamp":   ts_ms,
                "speed_kmh":   speed_kmh,
                # AI/metrics.py chưa có điểm tin cậy riêng cho TWF — đặt cố định 1.0,
                # có thể cải tiến sau (vd suy từ conf trung bình của detection).
                "confidence":  1.0,
            }
            await producer.send_and_wait(
                TRAFFIC_EVENT_TOPIC,
                key=edge_id.encode("utf-8"),
                value=json.dumps(traffic_event).encode(),
            )
            log.info("[%s] -> published edge=%s speed=%.1fkm/h", cam_id, edge_id, speed_kmh)


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