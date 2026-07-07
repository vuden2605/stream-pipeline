import asyncio, aiohttp, time, logging, hashlib, random, signal, json, sys
from datetime import datetime, timezone, timedelta
import aioboto3
from aiokafka import AIOKafkaProducer
from config import (
    KAFKA_BROKER, TOPIC_RAW_IMAGES,
    CAMERAS, INTERVAL_SECONDS, BROWSER_HEADERS,
    AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, BUCKET_NAME,
)

# Console Windows mặc định dùng codepage cp125x, không encode được ký tự
# box-drawing (━) hay tiếng Việt có dấu trong log → crash UnicodeEncodeError.
# Ép stdout/stderr sang UTF-8 để chạy được trên mọi console, mọi OS.
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

MAX_CONCURRENT_REQUESTS = 30
COOKIE_REFRESH_INTERVAL = 3600
VN_TZ = timezone(timedelta(hours=7))

S3_PREFIX = "raw_images"

logging.getLogger("aiokafka").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion")


def is_valid_jpeg(data: bytes) -> bool:
    return (
        len(data) > 5_000
        and data[:2] == b"\xff\xd8"
        and data[-2:] == b"\xff\xd9"
    )

def get_image_hash(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def get_current_interval() -> int:
    current_hour = datetime.now(VN_TZ).hour
    if current_hour >= 22 or current_hour < 6:
        return 10
    return INTERVAL_SECONDS


async def upload_to_s3(s3_client, image_data: bytes, s3_key: str) -> None:
    """Upload raw JPEG bytes to S3."""
    await s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=image_data,
        ContentType="image/jpeg",
    )


async def process_camera(
    session: aiohttp.ClientSession,
    producer: AIOKafkaProducer,
    s3_client,
    camera: dict,
    index: int,
    total_cams: int,
    sem: asyncio.Semaphore,
    last_hashes: dict,
    current_interval: int,
) -> tuple:
    cam_id = camera["id"]

    base_delay = (index / total_cams) * (current_interval * 0.8)
    jitter     = random.uniform(0, current_interval * 0.2)
    await asyncio.sleep(base_delay + jitter)

    url = f"{camera['url']}&t={int(time.time() * 1000)}"
    timeout = aiohttp.ClientTimeout(total=10, connect=5)

    async with sem:
        try:
            async with session.get(
                url, headers=BROWSER_HEADERS, timeout=timeout, ssl=False
            ) as resp:
                if resp.status != 200:
                    return cam_id, "http_error", f"HTTP {resp.status}"
                image_data = await resp.read()

            if not is_valid_jpeg(image_data):
                return cam_id, "invalid_image", "Not JPEG"

            img_hash = get_image_hash(image_data)
            if last_hashes.get(cam_id) == img_hash:
                return cam_id, "dedup_skipped", ""

            last_hashes[cam_id] = img_hash

            ts_ms  = int(time.time() * 1000)
            s3_key = f"{S3_PREFIX}/{cam_id}/{ts_ms}.jpg"

            # Upload ảnh lên S3
            await upload_to_s3(s3_client, image_data, s3_key)

            # Chỉ gửi metadata + s3_key vào Kafka (không gửi raw bytes)
            payload = json.dumps({
                "s3_key":    s3_key,
                "cam_id":    cam_id,
                "timestamp": ts_ms,
            }).encode()

            headers = [
                ("camera_id", cam_id.encode("utf-8")),
                ("timestamp", str(ts_ms).encode("utf-8")),
            ]

            await producer.send_and_wait(
                TOPIC_RAW_IMAGES,
                value=payload,
                key=cam_id.encode("utf-8"),
                headers=headers,
            )

            return cam_id, "success", ""

        except asyncio.TimeoutError:
            return cam_id, "timeout", "Timeout"
        except Exception as e:
            return cam_id, "exception", repr(e)


COOKIE_RETRY_ATTEMPTS = 5
COOKIE_RETRY_BASE_DELAY = 3.0   # giây, nhân đôi mỗi lần thất bại

async def refresh_cookie(
    http_session: aiohttp.ClientSession,
    *,
    required: bool = False,
) -> bool:
    """
    Thử lấy session cookie từ trang chủ.

    - required=True  → retry với exponential backoff cho đến khi thành công
                        (dùng lần đầu khởi động — không có cookie thì không fetch được)
    - required=False → thử 1 lần, log warning nếu lỗi, trả về False
                        (dùng trong vòng lặp định kỳ — cookie cũ vẫn còn dùng được)
    Trả về True nếu thành công.
    """
    attempts = COOKIE_RETRY_ATTEMPTS if required else 1

    for attempt in range(1, attempts + 1):
        try:
            async with http_session.get(
                "https://giaothong.hochiminhcity.gov.vn/",
                ssl=False,
                timeout=aiohttp.ClientTimeout(total=15),
                headers=BROWSER_HEADERS,
            ) as resp:
                body = await resp.read()   # đảm bảo Set-Cookie được xử lý
                if resp.status == 200:
                    log.info("Session cookie refreshed (HTTP %d, %d bytes)", resp.status, len(body))
                    return True
                # server trả về non-200 cũng log rõ
                log.warning("Cookie refresh: HTTP %d (attempt %d/%d)", resp.status, attempt, attempts)
        except Exception as e:
            # repr(e) hiển thị cả type lẫn message — tránh chuỗi rỗng
            log.warning("Cookie refresh failed (attempt %d/%d): %s", attempt, attempts, repr(e))

        if attempt < attempts:
            delay = COOKIE_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            log.info("Retrying cookie refresh in %.0fs...", delay)
            await asyncio.sleep(delay)

    log.error("Cookie refresh exhausted %d attempts — cameras may return 403", attempts)
    return False


async def main():
    shutdown = asyncio.Event()

    def _request_shutdown(*_):
        log.info("Shutdown signal received")
        shutdown.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT,  _request_shutdown)

    # Kafka producer — không cần max_request_size lớn vì chỉ gửi JSON nhỏ
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    log.info("Ingestion service started — %d cameras | bucket: %s", len(CAMERAS), BUCKET_NAME)

    last_hashes = {}
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    boto_session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    try:
        async with boto_session.client("s3") as s3_client:
            connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2)
            async with aiohttp.ClientSession(
                connector=connector,
                cookie_jar=aiohttp.CookieJar(unsafe=True),
            ) as http_session:

                await refresh_cookie(http_session, required=True)
                last_cookie_refresh = time.time()
                total_cams = len(CAMERAS)

                while not shutdown.is_set():
                    t0 = time.time()
                    current_interval = get_current_interval()

                    if t0 - last_cookie_refresh > COOKIE_REFRESH_INTERVAL:
                        ok = await refresh_cookie(http_session)
                        if ok:
                            last_cookie_refresh = t0

                    tasks = [
                        process_camera(
                            http_session, producer, s3_client,
                            cam, i, total_cams, sem, last_hashes, current_interval,
                        )
                        for i, cam in enumerate(CAMERAS)
                    ]
                    results = await asyncio.gather(*tasks)

                    stats = {
                        "success": [], "dedup_skipped": [], "timeout": [],
                        "http_error": [], "invalid_image": [], "exception": [],
                    }
                    error_messages = set()

                    for cam_id, status, err_msg in results:
                        stats[status].append(cam_id)
                        if err_msg:
                            error_messages.add(err_msg)

                    elapsed    = time.time() - t0
                    sleep_time = max(0, current_interval - elapsed)
                    total_errors = sum(len(stats[k]) for k in ("timeout", "http_error", "invalid_image", "exception"))

                    # Nếu toàn bộ camera trả 403 → cookie hết hạn, refresh ngay
                    http_403_count = sum(
                        1 for cam_id, status, msg in results
                        if status == "http_error" and "403" in msg
                    )
                    if http_403_count > 0 and http_403_count == len([
                        r for r in results if r[1] != "dedup_skipped"
                    ]):
                        log.warning("All cameras returned 403 — forcing cookie refresh")
                        ok = await refresh_cookie(http_session, required=True)
                        if ok:
                            last_cookie_refresh = time.time()

                    print("\n" + "━" * 70)
                    log.info("Cycle complete — elapsed: %.1fs | mode: %ds", elapsed, current_interval)
                    log.info("Uploaded to S3: %d | Skipped (duplicate): %d | Errors: %d",
                             len(stats["success"]), len(stats["dedup_skipped"]), total_errors)
                    if total_errors > 0:
                        log.warning("Top error reasons: %s", list(error_messages)[:3])
                    print("━" * 70 + "\n")

                    try:
                        await asyncio.wait_for(shutdown.wait(), timeout=sleep_time)
                    except asyncio.TimeoutError:
                        pass

    finally:
        log.info("Shutting down ingestion service...")
        await producer.stop()
        log.info("Ingestion service stopped")


if __name__ == "__main__":
    asyncio.run(main())