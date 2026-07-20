import sys
import time
import asyncio
import logging
from io import BytesIO
from pathlib import Path

import aiohttp
import numpy as np
import redis
from PIL import Image

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

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
CYCLE_SENTINEL = "__CYCLE_END__"

# Drain tối đa bao nhiêu camera mỗi vòng lặp
BATCH_SIZE = 100
# Số request HTTP đồng thời tối đa (semaphore + connector pool)
SEMAPHORE_LIMIT = 50

# Cookies lưu dạng dict, truyền vào mỗi ClientSession khi tạo — tránh khởi
# tạo CookieJar ở module level (yêu cầu event loop đang chạy).
_cookies: dict[str, str] = {}

_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=5)
_COOKIE_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

CAMERA_BY_ID = {cam["id"]: cam for cam in CAMERAS}

detector = TrafficDetector(AI_MODEL_PATH)
log.info("AI model loaded: %s", AI_MODEL_PATH)

_MEU_COEFFICIENTS = config_loader.getMeuCoefficients()
_CAMERA_THRESHOLDS = config_loader.loadJsonConfig("camera_thresholds.json")

COOKIE_REFRESH_INTERVAL_SECONDS = 3600

EMA_ALPHA = 0.3

RED_ENTER_TWF = 0.75
RED_EXIT_TWF = 0.65
YELLOW_ENTER_TWF = 0.40
YELLOW_EXIT_TWF = 0.30

_STATUS_VALUES = ("GREEN", "YELLOW", "RED")


def parse_prev_value(raw_val: str | None) -> tuple[float | None, str | None]:
    if not raw_val:
        return None, None
    parts = raw_val.split(":")
    if len(parts) < 2:
        return None, None
    try:
        prev_speed_ema = float(parts[0])
    except ValueError:
        return None, None
    prev_status = parts[2] if len(parts) >= 3 and parts[2] in _STATUS_VALUES else None
    return prev_speed_ema, prev_status


def classify_traffic_status(twf: float, prev_status: str | None) -> str:
    if prev_status == "RED":
        if twf >= RED_EXIT_TWF:
            return "RED"
        return "YELLOW" if twf >= YELLOW_EXIT_TWF else "GREEN"
    if prev_status == "YELLOW":
        if twf >= RED_ENTER_TWF:
            return "RED"
        return "GREEN" if twf < YELLOW_EXIT_TWF else "YELLOW"
    if prev_status == "GREEN":
        return "YELLOW" if twf >= YELLOW_ENTER_TWF else "GREEN"
    if twf >= RED_ENTER_TWF:
        return "RED"
    return "YELLOW" if twf >= YELLOW_ENTER_TWF else "GREEN"


def is_valid_jpeg(data: bytes) -> bool:
    return len(data) > 5_000 and data[:2] == b"\xff\xd8" and data[-2:] == b"\xff\xd9"


def get_meu_max(cam_id: str) -> float:
    return _CAMERA_THRESHOLDS.get(cam_id, {}).get("meuMax", MEU_MAX_DEFAULT)


# ── Async fetch layer ─────────────────────────────────────────────────────────

async def _refresh_cookie_async() -> None:
    jar = aiohttp.CookieJar(unsafe=True)
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(
        connector=connector,
        headers=BROWSER_HEADERS,
        cookie_jar=jar,
    ) as session:
        try:
            async with session.get(
                "https://giaothong.hochiminhcity.gov.vn/",
                timeout=_COOKIE_TIMEOUT,
            ) as resp:
                for morsel in jar:
                    _cookies[morsel.key] = morsel.value
                log.info("Cookie refreshed (HTTP %d)", resp.status)
        except Exception as e:
            log.warning("Cookie refresh failed: %r", e)


def refresh_cookie() -> None:
    asyncio.run(_refresh_cookie_async())


async def _fetch_one(
    session: aiohttp.ClientSession,
    camera: dict,
    sem: asyncio.Semaphore,
) -> tuple[dict, np.ndarray | None]:
    url = f"{camera['url']}&t={int(time.time() * 1000)}"
    async with sem:
        try:
            async with session.get(url, timeout=_FETCH_TIMEOUT) as resp:
                if resp.status != 200:
                    log.debug("[%s] HTTP %d", camera["id"], resp.status)
                    return camera, None
                data = await resp.read()
        except Exception as e:
            log.debug("[%s] fetch error: %r", camera["id"], e)
            return camera, None

    if not is_valid_jpeg(data):
        log.debug("[%s] not JPEG", camera["id"])
        return camera, None
    try:
        img = np.array(Image.open(BytesIO(data)).convert("RGB"))
        return camera, img
    except Exception as e:
        log.debug("[%s] decode error: %r", camera["id"], e)
        return camera, None


async def _fetch_all(cameras: list[dict]) -> list[tuple[dict, np.ndarray | None]]:
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    connector = aiohttp.TCPConnector(ssl=False, limit=SEMAPHORE_LIMIT)
    async with aiohttp.ClientSession(
        connector=connector,
        headers=BROWSER_HEADERS,
        cookies=_cookies,
    ) as session:
        return await asyncio.gather(
            *[_fetch_one(session, cam, sem) for cam in cameras],
            return_exceptions=False,
        )


# ── Queue helpers ─────────────────────────────────────────────────────────────

def drain_queue(r: redis.Redis, max_items: int) -> list[str]:
    """Block trên item đầu tiên (5s timeout), sau đó drain thêm non-blocking."""
    item = r.blpop(REDIS_QUEUE_KEY, timeout=5)
    if item is None:
        return []
    _, first_id = item
    ids = [first_id]
    for _ in range(max_items - 1):
        val = r.lpop(REDIS_QUEUE_KEY)
        if val is None:
            break
        ids.append(val)
    return ids


# ── Core processing ───────────────────────────────────────────────────────────

def process_batch(r: redis.Redis, cameras: list[dict]) -> tuple[int, int]:
    """
    Stage 1 — aiohttp + Semaphore: fetch đồng thời, giới hạn SEMAPHORE_LIMIT connections
    Stage 2 — batch YOLO inference (1 GPU forward pass)
    Stage 3 — hmget: đọc toàn bộ edge states trong 1 Redis round-trip
    Stage 4 — pipeline: gom toàn bộ writes trong 1 Redis round-trip

    Trả (ok, fail).
    """
    if not cameras:
        return 0, 0

    # Stage 1: async fetch
    raw: list[tuple[dict, np.ndarray | None]] = asyncio.run(_fetch_all(cameras))
    pairs = [(cam, img) for cam, img in raw if img is not None]

    fail = len(cameras) - len(pairs)
    if not pairs:
        return 0, fail

    # Stage 2: batch YOLO inference
    try:
        results = detector.predict_batch([img for _, img in pairs], conf=CONFIDENCE)
    except Exception as e:
        log.error("Batch inference error: %r", e)
        return 0, len(cameras)

    # Stage 3: batch Redis read — 1 round-trip cho toàn bộ edges
    all_edge_ids = [
        edge["edge_id"]
        for cam, _ in pairs
        for edge in cam.get("edges", [])
    ]
    prev_map: dict[str, str | None] = {}
    if all_edge_ids:
        prev_values = r.hmget(REDIS_SPEEDS_KEY, all_edge_ids)
        prev_map = dict(zip(all_edge_ids, prev_values))

    # Stage 4: compute + gom toàn bộ writes vào 1 pipeline
    ts = int(time.time())
    pipe = r.pipeline()
    ok = 0

    for (camera, _), result in zip(pairs, results):
        cam_id = camera["id"]
        edges = camera.get("edges", [])
        if not edges:
            ok += 1
            continue
        try:
            precise_occupancy = metrics.calculatePreciseOccupancyRatio(result, cam_id)
            camera_thresholds = {"meuMax": get_meu_max(cam_id)}
            density = metrics.calculateTrafficWeightFactor(
                precise_occupancy, result, cam_id, _MEU_COEFFICIENTS, camera_thresholds,
            )
            for edge in edges:
                edge_id = edge["edge_id"]
                free_flow = DEFAULT_EDGE_SPEED_KMH.get(edge_id, FREE_FLOW_DEFAULT_KMH)
                raw_speed = max(free_flow * (1 - density ** GREENSHIELDS_N), MIN_SPEED_KMH)

                prev_speed_ema, prev_status = parse_prev_value(prev_map.get(edge_id))
                speed_ema = (
                    raw_speed if prev_speed_ema is None
                    else EMA_ALPHA * raw_speed + (1 - EMA_ALPHA) * prev_speed_ema
                )
                twf_ngam = 1 - speed_ema / free_flow
                status = classify_traffic_status(twf_ngam, prev_status)

                pipe.hset(REDIS_SPEEDS_KEY, edge_id, f"{speed_ema:.1f}:{ts}:{status}")
                pipe.hexpire(REDIS_SPEEDS_KEY, REDIS_FIELD_TTL_SECONDS, edge_id)

            ok += 1
        except Exception as e:
            log.debug("[%s] processing error: %r", cam_id, e)
            fail += 1

    pipe.execute()

    if r.set(REDIS_PUBLISH_LOCK_KEY, "1", nx=True, ex=PUBLISH_DEBOUNCE_SECONDS):
        r.publish(REDIS_CHANNEL, "update")

    return ok, fail


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    refresh_cookie()
    last_cookie_refresh = time.time()

    log.info(
        "Worker started — %d cameras | queue='%s' | batch=%d semaphore=%d",
        len(CAMERAS), REDIS_QUEUE_KEY, BATCH_SIZE, SEMAPHORE_LIMIT,
    )

    cycle_ok = 0
    cycle_fail = 0
    cycle_start = time.time()

    while True:
        if time.time() - last_cookie_refresh > COOKIE_REFRESH_INTERVAL_SECONDS:
            refresh_cookie()
            last_cookie_refresh = time.time()

        ids = drain_queue(r, BATCH_SIZE)
        if not ids:
            continue

        sentinel_found = CYCLE_SENTINEL in ids
        cam_ids = [i for i in ids if i != CYCLE_SENTINEL]

        cameras = []
        for cam_id in cam_ids:
            cam = CAMERA_BY_ID.get(cam_id)
            if cam is None:
                log.debug("cam_id=%s không có trong config — bỏ qua", cam_id)
                cycle_fail += 1
            else:
                cameras.append(cam)

        try:
            ok, fail = process_batch(r, cameras)
            cycle_ok += ok
            cycle_fail += fail
        except Exception as e:
            log.error("Batch error: %r", e)
            cycle_fail += len(cameras)

        if sentinel_found:
            elapsed = time.time() - cycle_start
            queue_remaining = r.llen(REDIS_QUEUE_KEY)
            log.info(
                "Cycle %.1fs | ok=%d fail=%d total=%d queue_remaining=%d",
                elapsed, cycle_ok, cycle_fail, cycle_ok + cycle_fail, queue_remaining,
            )
            cycle_ok = 0
            cycle_fail = 0
            cycle_start = time.time()


if __name__ == "__main__":
    main()
