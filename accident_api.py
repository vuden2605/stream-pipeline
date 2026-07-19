import os
import sys
import time
from pathlib import Path

import redis
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Service này chỉ đọc/ghi các key Redis riêng của accident pipeline
# (accident:pending/image/meta, xem accident_worker.py) — không import
# config.py để tránh phải nạp cameras_with_zones_merged.json/default_traffic.json
# không cần thiết, và không đụng gì tới pipeline traffic cũ.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

REDIS_ACCIDENT_PENDING_KEY = os.getenv("REDIS_ACCIDENT_PENDING_KEY", "accident:pending")
ACCIDENT_IMAGE_KEY_PREFIX = "accident:image:"
ACCIDENT_META_KEY_PREFIX = "accident:meta:"

VALID_STATUSES = {"PENDING", "APPROVED", "REJECTED"}

app = FastAPI(title="Accident Review API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)


def _decode_meta(raw: dict) -> dict:
    return {k.decode(): v.decode() for k, v in raw.items()}


@app.get("/api/accidents")
def list_accidents(status: str = "PENDING"):
    """Danh sách sự kiện, mới nhất trước. status: PENDING|APPROVED|REJECTED|ALL."""
    status = status.upper()
    if status != "ALL" and status not in VALID_STATUSES:
        raise HTTPException(400, f"status không hợp lệ: {status}")

    event_ids = _r.zrevrange(REDIS_ACCIDENT_PENDING_KEY, 0, -1)
    items = []
    for raw_id in event_ids:
        event_id = raw_id.decode()
        raw_meta = _r.hgetall(f"{ACCIDENT_META_KEY_PREFIX}{event_id}")
        if not raw_meta:
            continue  # meta đã hết TTL — sự kiện quá cũ, bỏ qua
        meta = _decode_meta(raw_meta)
        if status != "ALL" and meta.get("status") != status:
            continue
        items.append({"event_id": event_id, **meta})
    return items


@app.get("/api/accidents/{event_id}/image")
def get_accident_image(event_id: str):
    image_data = _r.get(f"{ACCIDENT_IMAGE_KEY_PREFIX}{event_id}")
    if image_data is None:
        raise HTTPException(404, "Ảnh không còn tồn tại (hết TTL hoặc event_id sai)")
    return Response(content=image_data, media_type="image/jpeg")


def _set_decision(event_id: str, decision: str) -> dict:
    meta_key = f"{ACCIDENT_META_KEY_PREFIX}{event_id}"
    if not _r.exists(meta_key):
        raise HTTPException(404, "Sự kiện không tồn tại (hết TTL hoặc sai event_id)")

    _r.hset(meta_key, mapping={"status": decision, "decided_at": int(time.time())})
    return {"event_id": event_id, **_decode_meta(_r.hgetall(meta_key))}


@app.post("/api/accidents/{event_id}/approve")
def approve_accident(event_id: str):
    return _set_decision(event_id, "APPROVED")


@app.post("/api/accidents/{event_id}/reject")
def reject_accident(event_id: str):
    return _set_decision(event_id, "REJECTED")


_static_dir = Path(__file__).parent / "admin_dashboard"
if _static_dir.is_dir():
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="dashboard")
