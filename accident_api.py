import io
import logging
import os
import sys
import threading
import time
from pathlib import Path

import numpy as np
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from PIL import Image, UnidentifiedImageError

# Thư mục AI/ có package riêng (src/core) import kiểu tương đối — cần thêm
# AI/ vào sys.path để dùng được từ accident_api.py chạy ở root repo (giống
# accident_worker.py).
sys.path.insert(0, str(Path(__file__).parent / "AI"))
from src.core.accident_detector import AccidentDetector, INCIDENT_CLASSES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("accident_api")

# Cùng biến môi trường với accident_worker.py — service này dùng chung model
# best.pt nhưng chạy trong process/container riêng, không chia sẻ state.
ACCIDENT_MODEL_PATH = os.getenv(
    "ACCIDENT_MODEL_PATH",
    str(Path(__file__).parent / "AI" / "accident-detection" / "weights" / "best.pt"),
)
DEFAULT_CONFIDENCE = float(os.getenv("ACCIDENT_CONFIDENCE", "0.4"))

# Chặn ảnh quá lớn ngay từ input — tránh 1 request duy nhất tốn quá nhiều
# RAM khi decode (PIL) trước khi kịp trả lỗi.
MAX_UPLOAD_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_SIZE_BYTES", str(10 * 1024 * 1024)))

detector = AccidentDetector(ACCIDENT_MODEL_PATH)
log.info("Accident model loaded: %s", ACCIDENT_MODEL_PATH)

# ultralytics YOLO không đảm bảo an toàn khi nhiều thread gọi predict() đồng
# thời trên cùng 1 model instance — FastAPI chạy route "def" (sync) trong
# threadpool nên nhiều request /detect có thể chồng thread. Khoá lại để các
# lần predict chạy tuần tự, đổi lấy đúng kết quả thay vì tối đa hoá thông
# lượng (chấp nhận được vì đây là API on-demand, không phải hot path).
_inference_lock = threading.Lock()

app = FastAPI(title="Accident Detection API")


@app.get("/health")
def health():
    return {"status": "ok", "model_path": ACCIDENT_MODEL_PATH}


@app.post("/detect")
def detect(
    image: UploadFile = File(...),
    conf: float = Query(default=DEFAULT_CONFIDENCE, ge=0.0, le=1.0),
):
    if image.content_type is None or not image.content_type.startswith("image/"):
        raise HTTPException(status_code=422, detail=f"Content-Type phải là image/*, nhận: {image.content_type}")

    raw = image.file.read()
    if not raw:
        raise HTTPException(status_code=422, detail="File ảnh rỗng")
    if len(raw) > MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(status_code=413, detail=f"Ảnh vượt quá giới hạn {MAX_UPLOAD_SIZE_BYTES} bytes")

    try:
        pil_image = Image.open(io.BytesIO(raw)).convert("RGB")
    except UnidentifiedImageError:
        raise HTTPException(status_code=400, detail="Không đọc được ảnh — file hỏng hoặc sai định dạng")

    np_image = np.array(pil_image)

    try:
        with _inference_lock:
            t0 = time.time()
            result = detector.predict(np_image, conf=conf, save=False)
            elapsed_ms = (time.time() - t0) * 1000
    except Exception as e:
        log.error("Inference thất bại: %r", e)
        raise HTTPException(status_code=500, detail="Lỗi khi chạy model detect")

    incidents = detector.extractIncidentBoxes(result, INCIDENT_CLASSES)
    img_h, img_w = result.orig_shape

    return {
        "accident_detected": len(incidents) > 0,
        "num_detections": len(incidents),
        "detections": incidents,
        "image_size": {"width": img_w, "height": img_h},
        "inference_time_ms": round(elapsed_ms, 1),
        "confidence_threshold": conf,
    }
