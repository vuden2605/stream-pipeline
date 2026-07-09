import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# _THIS_DIR = AI/calibration, _AI_DIR = AI/, _ROOT_DIR = stream-pipeline (root repo)
_THIS_DIR = Path(__file__).parent.resolve()
_AI_DIR = _THIS_DIR.parent
_ROOT_DIR = _AI_DIR.parent

# ── Redis — dùng chung instance với pipeline chính nhưng namespace key riêng,
# để không xung đột với camera_queue/traffic:speeds của queue_feeder.py/worker.py.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

CALIBRATION_QUEUE_KEY = os.getenv("CALIBRATION_QUEUE_KEY", "calibration_queue")
CALIBRATION_MEU_MAX_KEY = os.getenv("CALIBRATION_MEU_MAX_KEY", "calibration:meu_max")
CALIBRATION_FRAME_COUNT_KEY = os.getenv("CALIBRATION_FRAME_COUNT_KEY", "calibration:frame_count")
CALIBRATION_DONE_KEY = os.getenv("CALIBRATION_DONE_KEY", "calibration:done")

INTERVAL_SECONDS = 10
DURATION_SECONDS = 4 * 3600  # chạy đúng 4 giờ
DRAIN_TIMEOUT_SECONDS = 5 * 60  # đợi queue rỗng tối đa 5 phút sau khi hết 4h rồi mới finalize

BACKLOG_GUARD_RATIO = 0.5  # giống queue_feeder.py — bỏ qua chu kỳ nếu queue tồn > 50%

ROAD_THRESHOLD_RATIO = 0.30  # >=30% số frame thật thu được của camera có xe -> coi là mặt đường
MIN_RELIABLE_FRAMES = 50  # camera có ít hơn số frame này thì cảnh báo dữ liệu không đáng tin

CONFIDENCE = float(os.getenv("CONFIDENCE", "0.3"))
AI_MODEL_PATH = os.getenv("AI_MODEL_PATH", str(_AI_DIR / "weights" / "best.pt"))

CAMERA_THRESHOLDS_PATH = str(_AI_DIR / "config" / "camera_thresholds.json")

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "close",
}

# Danh sách camera lấy trực tiếp từ cameras_with_zones_merged.json ở thư mục gốc
# (không import config.py của root — stream calibration độc lập hoàn toàn với
# pipeline chính, không cần dữ liệu edges/Valhalla). Lọc cam.get("zones") giống
# hệt root config.py để tập camera trùng khớp với pipeline chính (611 camera).
_CAMERAS_FILE = _ROOT_DIR / "cameras_with_zones_merged.json"
with open(_CAMERAS_FILE, encoding="utf-8") as _f:
    _camera_configs = json.load(_f)

_CAMERA_LIMIT = os.getenv("CAMERA_LIMIT")
_CAMERA_LIMIT = int(_CAMERA_LIMIT) if _CAMERA_LIMIT else None

CAMERAS = [
    {
        "id": cam["cam_id"],
        "url": (
            "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx"
            f"?id={cam['cam_id']}&bg=black&w=500&h=500"
        ),
    }
    for cam in _camera_configs
    if cam.get("zones")
][:_CAMERA_LIMIT]
