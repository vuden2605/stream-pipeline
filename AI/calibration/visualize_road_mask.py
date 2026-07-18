"""
Công cụ trực quan hóa ma trận heatmap/road mask của 1 camera bằng OpenCV.

Chạy: python visualize_road_mask.py <cam_id> [--no-fetch]
Kết quả: 2 ảnh PNG lưu vào AI/storage/visualizations/
  - {cam_id}_heatmap.png    : heatmap tô màu (JET) đè lên ảnh nền camera
  - {cam_id}_road_mask.png  : vùng được chấp nhận là mặt đường tô xanh lá
"""
import argparse
import os
import sys

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import cv2
import numpy as np
import requests
import urllib3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.core.metrics import MATRIX_STORAGE_DIR  # noqa: E402

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(_AI_DIR, "storage", "visualizations")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CAMERA_SNAPSHOT_URL = (
    "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx"
    "?id={cam_id}&bg=black&w=500&h=500"
)
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Referer": "https://giaothong.hochiminhcity.gov.vn/",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}


def fetchBackgroundImage(camId, shape, noFetch=False):
    """Tải ảnh nền thật của camera để đè ma trận lên cho dễ đối chiếu; lỗi mạng/--no-fetch thì dùng nền đen."""
    imgH, imgW = shape
    if not noFetch:
        try:
            resp = requests.get(
                CAMERA_SNAPSHOT_URL.format(cam_id=camId),
                headers=BROWSER_HEADERS, timeout=10, verify=False,
            )
            data = np.frombuffer(resp.content, dtype=np.uint8)
            img = cv2.imdecode(data, cv2.IMREAD_COLOR)
            if img is not None:
                return cv2.resize(img, (imgW, imgH))
        except Exception as e:
            print(f"[Cảnh báo] Không tải được ảnh nền camera {camId}: {e!r} — dùng nền đen")
    return np.zeros((imgH, imgW, 3), dtype=np.uint8)


def visualizeHeatmap(camId, background):
    heatmapPath = os.path.join(MATRIX_STORAGE_DIR, f"{camId}_heatmap.npy")
    if not os.path.exists(heatmapPath):
        print(f"[Lỗi] Không tìm thấy {heatmapPath}")
        return

    heatmap = np.load(heatmapPath)
    normalized = cv2.normalize(heatmap, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)
    colored = cv2.applyColorMap(normalized, cv2.COLORMAP_JET)
    blended = cv2.addWeighted(background, 0.5, colored, 0.5, 0)

    outPath = os.path.join(OUTPUT_DIR, f"{camId}_heatmap.png")
    cv2.imwrite(outPath, blended)
    print(f"[OK] Heatmap: giá trị lớn nhất={int(heatmap.max())}, lưu -> {outPath}")


def visualizeRoadMask(camId, background):
    maskPath = os.path.join(MATRIX_STORAGE_DIR, f"{camId}_road_mask.npy")
    if not os.path.exists(maskPath):
        print(f"[Lỗi] Không tìm thấy {maskPath}")
        return

    mask = np.load(maskPath)
    overlay = background.copy()
    overlay[mask == 1] = (0, 255, 0)  # xanh lá = pixel được chấp nhận là mặt đường
    blended = cv2.addWeighted(background, 0.6, overlay, 0.4, 0)

    roadRatio = mask.mean() * 100
    outPath = os.path.join(OUTPUT_DIR, f"{camId}_road_mask.png")
    cv2.imwrite(outPath, blended)
    print(f"[OK] Road Mask: {roadRatio:.1f}% diện tích ảnh được coi là mặt đường, lưu -> {outPath}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trực quan hóa heatmap/road mask của 1 camera")
    parser.add_argument("cam_id", help="ID camera, ví dụ: 56de42f611f398ec0c48127d")
    parser.add_argument("--no-fetch", action="store_true", help="Không tải ảnh nền camera, dùng nền đen")
    args = parser.parse_args()

    referenceShapePath = os.path.join(MATRIX_STORAGE_DIR, f"{args.cam_id}_heatmap.npy")
    if not os.path.exists(referenceShapePath):
        print(f"[Lỗi] Camera {args.cam_id} chưa có dữ liệu heatmap trong {MATRIX_STORAGE_DIR}")
        sys.exit(1)

    shape = np.load(referenceShapePath).shape
    background = fetchBackgroundImage(args.cam_id, shape, noFetch=args.no_fetch)

    visualizeHeatmap(args.cam_id, background)
    visualizeRoadMask(args.cam_id, background)
