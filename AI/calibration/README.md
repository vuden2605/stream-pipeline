# Calibration Stream — Heatmap + meuMax

Stream **độc lập hoàn toàn** với pipeline chính (`queue_feeder.py` + `worker.py` ở root repo). Chạy 1 lần trong **4 giờ**, 10 giây/frame cho tất cả camera, với 2 nhiệm vụ:

1. **Tích lũy heatmap** từng camera (`{cam_id}_heatmap.npy`) → cuối cùng trích ra **Road Mask** (`{cam_id}_road_mask.npy`), dùng bởi `calculatePreciseOccupancyRatio` trong pipeline chính.
2. **`meuMax`** từng camera = giá trị **MEU lớn nhất từng quan sát được** trong suốt 4h — ghi vào `AI/config/camera_thresholds.json`, dùng bởi `calculateTrafficWeightFactor` (`cameraThresholds.get("meuMax", 1.0)`).

Đây chính là bước hiệu chuẩn dữ liệu lịch sử mà pipeline chính đang thiếu (`worker.py` hiện chỉ **đọc** road mask/`meuMax`, không tự tích lũy) — xem `AI/README.md` mục "Traffic Weight Factor".

---

## Vì sao tách riêng, độc lập với pipeline chính

| | Pipeline chính (root) | Calibration stream (đây) |
|---|---|---|
| Redis queue | `camera_queue` | `calibration_queue` (khác hẳn) |
| Redis state khác | `traffic:speeds` | `calibration:meu_max`, `calibration:frame_count`, `calibration:done` |
| Ghi file `.npy` | chỉ **đọc** `{cam}_road_mask.npy` | **ghi** `{cam}_heatmap.npy` liên tục + `{cam}_road_mask.npy` lúc finalize |
| Ghi `camera_thresholds.json` | không bao giờ ghi | ghi đè field `meuMax` lúc finalize |
| Danh sách camera | `config.CAMERAS` (root `config.py`, có `edges` cho Valhalla) | đọc thẳng `cameras_with_zones_merged.json` ở root, không cần `edges` |
| Nguồn ảnh | `requests.Session` riêng của `worker.py` | `requests.Session` riêng của `calibration_worker.py` (process khác hẳn) |

→ Có thể chạy **song song** với pipeline chính (cùng lúc `queue_feeder.py`/`worker.py` và `calibration_feeder.py`/`calibration_worker.py`) mà không xung đột Redis key hay tranh chấp resource, ngoại trừ đúng 1 điểm giao nhau: **file `{cam}_road_mask.npy`** — pipeline chính đọc (`getRoadMaskByCameraId`), calibration ghi đè lúc finalize. Để tránh đọc phải file ghi dở, `generateAndSaveRoadMask` (`AI/src/core/metrics.py`) ghi atomic (temp file + `os.replace`).

**Lưu ý:** `worker.py` (pipeline chính) chỉ load `camera_thresholds.json` **1 lần lúc khởi động**. Sau khi calibration ghi `meuMax` mới, cần restart `worker.py` để áp dụng — nằm ngoài phạm vi của stream này, không tự động xử lý.

---

## Kiến trúc & Flow

```
cameras_with_zones_merged.json (root, lọc cam.get("zones") — khớp tập camera của pipeline chính)
        │
        ▼
┌──────────────────────────┐
│ calibration_feeder.py      │  mỗi 10s trong 4h: RPUSH cam_id vào "calibration_queue"
└──────────┬─────────────────┘  (backlog guard giống queue_feeder.py: bỏ qua chu kỳ nếu queue tồn > 50%)
           │
           ▼
   Redis LIST "calibration_queue"
           │
           ▼
┌────────────────────────────────────────────────────┐
│ calibration_worker.py (chạy 2-3 instance song song)   │
│  1. BLPOP cam_id                                        │
│  2. GET snapshot trực tiếp (RAM, không qua S3)           │
│  3. YOLO predict (dùng chung AI/src/core/detector.py)    │
│  4. updateHeatmapByCameraId → ghi {cam}_heatmap.npy      │
│  5. MEU = calculateMotorcycleEquivalentUnit              │
│  6. ZADD calibration:meu_max GT cam_id MEU  (atomic max) │
│  7. HINCRBY calibration:frame_count cam_id 1             │
└────────────────────────────────────────────────────┘
           │
           │  (feeder dừng đẩy sau 4h, đợi queue rỗng — timeout 5 phút)
           ▼
┌────────────────────────────────────────────────────┐
│ calibration_finalize.py (tự động, do feeder gọi)        │
│  - Với mỗi cam: threshold = frame_count × 30%            │
│    → generateAndSaveRoadMask(cam, threshold)             │
│    → ghi atomic {cam}_road_mask.npy                      │
│  - ZRANGE calibration:meu_max WITHSCORES                 │
│    → merge field "meuMax" vào camera_thresholds.json      │
│    → ghi atomic (temp file + os.replace)                 │
│  - Cảnh báo camera có frame_count < 50 (dữ liệu yếu)      │
│  - DEL calibration:meu_max calibration:frame_count        │
└────────────────────────────────────────────────────┘
```

### Vì sao dùng `ZADD ... GT` cho meuMax thay vì đọc-so sánh-ghi

`calibration_worker.py` chạy **nhiều instance song song** (giống `worker.py`). Nếu tính max bằng cách tự đọc giá trị cũ trong Redis, so sánh, rồi ghi lại — 2 instance cùng đọc một giá trị cũ rồi cùng ghi sẽ mất dữ liệu (race condition). Redis sorted set với cờ `GT` (`ZADD key GT CH score member`, Redis ≥ 6.2) chỉ ghi nếu giá trị mới lớn hơn giá trị hiện tại, thực hiện **atomic** ngay trong Redis — không cần khoá riêng, không race dù chạy bao nhiêu instance.

### Vì sao threshold Road Mask tính tương đối (30%) thay vì số cố định

611 camera có thể có số frame thành công rất khác nhau trong 4h (do lỗi mạng, cookie hết hạn, ảnh không hợp lệ...). Dùng ngưỡng tuyệt đối cố định (như `threshold=100` mặc định của `generateAndSaveRoadMask`) sẽ sai lệch giữa camera có 1400 frame và camera chỉ có 200 frame. Vì vậy: `threshold = round(frame_count_thật_của_camera_đó × 0.30)` — pixel nào có xe đè lên ở ≥ 30% số frame **thực tế thu được** của chính camera đó mới coi là mặt đường.

Camera có `frame_count < 50` (cấu hình ở `MIN_RELIABLE_FRAMES` trong `calibration_config.py`) sẽ được cảnh báo riêng trong log — road mask/`meuMax` của các camera này nên được xem lại thủ công (dữ liệu quá ít để tin cậy).

---

## Cách chạy

Yêu cầu: Redis đã chạy (`smart-transport` docker compose, xem README root), Python đã cài các dependency giống pipeline chính (`ultralytics opencv-python numpy pillow requests redis python-dotenv`).

**Cửa sổ 1 — `calibration_feeder.py`** (đẩy cam_id vào queue trong 4h, tự động gọi finalize khi xong):
```powershell
cd C:\Users\VIET\Desktop\stream-pipeline\AI\calibration
python calibration_feeder.py
```

**Cửa sổ 2, 3, 4 — `calibration_worker.py`** (2-3 instance song song, phải chạy **trước hoặc cùng lúc** với feeder để không bỏ lỡ cam_id trong queue):
```powershell
cd C:\Users\VIET\Desktop\stream-pipeline\AI\calibration
python calibration_worker.py
```

Sau khi feeder log `"Finalize xong..."`, `AI/config/camera_thresholds.json` đã được cập nhật `meuMax` mới và `{cam}_road_mask.npy` đã được ghi cho từng camera có dữ liệu. Muốn chạy lại finalize thủ công (ví dụ muốn thử `ROAD_THRESHOLD_RATIO` khác mà không chạy lại 4h) — chỉnh `calibration_config.py` rồi chạy trực tiếp:
```powershell
python calibration_finalize.py
```
(chỉ hợp lệ nếu Redis vẫn còn `calibration:meu_max`/`calibration:frame_count` từ lần chạy trước — 2 key này bị xoá sau khi finalize chạy xong 1 lần).

### Kiểm tra tiến độ khi đang chạy

| Muốn xem | Lệnh |
|---|---|
| Hàng đợi calibration còn tồn bao nhiêu | `docker exec smart-transport-redis redis-cli LLEN calibration_queue` |
| Camera nào đã có dữ liệu, đã xử lý bao nhiêu frame | `docker exec smart-transport-redis redis-cli HGETALL calibration:frame_count` |
| meuMax hiện tại của từng camera (đang chạy dở, chưa finalize) | `docker exec smart-transport-redis redis-cli ZRANGE calibration:meu_max 0 -1 WITHSCORES` |

---

## Cấu hình (`calibration_config.py`)

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `DURATION_SECONDS` | 4 giờ (14400s) | Tổng thời gian chạy, tính theo đồng hồ thật (không đếm frame) |
| `INTERVAL_SECONDS` | 10s | Chu kỳ đẩy queue |
| `ROAD_THRESHOLD_RATIO` | 0.30 | % số frame thật của camera phải có xe để tính là mặt đường |
| `MIN_RELIABLE_FRAMES` | 50 | Ngưỡng cảnh báo camera có quá ít frame, dữ liệu không đáng tin |
| `DRAIN_TIMEOUT_SECONDS` | 5 phút | Thời gian tối đa đợi `calibration_queue` rỗng sau khi hết 4h, trước khi ép finalize |
| `CAMERA_LIMIT` (env, tùy chọn) | — | Giới hạn số camera để test nhẹ, giống pipeline chính |

Tất cả file trong thư mục này **không import bất kỳ file nào ở root repo** (không `config.py`, không `worker.py`) — chỉ đọc trực tiếp `cameras_with_zones_merged.json` và dùng chung `AI/src/core/`, `AI/src/data/` (module tính toán thuần, không có state runtime chia sẻ với pipeline chính).
