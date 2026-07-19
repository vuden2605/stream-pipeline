# stream-pipeline — Smart Traffic Stream Pipeline

Hệ thống thu thập ảnh camera giao thông TP.HCM → phân tích mật độ bằng AI (YOLO + MEU + Greenshields) → publish tốc độ từng edge vào Redis cho hệ thống định tuyến Valhalla (`smart-transport/valhalla-hcm-traffic`).

**Kiến trúc hiện hành: Redis queue (BLPOP) — không Kafka, không S3.** Bản Kafka + Flink + S3 cũ vẫn còn trong repo (xem mục "Kiến trúc cũ") nhưng không còn được dùng.

Repo còn có **1 pipeline thứ hai, độc lập hoàn toàn**: phát hiện tai nạn giao thông + dashboard cho admin duyệt (xem mục "Phần 3 — Accident Detection Pipeline" bên dưới) — không chia sẻ code/Redis key nào với pipeline traffic ở Phần 1-2.

---

## Phần 1 — Cách chạy từ đầu

### 0. Yêu cầu

- Python 3.12 (kiểm tra `python -c "import sys; print(sys.executable)"` nếu máy có nhiều bản Python).
- Docker Desktop đang chạy.
- Repo `smart-transport` (cùng cấp thư mục với `stream-pipeline`) — cung cấp Redis, Valhalla. (Kafka/Postgres trong đó vẫn khởi động cùng nhưng pipeline hiện hành **không dùng**.)
- File `.env` ở root project (không cần AWS/Kafka nữa cho pipeline hiện hành, chỉ giữ lại nếu muốn chạy song song bản legacy):
  ```
  CONFIDENCE=0.3
  REDIS_HOST=localhost
  REDIS_PORT=6379
  ```

### 1. Cài dependency Python

```powershell
python -m pip install ultralytics opencv-python numpy pillow requests redis python-dotenv
```

### 2. Lên hạ tầng

```powershell
cd C:\Users\VIET\Desktop\smart-transport
docker compose up -d
```
Đợi `docker ps` thấy `smart-transport-redis` ở trạng thái `healthy`. Không cần build/chạy gì thêm bên `stream-pipeline` (không còn Flink/Kafka trong pipeline hiện hành).

### 3. Chạy pipeline — **1 `queue_feeder.py` + 2-3 `worker.py`, mỗi cái 1 cửa sổ PowerShell riêng**

**Cửa sổ 1 — `queue_feeder.py`** (đẩy `cam_id` vào Redis queue mỗi chu kỳ):
```powershell
cd C:\Users\VIET\Desktop\stream-pipeline
$env:CAMERA_LIMIT = "5"    # giới hạn số camera để test nhẹ; bỏ biến này = bật hết 611 camera
python queue_feeder.py
```

**Cửa sổ 2, 3, 4 — `worker.py`** (mỗi cửa sổ 1 instance, chạy song song 2-3 instance để đủ throughput cho 611 camera):
```powershell
cd C:\Users\VIET\Desktop\stream-pipeline
$env:CAMERA_LIMIT = "5"    # PHẢI giống queue_feeder.py
python worker.py
```

> Lưu ý PowerShell: biến môi trường set bằng `$env:TÊN = "giá_trị"`, không dùng cú pháp `VAR=value command` như bash.
>
> Nếu sửa `config.py`/`cameras_with_zones_merged.json` trong lúc đang chạy — phải Ctrl+C và chạy lại **tất cả** cửa sổ, Python không tự reload code.
>
> Số instance `worker.py` cần chạy phụ thuộc tốc độ máy: 1 instance không đủ throughput cho 611 camera (đã kiểm chứng — xem mục "Kết quả kiểm thử"), 3 instance đủ để giữ hàng đợi không phình to và phủ ~97% tổng số edge mỗi vòng.

### 4. Kiểm tra kết quả

| Muốn xem | Lệnh |
|---|---|
| `worker.py` tính occupancy/TWF/speed đúng không | log của chính nó — tìm dòng `occupancy=...% TWF=... speed=...km/h` |
| Hàng đợi camera còn tồn bao nhiêu | `docker exec smart-transport-redis redis-cli LLEN camera_queue` |
| Số edge đã có tốc độ trong Redis | `docker exec smart-transport-redis redis-cli HLEN traffic:speeds` |
| Toàn bộ dữ liệu tốc độ | `docker exec smart-transport-redis redis-cli HGETALL traffic:speeds` |
| `traffic_updater.py` có ghi được vào Valhalla không | `docker logs -f valhalla-traffic-daemon` — tìm `written=N stale=0` |

### 5. Dừng lại

```powershell
# Ctrl+C ở tất cả cửa sổ queue_feeder.py / worker.py

cd C:\Users\VIET\Desktop\smart-transport
docker compose down
```

---

## Phần 2 — Pipeline chi tiết & logic từng thành phần

### Sơ đồ tổng thể

```
config.CAMERAS (611 camera thật, 1609 edge_id thật từ Valhalla graph)
        │
        ▼
┌──────────────────────┐
│ 1. queue_feeder.py     │  mỗi 10s: RPUSH cam_id (611 lần) vào Redis LIST "camera_queue"
└──────────┬─────────────┘  (có backlog guard — bỏ qua chu kỳ nếu queue còn > 50% từ trước)
           │
           ▼
   Redis LIST "camera_queue"
           │
           ▼
┌──────────────────────────────────────────────┐
│ 2. worker.py (chạy 2-3 instance song song)      │
│    1. BLPOP cam_id                                │
│    2. GET snapshot trực tiếp (RAM, không qua S3)  │
│    3. YOLO predict (AI/ module) → MEU + occupancy │
│    4. TWF (occupancy gatekeeper + ramp, thay MEU/  │
│       meuMax thô) → density cho Greenshields       │
│    5. Greenshields: raw_speed = free_flow×(1-d^n) │
│    8. HSET + HEXPIRE traffic:speeds               │
└──────────┬─────────────────────────────────────┘
           │
           ▼
   Redis HASH "traffic:speeds" (mỗi field tự có TTL 1200s)
           │
           │  (worker.py KHÔNG chờ ai đọc — chỉ ghi rồi thôi)
           ▼
   /app/traffic_updater.py — daemon trong container valhalla-traffic-daemon
   tự thức dậy mỗi ~15s (đồng hồ riêng, KHÔNG phụ thuộc worker.py ghi
   bao nhiêu/khi nào) → HGETALL toàn bộ → ghi thẳng vào traffic.tar (mmap)
           │
           ▼
   Valhalla live traffic → routing
```

### 1. `queue_feeder.py` — nạp việc vào hàng đợi

- Đọc danh sách camera từ `config.CAMERAS` (nguồn: `cameras_with_zones_merged.json`).
- Mỗi `INTERVAL_SECONDS=10s`: kiểm tra độ dài `camera_queue` hiện tại.
  - Nếu còn tồn **> 50%** tổng số camera từ chu kỳ trước (`WORKER` chưa xử lý kịp) → **bỏ qua chu kỳ này**, không đẩy thêm — tránh hàng đợi phình to vô hạn (đây chính là vấn đề "consumer lag" từng gặp ở kiến trúc Kafka cũ, giờ được chặn chủ động ở phía nguồn).
  - Ngược lại → `RPUSH` toàn bộ `cam_id` (không kèm dữ liệu ảnh, chỉ là chuỗi ID) vào Redis LIST `camera_queue`.
- Không tải ảnh, không upload S3, không dùng Kafka — chỉ đẩy "phiếu công việc" (`cam_id`) để `worker.py` tự lấy và xử lý.

### 2. `worker.py` — lấy việc, phân tích AI, tính tốc độ, ghi Redis

Chạy **nhiều instance song song** (khuyến nghị 2-3, mỗi instance 1 process Python độc lập, tự quản lý session cookie + model YOLO riêng). Mỗi vòng lặp của 1 instance:

1. **`BLPOP camera_queue`** (timeout 5s) — chờ và lấy đúng 1 `cam_id`; nhiều instance cùng `BLPOP` trên 1 queue tự động chia việc, không cần điều phối thêm.
2. **`GET` snapshot trực tiếp** từ `{camera_url}&t={now_ms}` bằng `requests.Session` (giữ cookie phiên, header giả lập trình duyệt) — ảnh chỉ tồn tại dưới dạng `bytes` trong RAM, xử lý xong là mất, **không upload S3**.
3. **YOLO predict** (`AI/src/core/detector.py`, model `AI/weights/best.pt`, ngưỡng `CONFIDENCE=0.3`) → bounding box từng xe. Từ đó tính 2 chỉ số độc lập:
   - **`preciseOccupancyRatio`** (`AI/src/core/metrics.py:calculatePreciseOccupancyRatio`) — % diện tích **Road Mask** (mặt nạ mặt đường tích lũy từ heatmap lịch sử) thực sự bị xe che phủ trong frame hiện tại. Đo trực tiếp từ ảnh, **không phụ thuộc hiệu chuẩn MEU lịch sử**.
   - **MEU** (Motorcycle Equivalent Unit) = tổng hệ số quy đổi theo loại xe (`AI/config/meu_coefficients.json`: motorcycle=1.0, car=3.236, bus=8.568, truck=10).
4. **TWF (Traffic Weight Factor)** (`AI/src/core/metrics.py:calculateTrafficWeightFactor`) — thay hoàn toàn cho MEU/meuMax thô dùng làm density trực tiếp. Lý do: nếu `meuMax` được hiệu chuẩn từ dữ liệu lịch sử mà con đường đó chưa từng kẹt thật, `meuMax` sẽ thấp hơn sức chứa thật → báo kẹt giả dù traffic bình thường. TWF neo quyết định "có kẹt hay không" vào `preciseOccupancyRatio` (occupancy đo bằng ảnh) thay vì vào `meuMax`, nên `meuMax` bị đặt sai không còn gây báo kẹt giả:
   - `occupancy <= 70%` → `TWF = 0.0` (đường thông thoáng, bỏ qua phạt hoàn toàn).
   - `occupancy >= 90%` → `TWF = min(MEU / MEU_max[cam], 1.0)` — density đầy đủ, clamp về tối đa 1.0 (tránh `MEU_max` hiệu chuẩn thấp khiến tỉ số > 1.0).
   - `70% < occupancy < 90%` → `TWF = rampFactor × min(MEU / MEU_max[cam], 1.0)`, với `rampFactor = (occupancy - 70) / 20` — dải đệm nội suy tuyến tính, tránh `raw_speed` giật cục khi occupancy dao động nhẹ quanh ngưỡng giữa các frame.
   - `MEU_max` tra theo `cam_id` trong `AI/config/camera_thresholds.json` (`meuMax`), fallback `MEU_MAX_DEFAULT=200.0` nếu camera chưa hiệu chuẩn (610/611 camera hiện đang dùng fallback — xem "Giới hạn").
5. **Greenshields → `raw_speed`** — TWF ở bước 4 được cắm thẳng làm density (không phải hệ số nhân tốc độ):
   - `raw_speed = free_flow[edge] × (1 - TWF^n)` — `free_flow[edge]` tra theo `edge_id` trong `default_traffic.json` (`DEFAULT_EDGE_SPEED_KMH`), fallback `FREE_FLOW_DEFAULT_KMH=50` nếu edge không có; `n = GREENSHIELDS_N` (mặc định 1.0 = Greenshields tuyến tính gốc — **điều kiện bắt buộc** cho bước 5.5 bên dưới, xem ghi chú).
   - Chặn dưới: `raw_speed = max(raw_speed, MIN_SPEED_KMH=3.0)` — tránh tốc độ âm/bằng 0 khi TWF chạm 1.0.
   - `raw_speed` chỉ là giá trị tức thời của riêng chu kỳ này, **không** phải giá trị ghi Redis — xem bước 5.5.
5.5. **EMA làm mượt `raw_speed` → `speed_ema`** (thay cho "ghi thẳng raw_speed, không làm mượt" của thiết kế đơn giản hoá ban đầu):
   - Đọc `prev_speed_ema` từ chính cột `speed` cũ trong Redis (`HGET traffic:speeds {edge_id}` — không giữ state trong worker process vì nhiều instance dùng chung queue, edge có thể do worker khác xử lý ở chu kỳ trước). Field mới/hết TTL → không có prev.
   - `speed_ema = EMA_ALPHA × raw_speed + (1 − EMA_ALPHA) × prev_speed_ema`, `EMA_ALPHA = 0.3` (cửa sổ hiệu dụng ~5-6 mẫu ≈ 60-85s ở nhịp ghi ~10-15s/edge — lọc nhiễu 1-frame như occlusion/detect sai, nhưng vẫn phản ánh kẹt xe thật trong khoảng 1 phút). Cold-start (không có prev): `speed_ema = raw_speed`.
   - Đây là giá trị **thật sự ghi vào Redis** (cột `speed` trong `traffic:speeds`), không phải `raw_speed`.
7.5. **Traffic status (GREEN/YELLOW/RED)** — suy từ `speed_ema` (không dùng lại TWF thô), qua 2 bước:
   - `twf_ngam = 1 − speed_ema / free_flow[edge]` — tương đương TWF đã làm mượt. Phép suy này **chỉ đúng khi `GREENSHIELDS_N = 1.0`** (Greenshields tuyến tính): vì `raw_speed` là hàm tuyến tính của TWF khi `n=1`, và EMA là toán tử tuyến tính, nên "làm mượt speed rồi suy ngược TWF" cho kết quả giống hệt "làm mượt TWF trực tiếp". Nếu sau này đổi `GREENSHIELDS_N` khỏi 1.0, quan hệ này không còn tuyến tính nữa — cần xem lại cách tính `twf_ngam` và hiệu chỉnh lại ngưỡng bên dưới.
   - Áp hysteresis 2 ngưỡng (Schmitt trigger, tránh nhấp nháy status khi dao động quanh biên) lên `twf_ngam`:
     - `GREEN ↔ YELLOW`: vào YELLOW khi `twf_ngam ≥ 0.40`, về GREEN khi `twf_ngam < 0.30`.
     - `YELLOW ↔ RED`: vào RED khi `twf_ngam ≥ 0.75`, về YELLOW khi `twf_ngam < 0.65`.
   - Xem `classify_traffic_status()` / `parse_prev_value()` trong `worker.py`. Ngưỡng là điểm khởi đầu, cần hiệu chỉnh theo `road_class` sau khi có dữ liệu thực tế.
8. **Ghi Redis** (không gom batch, ghi trực tiếp ngay khi tính xong — khác hẳn kiến trúc Flink cũ):
   - `HSET traffic:speeds {edge_id} "{speed_ema}:{timestamp_giây}:{status}"` — cột đầu là `speed_ema` (**đã làm mượt**, không phải `raw_speed`), status nối thêm ở cuối (`GREEN`/`YELLOW`/`RED`). Giữ nguyên `speed:timestamp` làm tiền tố vì daemon bên Valhalla parse theo vị trí cột, chỉ dùng 2 cột đầu và bỏ qua cột thừa (xem mục 3.1) — nên đổi nội dung cột status (kể cả đổi tên nhãn FREE/SLOW/JAM → GREEN/YELLOW/RED) **không cần sửa/restart gì bên `valhalla-hcm-traffic`**.
   - `HEXPIRE` (Redis ≥ 7.4) đúng field vừa ghi, TTL = `REDIS_FIELD_TTL_SECONDS` (mặc định 1200s) — dọn field nếu camera chết hẳn, không cần cho tính đúng đắn (xem mục 3 bên dưới), chỉ để Redis không phình vô hạn. Khi field hết TTL, cả EMA lẫn hysteresis đều mất state, chu kỳ ghi lại tiếp theo coi như cold-start.

Camera nào không có `edges` (không map được với Valhalla graph) bị bỏ qua ngay từ đầu `process_camera()`.

### 3. Redis + daemon cập nhật Valhalla (bên `valhalla-hcm-traffic`, không thuộc repo này)

#### 3.1. Cấu trúc dữ liệu Redis

- `traffic:speeds` — **HASH**: field = `edge_id`, value = `"{speed_ema_kmh}:{timestamp_giây}:{status}"` (cột đầu là `speed_ema` đã làm mượt EMA, không phải `raw_speed` tức thời; status = `GREEN`/`YELLOW`/`RED`, xem mục 5.5/7.5 ở Phần 1). Daemon Valhalla chỉ đọc 2 cột đầu, cột status bị bỏ qua (xem 3.2) — đổi tên nhãn status không ảnh hưởng daemon.
- Có **TTL riêng từng field** (`HEXPIRE`, 1200s) — chỉ để dọn field khi camera chết hẳn, không phải cơ chế quyết định "còn tươi hay không" (xem 3.3).

#### 3.2. Có 2 bản daemon đọc Redis trong `valhalla-hcm-traffic` — chỉ 1 bản đang chạy

Khi tìm hiểu kỹ (đọc trực tiếp source bên trong container, vì 2 file này khác nhau và dễ nhầm), phát hiện project `valhalla-hcm-traffic` có **2 cách cập nhật live traffic hoàn toàn khác nhau**:

| | `scripts/traffic_updater.py` (container `valhalla-hcm`) | `/app/traffic_updater.py` (container `valhalla-traffic-daemon`) |
|---|---|---|
| Cách chạy | Subscribe Pub/Sub `traffic:events`, chạy ngay mỗi khi nhận message | **Tự poll theo đồng hồ cố định** (~15s), không quan tâm Pub/Sub |
| Cách ghi | Gọi subprocess `valhalla_traffic_demo_utils --seed-live-traffic-all` rồi `--update-live-traffic-from-csv` — dùng **tool chính thức của Valhalla**, tự tính offset đúng | Ghi **trực tiếp qua `mmap`** (tự tính offset bằng tay), incremental — chỉ đúng edge nào thay đổi |

**Vấn đề #1 (đã xử lý từ trước):** 2 daemon này từng chạy song song, cùng ghi vào 1 file `traffic.tar` → xung đột trực tiếp — mỗi lần `worker.py` publish, `scripts/traffic_updater.py` lại chạy `seed-live-traffic-all` (reset TOÀN BỘ edge về UNKNOWN) ngay trong lúc `/app` daemon đang ghi incremental. Đã tắt `scripts/traffic_updater.py` (gỡ khỏi `command:` trong `docker-compose.yml`), chỉ giữ `/app/traffic_updater.py` chạy.

**Vấn đề #2 (phát hiện sau, nghiêm trọng hơn):** ngay cả khi chỉ còn `/app/traffic_updater.py` chạy một mình, `/locate` của Valhalla **không bao giờ** hiển thị `live_speed` cho bất kỳ edge nào, dù byte đọc thẳng từ `traffic.tar` (bằng đúng công thức offset của chính daemon) luôn khớp với Redis. Trace vào source thật của Valhalla (`valhalla/baldr/traffictile.h`, đúng commit `9c06fece...` mà Dockerfile build) phát hiện `/app/traffic_updater.py` **tự đoán sai 2 hằng số**:

```cpp
// Struct thật (valhalla/baldr/traffictile.h):
struct TrafficTileHeader {
  uint64_t tile_id; uint64_t last_update;
  uint32_t directed_edge_count; uint32_t traffic_tile_version;
  uint32_t spare2; uint32_t spare3;
};
static_assert(sizeof(TrafficTileHeader) == sizeof(uint64_t) * 4, ...);  // 32 byte

struct TrafficSpeed {
  uint64_t overall_encoded_speed : 7;  // bit 0-6
  uint64_t encoded_speed1 : 7;          // bit 7-13
  uint64_t encoded_speed2 : 7;          // bit 14-20
  uint64_t encoded_speed3 : 7;          // bit 21-27
  uint64_t breakpoint1 : 8;             // bit 28-35
  uint64_t breakpoint2 : 8;             // bit 36-43
  ...
};
```

`/app/traffic_updater.py` bản gốc dùng `TILE_HDR_SIZE = 40` (đáng lẽ **32**) và đặt `breakpoint1` ngay sau `encoded_speed1` (đáng lẽ phải sau `encoded_speed2`/`encoded_speed3`). Lệch header đúng 8 byte = đúng 1 slot `TrafficSpeed` → **mọi write đều ghi lệch sang slot của edge kế tiếp** (`local_idx + 1`), còn slot thật của edge cần ghi thì không bao giờ được set `breakpoint1` → `speed_valid()` phía Valhalla luôn `false` → `/locate` luôn trả `{}`, bất kể dữ liệu Redis đúng hay daemon "tưởng" mình ghi thành công.

**Đã sửa cả 2 hằng số trong `/app/traffic_updater.py`** (`TILE_HDR_FMT`: `"<QQIIQQ"` → `"<QQIIII"`, và thứ tự bit trong `_speed_u64()`). **Đã kiểm chứng cả ghi lẫn xoá đều đúng qua `/locate` thật** (không chỉ đọc byte bằng tay):

- Ghi: `/locate` trả `{"overall_speed": 70, "speed_0": 70, "speed_1": 70, "speed_2": 70, "breakpoint_0": 1.0, ...}` — khớp 100% với Redis.
- Xoá (test bằng `HDEL traffic:speeds <edge_id>`): log daemon hiện đúng `cleared=1` ngay chu kỳ edge biến mất, và `/locate` cho edge đó quay lại `{}` (UNKNOWN) ngay sau đó.

Nhờ vậy `/app/traffic_updater.py` (đã sửa) tiếp tục là nguồn cập nhật live traffic **duy nhất** — không cần quay lại `scripts/traffic_updater.py` (giữ tắt, chỉ để tham khảo) và không có khoảng "toàn bộ UNKNOWN" nào cả vì vẫn ghi incremental đúng như thiết kế gốc.

**Cập nhật — đã hết orphan, file giờ sống trên host:** container `valhalla-traffic-daemon` từng là orphan (patch offset chỉ tồn tại trong writable layer qua `docker cp`, mất khi `docker rm`). Đã thêm service `traffic-daemon` chính thức vào `docker-compose.yml` của `valhalla-hcm-traffic`, bind-mount `./app_daemon_backup:/app` — sửa file trên host (`smart-transport/valhalla-hcm-traffic/app_daemon_backup/traffic_updater.py`) có hiệu lực ngay trên đĩa trong container, **không cần `docker cp` nữa**; chỉ cần `docker restart valhalla-traffic-daemon` để process Python nạp lại code (Python không tự hot-reload file đã đổi). Bản vá parser cho phép value 3 cột (`speed:ts:status`) đã áp dụng theo cách này, xem mục 3.1.

**Hệ quả:** vì daemon đang dùng hoàn toàn phớt lờ Pub/Sub (tự poll cố định) và không còn consumer nào khác lắng nghe, lời gọi `PUBLISH traffic:events` trong `worker.py` đã được **xoá khỏi code** (cùng với `REDIS_CHANNEL`, `PUBLISH_DEBOUNCE_SECONDS`, khoá `traffic:publish_lock`) — nếu sau này cần một consumer event-driven, thêm lại kênh Pub/Sub lúc đó.

#### 3.3. Cách daemon đang dùng quyết định "edge nào còn tươi"

Mỗi ~15s, `/app/traffic_updater.py`:
1. `HGETALL traffic:speeds` — đọc **toàn bộ** hash (không phải chỉ phần mới đổi).
2. Với mỗi edge: tính `age = now - timestamp_nhúng_trong_value`. Nếu `age > speed_ttl_seconds` (900s, cấu hình riêng của daemon này, **độc lập với TTL Redis 1200s** ở mục 3.1) → coi stale, loại khỏi danh sách "đang active".
3. Edge nào **trước đó** daemon từng ghi tốc độ thật, mà giờ rớt khỏi danh sách active (do stale hoặc do field đã bị Redis tự xoá) → daemon **tự ghi UNKNOWN** cho đúng edge đó vào `traffic.tar`.
4. Edge còn tươi (age ≤ 900s) → ghi thẳng tốc độ mới vào `traffic.tar`.

→ TTL Redis (1200s) và ngưỡng stale của daemon (900s) là **2 cơ chế độc lập, không xung đột** vì 1200 > 900 — daemon luôn tự coi edge stale (bước 2) **trước khi** Redis kịp xoá hẳn field đó (bước dọn bộ nhớ), nên không có tình huống field biến mất bất ngờ trước khi daemon xử lý đúng.

**Vị trí code chính xác** — hàm `process_snapshot()` trong `/app/traffic_updater.py` (container `valhalla-traffic-daemon`, xem qua `docker exec valhalla-traffic-daemon cat /app/traffic_updater.py`, không có file thường trên host):

```python
for eid_str, val_str in snapshot.items():
    ...
    age = now - ts
    if age > ttl:                 # ttl = cfg["speed_ttl_seconds"] = 900
        n_stale += 1
        continue                  # ← loại khỏi new_active của chu kỳ này
    ...
    new_active[eid] = kph

# Clear edges that dropped out of the snapshot (not stale — just absent)
for eid in set(active_edges) - set(new_active):   # "trước active, giờ không còn"
    if write_speed(mm, index, eid, UNKNOWN_BYTES): # ghi UNKNOWN thẳng vào traffic.tar
        n_cleared += 1
```

#### 3.4. Đã test thực tế — xác nhận cơ chế ở 3.3 hoạt động đúng

Cách test (thực hiện trực tiếp trên môi trường đang chạy, không chỉ đọc code):

1. Tạm dừng toàn bộ `worker.py` (tránh bị ghi đè lại trong lúc test).
2. Chọn 1 `edge_id` đang có trong `traffic:speeds`, cố tình sửa timestamp lùi về quá khứ hơn 900s:
   ```powershell
   docker exec smart-transport-redis redis-cli HSET traffic:speeds <edge_id> "<speed>:<now_epoch_minus_1000>"
   ```
3. Đợi 1-2 chu kỳ poll của daemon (~30s), theo dõi `docker logs -f valhalla-traffic-daemon` — thấy `stale=1` (hoặc hơn) xác nhận daemon đã phát hiện đúng.
4. Xác nhận **thật sự đã ghi UNKNOWN vào `traffic.tar`** (đáng tin hơn đếm `cleared` trong log, vì log có thể lệch nếu quan sát không đúng thời điểm poll) — đọc thẳng byte đã mã hoá:
   ```python
   # chạy trong container valhalla-traffic-daemon (docker exec ... python3 -c "...")
   import struct, mmap
   with open('/valhalla_tiles/traffic.tar', 'rb') as f:
       mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
       # data_off, TILE_HDR_SIZE=40, SPEED_SIZE=8 lấy từ index cache
       # (/tmp/traffic_tar_index_cache.json) đúng tile của edge_id đó
       off = data_off + 40 + (edge_id >> 25) * 8
       raw = struct.unpack('<Q', mm[off:off+8])[0]
       print(raw & 0x7F)   # 127 = UNKNOWN, khác 127 = round(kph/2)
   ```
   Kết quả thực tế khi test: `127` (UNKNOWN) — đúng như kỳ vọng.

**Cập nhật (đã tự hết):** tại thời điểm test ban đầu, `traffic.tar` chỉ có đúng 1 tile trong index cache, khiến ~67% edge bị `oob`. Sau khi sửa xong bug offset ở mục 3.2 và `traffic.tar` được ghi/rebuild thêm qua thời gian, index hiện đã lên **31 tile**, `oob=0/1581` (0%) — vấn đề này đã tự giải quyết, không cần hành động thêm.

#### 3.4b. Test riêng nhánh "field bị Redis tự xoá" (khác nhánh "stale" ở 3.4)

3.4 test nhánh **stale** (field vẫn còn trong Redis nhưng timestamp quá hạn). Còn nhánh **"field đã bị Redis tự xoá"** (do TTL `HEXPIRE` hết hạn, hoặc camera chết hẳn không còn `worker.py` nào ghi) test bằng cách xoá field trực tiếp thay vì backdate timestamp — cả 2 nhánh đều dẫn tới cùng kết quả ở `process_snapshot()`: edge rớt khỏi `new_active` của chu kỳ đó → daemon ghi UNKNOWN (xem code trích ở 3.3).

Cách test (đã thực hiện thật, không chỉ suy luận):

1. **Dừng hết `queue_feeder.py` và mọi `worker.py`** (Ctrl+C tất cả cửa sổ, hoặc `Stop-Process` theo PID lấy từ `Get-CimInstance Win32_Process -Filter "Name='python.exe'"`) — bắt buộc, nếu không edge sẽ bị ghi lại gần như ngay lập tức (chu kỳ `queue_feeder` chỉ 10s) trước khi daemon kịp phát hiện.
2. Chọn 1 `edge_id` đang có trong `traffic:speeds` **và nằm trong tile daemon đang index** (không phải `oob`) — kiểm tra bằng cách so `edge_id & 0x1FFFFFF` với các key trong `/tmp/traffic_tar_index_cache.json` (container `valhalla-traffic-daemon`). Ví dụ đã dùng: `3380307324401` (tile `290289`, giá trị trước khi xoá: `47.3 km/h`, `raw_speed_code=24`).
3. Xoá thẳng field:
   ```powershell
   docker exec smart-transport-redis redis-cli HDEL traffic:speeds <edge_id>
   ```
4. Đợi 1-2 chu kỳ poll (~30s), rồi đọc thẳng byte đã mã hoá trong `traffic.tar` (theo đúng script ở bước 4 của 3.4) để xác nhận `raw_speed_code == 127`.

**Kết quả thực tế:** `raw_speed_code` chuyển từ `24` → `127` (UNKNOWN) — đúng cơ chế. **Log daemon lại show `cleared=0`** ở toàn bộ chu kỳ quan sát được, dù byte đã đổi đúng — tái xác nhận lưu ý ở 3.4: chu kỳ poll thực sự thực hiện việc "clear" nhiều khả năng xảy ra ngay trước khi bắt đầu tail log (độ trễ vài giây giữa lệnh `HDEL` và lúc chạy `docker logs -f`), nên **đọc byte trực tiếp vẫn là cách kiểm chứng đáng tin duy nhất**, không nên dựa vào đếm `cleared` trong log.

#### 3.5. Rủi ro còn lại — edge "kẹt vĩnh viễn" sau khi daemon restart, và giải pháp seed định kỳ

Cơ chế ở 3.3 (bước 3) chỉ hoạt động đúng vì daemon **tự nhớ trong RAM** danh sách "edge nào đang active" (biến `active_edges`). Danh sách này **không được lưu lại** — nếu daemon bị restart (crash, `docker restart`, deploy lại), biến này reset về rỗng.

Nếu ngay trước lúc restart có edge đang mang tốc độ thật (chưa kịp bị coi stale), và ngay sau restart camera đó ngừng gửi dữ liệu luôn → daemon mới khởi động **không hề biết** edge đó "từng active" để so sánh/xoá — tốc độ cũ đó **kẹt vĩnh viễn** trong `traffic.tar` cho tới khi chính edge_id đó tình cờ có dữ liệu mới ghi đè (có thể không bao giờ xảy ra nếu camera đã chết hẳn).

### 4. Kiến trúc cũ — Kafka + Flink + S3 (gom vào `old-pipeline/`, không còn dùng)

Các file sau đã chuyển vào thư mục `old-pipeline/` (dùng `git mv`, giữ lịch sử) — không nằm trong pipeline đang chạy, giữ lại để tham khảo:

- `old-pipeline/ingestion.py` — fetch ảnh + upload S3 + publish Kafka `raw_images`.
- `old-pipeline/consumer_ai.py` — consume Kafka, tính TWF/HCI (không phải Greenshields), publish Kafka `traffic_events`.
- `old-pipeline/vehicle_count_agg_job.py` — Flink job: dedup + EWMA + gom batch + publish Redis (xoá sạch `traffic:speeds` rồi ghi lại theo batch, khác hẳn cơ chế ghi trực tiếp per-edge của `worker.py` hiện tại).
- `old-pipeline/Dockerfile`, `old-pipeline/docker-compose.yml` — build/chạy Flink jobmanager/taskmanager. `Dockerfile` có `COPY jobs/ /opt/flink/jobs/` và `COPY cameras_with_zones_merged.json ...` — đường dẫn này khớp bối cảnh build cũ (chạy từ root repo), **không còn đúng** nếu build trực tiếp từ trong `old-pipeline/` (context khác, `cameras_with_zones_merged.json` vẫn nằm ở root repo) — cần chỉnh lại path nếu có ý định build/chạy lại nguyên trạng.
- `old-pipeline/consumer_db.py` — vẫn broken như trước (import biến không tồn tại), không liên quan tới thay đổi lần này.

**Lưu ý:** `jobs/vehicle_count_agg_job.py` từng bị xoá khỏi git ở 1 commit trước đó (`427146e "kafka pipeline"`), nhưng file vẫn còn tồn tại thật trên đĩa (không rõ cơ chế — có thể do editor/backup nào đó giữ lại, phát hiện lúc dọn sang `old-pipeline/`) với nội dung gần như giống hệt bản trước khi xoá (chỉ khác `BATCH_WINDOW_MS=800_000` thay vì `300_000`) — đã giữ lại và thêm vào git cùng đợt gom file này.

**Lý do chuyển hẳn sang Redis queue:** loại bỏ được toàn bộ chi phí S3 (không cần upload/download ảnh), loại bỏ được Kafka/Flink (hạ tầng nặng, khó vận hành hơn so với quy mô thực tế cần), và quan trọng nhất — kiến trúc cũ có lỗi nghiêm trọng do nghẽn throughput (xem "Kết quả kiểm thử" bên dưới), kiến trúc mới đã kiểm chứng khắc phục được.

### 5. `old-pipeline/consumer_db.py` — vẫn chưa hoạt động được

Không đổi so với trước — import biến không tồn tại (`TOPIC_COUNTS`), nằm ngoài phạm vi các lần sửa vừa qua.

---

## Phần 3 — Accident Detection Pipeline (tách biệt hoàn toàn khỏi Phần 1-2)

**Mục đích:** phát hiện tai nạn giao thông qua camera → tạo sự kiện chờ **admin duyệt qua dashboard web** → KHÔNG tự động hành động gì (model còn train trên tập ảnh mẫu rất nhỏ, chưa đủ tin cậy để tự động hoá — xem "Giới hạn").

**Nguyên tắc thiết kế bắt buộc:** không sửa bất kỳ file nào của pipeline traffic (`worker.py`, `queue_feeder.py`, `config.py`, `AI/src/core/detector.py`, `AI/src/core/metrics.py`), không dùng chung Redis key/queue với pipeline đó. Đánh đổi: `accident_worker.py` tự fetch snapshot camera theo chu kỳ riêng thay vì dùng chung `camera_queue` → **tải lên nguồn ảnh camera (`giaothong.hochiminhcity.gov.vn`) tăng gấp đôi** so với chỉ chạy Phần 1-2 (worker traffic và worker accident cùng fetch độc lập cùng 1 tập camera).

### Sơ đồ tổng thể

```
config.CAMERAS (đọc CHUNG với Phần 1-2, chỉ IMPORT — không sửa config.py)
        │
        ▼
┌────────────────────────────────────────────────────┐
│ accident_worker.py — vòng lặp riêng, độc lập worker.py │
│   for camera in CAMERAS (mỗi ACCIDENT_INTERVAL_SECONDS=10s):│
│    1. GET snapshot trực tiếp (tự fetch, không qua camera_queue)│
│    2. YOLO predict (AI/accident-detection/weights/best.pt)   │
│       → box class: human_incident/human_normal/               │
│         vehicle_incident/vehicle_normal                       │
│    3. Lọc box *_incident đạt ACCIDENT_CONFIDENCE (0.4)         │
│    4. Streak counter/camera (Redis) — cần đủ                  │
│       ACCIDENT_STREAK_THRESHOLD=3 chu kỳ LIÊN TIẾP mới        │
│       tính là nghi vấn thật (chống báo giả 1 frame nhiễu)     │
│    5. Nếu đủ streak + qua cooldown (300s/camera, chống spam    │
│       nhiều sự kiện trùng 1 vụ) → tạo event_id, ghi Redis,     │
│       rồi gửi email báo admin (nếu đã cấu hình SMTP_*)         │
└──────────────────────┬───────────────────────────────────────┘
                        ▼
     Redis (key riêng, không đụng traffic:speeds/camera_queue):
       accident:pending          ZSET  member=event_id score=ts
       accident:image:<event_id> STRING JPEG bytes thô, TTL 48h
       accident:meta:<event_id>  HASH  cam_id/ts/class_name/
                                       confidence/status/decided_at
                        │
                        ├──────────────────────────────┐
                        ▼                               ▼
┌────────────────────────────────────────────────────┐  Email tới ADMIN_EMAILS
│ accident_api.py (FastAPI, container/image RIÊNG —    │  (SMTP, optional — bỏ qua
│ nhẹ, không cần torch/ultralytics)                     │  nếu chưa cấu hình SMTP_HOST)
│   GET  /api/accidents?status=PENDING|APPROVED|         │  subject: "[Cảnh báo tai nạn]
│        REJECTED|ALL                                   │  Camera <cam_id>", body có link
│   GET  /api/accidents/{id}/image                       │  DASHBOARD_BASE_URL/?event=<id>
│   POST /api/accidents/{id}/approve                      │            │
│   POST /api/accidents/{id}/reject                        │            ▼
│   GET  /  → phục vụ admin_dashboard/index.html (SPA)      │  Admin click link trong mail
└──────────────────────┬───────────────────────────────────┘            │
                        ▼                                                │
            Admin mở http://localhost:8080 (trực tiếp hoặc từ ─────────┘
            link email — dashboard tự nhận ?event=<id>, chuyển
            sang tab "Tất cả", cuộn + khoanh nổi đúng card đó),
            xem ảnh + bấm Duyệt/Từ chối → HSET accident:meta:<id> status=...
```

### 1. Các file liên quan

| File | Vai trò |
|---|---|
| `AI/src/core/accident_detector.py` | Class `AccidentDetector` bọc YOLO (`AI/accident-detection/weights/best.pt`), `extractIncidentBoxes()` lọc box `*_incident` |
| `accident_worker.py` | Vòng lặp fetch + detect + streak/cooldown + ghi sự kiện vào Redis |
| `accident_api.py` | FastAPI — CRUD sự kiện qua Redis, phục vụ luôn `admin_dashboard/` |
| `admin_dashboard/index.html` | SPA thuần HTML/JS (không framework), poll API mỗi 5s, có tab Chờ duyệt/Đã duyệt/Đã từ chối/Tất cả |
| `Dockerfile.api` | Image riêng cho `accident_api.py` (chỉ `fastapi`, `uvicorn`, `redis` — không cài torch/ultralytics như `Dockerfile` chính) |
| `AI/accident-detection/` | Model + ảnh mẫu, clone từ `github.com/caogiabao0909/accident-detection` (đã gỡ `.git` lồng bên trong để track trực tiếp trong repo này) |

### 2. Biến môi trường (tất cả đều optional, có default)

| Biến | Default | Ý nghĩa |
|---|---|---|
| `ACCIDENT_MODEL_PATH` | `AI/accident-detection/weights/best.pt` | Đường dẫn model YOLO tai nạn |
| `ACCIDENT_CONFIDENCE` | `0.4` | Ngưỡng confidence tối thiểu cho box `*_incident` |
| `ACCIDENT_INTERVAL_SECONDS` | `10` | Chu kỳ quét lại toàn bộ camera |
| `ACCIDENT_STREAK_THRESHOLD` | `3` | Số chu kỳ liên tiếp phải thấy incident trên cùng 1 camera trước khi tạo sự kiện |
| `ACCIDENT_COOLDOWN_SECONDS` | `300` | Sau khi tạo 1 sự kiện cho 1 camera, tạm ngưng tạo thêm sự kiện mới cho camera đó |
| `ACCIDENT_IMAGE_TTL_SECONDS` | `172800` (48h) | TTL của ảnh JPEG lưu trong Redis — hết hạn thì nút xem ảnh trả 404 dù meta còn |
| `ACCIDENT_META_TTL_SECONDS` | `604800` (7 ngày) | TTL của metadata sự kiện (camera, class, confidence, status) |
| `REDIS_ACCIDENT_PENDING_KEY` | `accident:pending` | Tên Redis ZSET index toàn bộ event_id (dùng chung cho mọi trạng thái, lọc theo `status` trong meta hash khi query) |
| `SMTP_HOST` | *(rỗng)* | Host SMTP để gửi email báo admin. **Rỗng = tắt tính năng gửi email hoàn toàn** (chỉ log, không lỗi) |
| `SMTP_PORT` | `587` | Port SMTP (STARTTLS) |
| `SMTP_USER` / `SMTP_PASSWORD` | *(rỗng)* | Tài khoản đăng nhập SMTP (nếu server yêu cầu auth) |
| `SMTP_FROM` | = `SMTP_USER` | Địa chỉ người gửi |
| `ADMIN_EMAILS` | *(rỗng)* | Danh sách email nhận báo, phân cách bởi dấu phẩy. Rỗng = không gửi (dù đã có `SMTP_HOST`) |
| `DASHBOARD_BASE_URL` | `http://localhost:8080` | Base URL chèn vào link trong email (`{DASHBOARD_BASE_URL}/?event=<id>`) — **cần đổi thành URL thật admin truy cập được** nếu deploy (xem Phần 4), không phải `localhost` |

`REDIS_HOST`/`REDIS_PORT`/`REDIS_DB` dùng đúng 3 biến đã có sẵn trong `.env.prod` (đọc lại, không định nghĩa biến mới) — `accident_worker.py` import từ `config.py`, `accident_api.py` tự đọc `os.getenv` (không import `config.py` để tránh nạp `cameras_with_zones_merged.json`/`default_traffic.json` không cần thiết vào image nhẹ).

**Email báo admin:** `accident_worker.py` tự gửi qua `smtplib` (thư viện chuẩn Python, không cần cài thêm package) ngay sau khi tạo sự kiện — không cần cấu hình gì thêm ngoài các biến `SMTP_*`/`ADMIN_EMAILS` ở trên trong `.env.prod`. Vì việc tạo sự kiện đã tự giới hạn qua streak+cooldown (xem sơ đồ), mỗi sự kiện chỉ gửi đúng 1 email — không cần chống spam thêm. Nếu gửi lỗi (sai SMTP, mất mạng...) chỉ log lỗi, không làm crash worker hay chặn việc ghi sự kiện vào Redis.

### 3. Cách chạy

```powershell
docker compose up --build accident-worker accident-api
```

Mở `http://localhost:8080` để xem dashboard. Chạy độc lập với `queue-feeder`/`worker` (Phần 1-2) — bật/tắt riêng không ảnh hưởng nhau.

### 4. Giới hạn / lưu ý (đã biết, chưa xử lý)

| # | Vấn đề | Ảnh hưởng |
|---|---|---|
| 1 | Model `AI/accident-detection/weights/best.pt` chỉ có ảnh mẫu để test (23 ảnh `image_of_accident/`), không rõ quy mô/chất lượng tập train thật | Dễ **false positive** trên cảnh đông người/xe dừng đèn đỏ bình thường (đã quan sát thực tế) — đây là lý do bắt buộc phải qua admin duyệt, không tự động hoá bất kỳ hành động nào từ kết quả detect |
| 2 | Dashboard (`accident_api.py`) **không có auth** | Ai truy cập được cổng 8080 cũng duyệt/từ chối được — cần thêm ít nhất basic auth trước khi expose ra ngoài mạng nội bộ/internet |
| 3 | Ảnh sự kiện lưu trong Redis (không phải file/S3), có TTL 48h | Sự kiện cũ hơn 48h vẫn hiện trong danh sách (meta TTL 7 ngày) nhưng bấm xem ảnh sẽ lỗi 404 |
| 4 | `accident_worker.py` tự fetch camera riêng, không qua `queue_feeder.py` | Tải lên `giaothong.hochiminhcity.gov.vn` tăng gấp đôi so với chỉ chạy Phần 1-2 (xem "Nguyên tắc thiết kế" ở đầu Phần 3) |
| 5 | Streak/cooldown/threshold hiện là giá trị mặc định đoán, chưa hiệu chỉnh bằng dữ liệu thực tế | Cần tinh chỉnh `ACCIDENT_STREAK_THRESHOLD`/`ACCIDENT_CONFIDENCE`/`ACCIDENT_COOLDOWN_SECONDS` sau khi chạy thử dài hạn với camera thật |
| 6 | Gửi email **tắt mặc định** (`SMTP_HOST`/`ADMIN_EMAILS` rỗng) | Nếu không tự thêm biến `SMTP_*`/`ADMIN_EMAILS` vào `.env.prod`, sự kiện vẫn tạo/ghi Redis bình thường nhưng admin sẽ không nhận được email — phải tự vào dashboard kiểm tra |
| 7 | `DASHBOARD_BASE_URL` mặc định `http://localhost:8080` | Nếu không đổi giá trị này khi deploy (Phần 4), link trong email sẽ trỏ về `localhost` của máy chạy `accident_worker.py` — vô dụng với admin ở máy khác |

---

## Phần 4 — Chạy toàn bộ stream (traffic + accident) qua Docker Compose

`docker-compose.yml` ở root repo có **4 service độc lập** (2 image khác nhau — `smart-transport-ai` cho 3 service AI nặng, `smart-transport-accident-api` nhẹ riêng cho dashboard):

| Service | Image | Việc làm |
|---|---|---|
| `queue-feeder` | `smart-transport-ai` | Đẩy `cam_id` vào `camera_queue` mỗi 10s (Phần 1-2) |
| `worker` | `smart-transport-ai` | Nhận diện mật độ giao thông, ghi `traffic:speeds` (Phần 1-2) |
| `accident-worker` | `smart-transport-ai` | Nhận diện tai nạn, ghi `accident:*` (Phần 3) |
| `accident-api` | `smart-transport-accident-api` | API + dashboard duyệt tai nạn, cổng `8080` (Phần 3) |

Cả 4 service đều đọc `.env.prod` (cùng `REDIS_HOST`/`REDIS_PORT`/`REDIS_DB`) — không cần chạy `smart-transport` cùng máy, khác với cách chạy thủ công ở Phần 1 (vốn giả định Redis local qua `smart-transport`); ở đây Redis là instance đã cấu hình sẵn trong `.env.prod`.

### Chạy tất cả cùng lúc

```powershell
docker compose up --build -d
```

- `-d` chạy nền; bỏ đi nếu muốn xem log trực tiếp trên terminal.
- Xem log riêng từng service: `docker compose logs -f worker` (hoặc `queue-feeder`/`accident-worker`/`accident-api`).
- Xem trạng thái: `docker compose ps`.
- Dashboard duyệt tai nạn: `http://localhost:8080`.

### Chỉ chạy 1 phần

Compose chỉ build/khởi động đúng service được liệt kê, không đụng service còn lại:

```powershell
docker compose up --build queue-feeder worker        # chỉ traffic pipeline
docker compose up --build accident-worker accident-api  # chỉ accident pipeline (Phần 3)
```

### Dừng lại

```powershell
docker compose down            # dừng + xoá container (image vẫn giữ lại)
```

---

## Kết quả kiểm thử (chạy full 611 camera, 2026-07-06)

So sánh trực tiếp với kiến trúc Kafka+Flink+S3 cũ (đã kiểm thử ngay trước khi chuyển sang Redis queue):

| Chỉ số | Kafka + Flink + S3 (cũ) | Redis queue (mới) |
|---|---|---|
| Số edge có tốc độ trong Redis | Dao động thất thường: 553 → 1152 → 603 (tối đa 72%, không ổn định) | Tăng đều rồi ổn định ở **1567/1609 (97.4%)** |
| Độ trễ dữ liệu (freshness) | ~12 giờ (backlog Kafka `raw_images` tồn ~149 000 message, tăng liên tục) | **~6 giây** |
| `traffic_updater.py` ghi vào Valhalla | `written=0` mọi lần — **100% dữ liệu bị coi stale**, Valhalla không nhận traffic nào | `written=1513, stale=0` — ghi thành công liên tục |
| Lỗi trong log worker/consumer | 1 lỗi warm-up (tự phục hồi), còn lại sạch | **0 lỗi** trên cả 3 instance sau nhiều phút chạy |

**Nguyên nhân kiến trúc cũ thất bại:** `consumer_ai.py` (1 process, `CONCURRENCY=4`) không xử lý kịp lượng ảnh của 611 camera → backlog Kafka phình to → dữ liệu tới Redis luôn mang timestamp cũ hàng giờ → bị `traffic_updater.py` tự động loại bỏ vì vượt `SPEED_TTL_SECONDS=900`. Kiến trúc Redis queue giải quyết bằng cách **chạy nhiều `worker.py` song song** (throughput scale ngang theo số instance, không bị giới hạn bởi 1 process duy nhất) và **bỏ qua bước upload/download S3** (giảm độ trễ mỗi camera).

**Cập nhật:** cảnh báo `N edge(s) not found in traffic.tar index` (oob) ghi nhận ban đầu (~54/1567, ~3.4%, sau có lúc tăng lên ~67% do `traffic.tar` bị thu hẹp tạm thời) đã **hết hẳn** sau khi sửa bug offset của `/app/traffic_updater.py` và index được rebuild đầy đủ (xem Phần 2, mục 3.2) — hiện `oob=0/1581`.

---

## Giới hạn hiện tại (đã biết, chưa xử lý)

| # | Vấn đề | Ảnh hưởng |
|---|---|---|
| 1 | `MEU_max` chỉ hiệu chuẩn phẳng theo camera, không tách theo giờ trong ngày như thiết kế gốc muốn | 610/611 camera dùng `MEU_MAX_DEFAULT=200.0`. Tác động đã giảm nhẹ so với trước: TWF chỉ dùng `MEU/MEU_max` khi `occupancy > 70%` (gatekeeper neo vào ảnh thực tế), nên `MEU_max` sai lệch không còn tự gây báo kẹt giả khi đường thực sự thông thoáng — nhưng vẫn ảnh hưởng đến **độ chính xác của TWF khi đã kẹt thật** (occupancy > 70%) |
| 2 | `camera_thresholds.json` chỉ có hiệu chuẩn cho `camera_001` | Tương tự #1 — cần thu thập dữ liệu lịch sử để hiệu chuẩn thêm |
| 3 | `old-pipeline/consumer_db.py` | Không chạy được — xem mục 5 ở Phần 2 |
| 4 | Số instance `worker.py` cần chạy tay theo cấu hình máy, chưa có auto-scaling | Vận hành thủ công phải tự theo dõi độ dài `camera_queue` để quyết định thêm/bớt instance |
| 5 | Daemon Valhalla (`/app/traffic_updater.py`) nhớ "edge nào đang active" trong RAM, mất hết khi daemon restart | Edge có tốc độ thật ngay trước lúc restart, rồi camera chết ngay sau đó → tốc độ cũ **kẹt vĩnh viễn** trong `traffic.tar`, không tự xoá được nữa (xem Phần 2, mục 3.5) — thuộc hạ tầng `valhalla-hcm-traffic`, ngoài phạm vi repo này |

## Lưu ý vận hành

- **Console Windows** mặc định dùng codepage không hỗ trợ Unicode — `queue_feeder.py`/`worker.py` đã tự reconfigure `stdout`/`stderr` sang UTF-8 khi phát hiện encoding khác UTF-8.
- Sửa `config.py`/`cameras_with_zones_merged.json`/`default_traffic.json` trong lúc `queue_feeder.py`/`worker.py` đang chạy — phải Ctrl+C và chạy lại **tất cả**, Python không tự reload code.
- Kiến trúc Kafka+Flink+S3 cũ vẫn cần rebuild/resubmit thủ công như trước nếu có ai chủ động quay lại dùng (xem mục "Kiến trúc cũ").
- Đã tắt daemon Pub/Sub cũ (`python3 /scripts/traffic_updater.py`) trong container `valhalla-hcm` — sửa trực tiếp `smart-transport/valhalla-hcm-traffic/docker-compose.yml`, gỡ dòng chạy script đó khỏi `command:` (chi tiết + lý do ở Phần 2, mục 3.2). Nếu ai đó `git pull`/rebuild lại project `valhalla-hcm-traffic` từ nơi khác mà không có thay đổi này, xung đột 2 daemon có thể quay lại.
