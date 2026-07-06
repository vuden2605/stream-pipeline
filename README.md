# stream-pipeline — Smart Traffic Stream Pipeline

Hệ thống thu thập ảnh camera giao thông TP.HCM → phân tích mật độ bằng AI (YOLO + MEU + Greenshields) → publish tốc độ từng edge vào Redis cho hệ thống định tuyến Valhalla (`smart-transport/valhalla-hcm-traffic`).

**Kiến trúc hiện hành: Redis queue (BLPOP) — không Kafka, không S3.** Bản Kafka + Flink + S3 cũ vẫn còn trong repo (xem mục "Kiến trúc cũ") nhưng không còn được dùng.

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
| `worker.py` tính MEU/speed đúng không | log của chính nó — tìm dòng `MEU=... raw=...km/h smooth=...km/h` |
| Hàng đợi camera còn tồn bao nhiêu | `docker exec smart-transport-redis redis-cli LLEN camera_queue` |
| Số edge đã có tốc độ trong Redis | `docker exec smart-transport-redis redis-cli HLEN traffic:speeds` |
| Toàn bộ dữ liệu tốc độ | `docker exec smart-transport-redis redis-cli HGETALL traffic:speeds` |
| Edge nào đang được coi là kẹt liên tục | `docker exec smart-transport-redis redis-cli KEYS "congestion:streak:*"` |
| Redis publish sự kiện real-time | `docker exec smart-transport-redis redis-cli SUBSCRIBE traffic:events` |
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
│    3. YOLO predict (AI/ module)                   │
│    4. MEU = Σ meu_coefficient[class]              │
│    5. Greenshields: raw_speed = free_flow×(1-d^n) │
│    6. EMA smoothing (chống outlier)               │
│    7. Streak counter (đếm chu kỳ đang kẹt)         │
│    8. HSET + HEXPIRE traffic:ema/traffic:speeds    │
└──────────┬─────────────────────────────────────┘
           │
           ▼
   Redis HASH "traffic:speeds" + "traffic:ema" (mỗi field tự có TTL 1200s)
   + Redis STRING "congestion:streak:{edge_id}"
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
3. **YOLO predict** (`AI/src/core/detector.py`, model `AI/weights/best.pt`, ngưỡng `CONFIDENCE=0.3`) → bounding box từng xe.
4. **MEU** (Motorcycle Equivalent Unit) = tổng hệ số quy đổi theo loại xe (`AI/config/meu_coefficients.json`: motorcycle=1.0, car=3.236, bus=8.568, truck=10).
5. **Greenshields → `raw_speed`** (thay hoàn toàn cho TWF/HCI của kiến trúc cũ):
   - `density = min(MEU / MEU_max[cam], 1.0)` — `MEU_max` tra theo `cam_id` trong `AI/config/camera_thresholds.json` (`meuMax`), fallback `MEU_MAX_DEFAULT=200.0` nếu camera chưa hiệu chuẩn (610/611 camera hiện đang dùng fallback — xem "Giới hạn").
   - `raw_speed = free_flow[edge] × (1 - density^n)` — `free_flow[edge]` tra theo `edge_id` trong `default_traffic.json` (`DEFAULT_EDGE_SPEED_KMH`), fallback `FREE_FLOW_DEFAULT_KMH=50` nếu edge không có; `n = GREENSHIELDS_N` (mặc định 1.0 = Greenshields tuyến tính gốc).
   - Chặn dưới: `raw_speed = max(raw_speed, MIN_SPEED_KMH=3.0)` — tránh tốc độ âm/bằng 0 khi density chạm 1.0.
6. **EMA → `smooth_speed`**: đọc giá trị trước đó qua `HGET traffic:ema {edge_id}`.
   - Nếu `|raw_speed - prev| > EMA_OUTLIER_THRESHOLD_KMH` (mặc định 25km/h) → coi là nhiễu (ảnh lỗi, YOLO detect sai đột biến), **bỏ qua toàn bộ bước 6-8 cho edge này**, giữ nguyên giá trị cũ trong Redis.
   - Ngược lại: `smooth = α × raw_speed + (1-α) × prev` (α = `EMA_ALPHA`, mặc định 0.3); nếu chưa có `prev` (edge lần đầu xuất hiện) thì `smooth = raw_speed`.
7. **Streak counter** — đếm số chu kỳ liên tiếp đang kẹt: nếu `smooth < JAM_THRESHOLD_KMH` (mặc định 15km/h) → `INCR congestion:streak:{edge_id}` (TTL 180s, tự xoá nếu không có chu kỳ nào cập nhật thêm); ngược lại → `DEL congestion:streak:{edge_id}`.
8. **Ghi Redis** (không gom batch, ghi trực tiếp ngay khi tính xong — khác hẳn kiến trúc Flink cũ):
   - `HSET traffic:ema {edge_id} {smooth}` — lưu giá trị mượt để làm `prev` cho vòng sau.
   - `HSET traffic:speeds {edge_id} "{smooth}:{timestamp_giây}"` — đúng định dạng daemon bên Valhalla cần.
   - `HEXPIRE` (Redis ≥ 7.4) đúng field vừa ghi trên cả 2 hash, TTL = `REDIS_FIELD_TTL_SECONDS` (mặc định 1200s) — dọn field nếu camera chết hẳn, không cần cho tính đúng đắn (xem mục 3 bên dưới), chỉ để Redis không phình vô hạn.
   - `PUBLISH traffic:events "update"` (giới hạn tối đa 1 lần/`PUBLISH_DEBOUNCE_SECONDS` bằng khoá `SET NX EX` trên Redis, tránh spam) — **hiện tại không ai lắng nghe kênh này** (xem mục 3), giữ lại phòng khi sau này có consumer event-driven khác.

Camera nào không có `edges` (không map được với Valhalla graph) bị bỏ qua ngay từ đầu `process_camera()`.

### 3. Redis + daemon cập nhật Valhalla (bên `valhalla-hcm-traffic`, không thuộc repo này)

#### 3.1. Cấu trúc dữ liệu Redis

- `traffic:speeds` — **HASH**: field = `edge_id`, value = `"{speed_kmh}:{timestamp_giây}"`.
- `traffic:ema` — **HASH** riêng, chỉ để `worker.py` tự đọc lại làm `prev` cho lần sau — **không phải** dữ liệu daemon Valhalla cần đọc.
- `congestion:streak:{edge_id}` — **STRING** (dùng như counter qua `INCR`), TTL 180s, phục vụ giám sát/log — chưa có consumer nào đọc field này trong pipeline (chỗ để mở rộng sau, ví dụ cảnh báo kẹt xe kéo dài).
- Cả `traffic:ema` và `traffic:speeds` đều có **TTL riêng từng field** (`HEXPIRE`, 1200s) — chỉ để dọn field khi camera chết hẳn, không phải cơ chế quyết định "còn tươi hay không" (xem 3.3).

#### 3.2. Có 2 bản daemon đọc Redis trong `valhalla-hcm-traffic` — chỉ 1 bản đang chạy

Khi tìm hiểu kỹ (đọc trực tiếp source bên trong container, vì 2 file này khác nhau và dễ nhầm), phát hiện project `valhalla-hcm-traffic` có **2 cách cập nhật live traffic hoàn toàn khác nhau**:

| | Bản cũ (`scripts/traffic_updater.py`, container `valhalla-hcm`) | Bản đang dùng (`/app/traffic_updater.py`, container `valhalla-traffic-daemon`) |
|---|---|---|
| Cách chạy | Subscribe Pub/Sub `traffic:events`, chạy ngay mỗi khi nhận message | **Tự poll theo đồng hồ cố định** (~15s), không quan tâm Pub/Sub |
| Cách ghi | Gọi subprocess `valhalla_traffic_demo_utils --seed-live-traffic-all` (**reset TOÀN BỘ edge về UNKNOWN**) rồi `--update-live-traffic-from-csv` (ghi lại từ CSV) | Ghi **trực tiếp qua `mmap`** vào `traffic.tar`, chỉ đúng edge nào thay đổi (không reset gì cả) |
| Vấn đề | Vì `worker.py` publish rất dày (per-camera), mỗi lần publish daemon này chạy `seed-live-traffic-all` → **mọi edge tạm thời về UNKNOWN** trước khi ghi lại — xảy ra gần như liên tục | Không có vấn đề này — ghi từng edge độc lập, không có khoảnh khắc "toàn bộ UNKNOWN" |

**2 daemon này từng chạy song song, cùng ghi vào 1 file `traffic.tar` → xung đột trực tiếp.** Đã xử lý: sửa `smart-transport/valhalla-hcm-traffic/docker-compose.yml`, gỡ dòng `python3 /scripts/traffic_updater.py &` khỏi container `valhalla-hcm` — giờ chỉ còn `/app/traffic_updater.py` (`valhalla-traffic-daemon`) là nguồn cập nhật live traffic duy nhất.

**Hệ quả:** `PUBLISH traffic:events` trong `worker.py` giờ **không còn ai lắng nghe** — daemon đang dùng hoàn toàn phớt lờ Pub/Sub. Vẫn giữ lại lời gọi này (đã debounce) trong code vì vô hại, phòng khi sau này có consumer event-driven khác cần.

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

**Lưu ý phát hiện thêm khi test:** lúc này `traffic.tar` chỉ có **đúng 1 tile** trong index cache (`/tmp/traffic_tar_index_cache.json`), khiến ~1046/1567 edge bị `oob` (không tìm thấy trong index) — cao hơn nhiều so với ~54/1567 (~3.4%) ghi nhận lúc mới chuyển sang kiến trúc Redis (mục "Kết quả kiểm thử"). Nhiều khả năng là hệ quả của các lần `--seed-live-traffic-all` từ script cũ (đã tắt, mục 3.2) chạy dồn dập trong lúc 2 daemon xung đột, làm `traffic.tar` bị thu hẹp lại. Cần kiểm tra/rebuild lại `traffic.tar` đầy đủ bên `valhalla-hcm-traffic` — **nằm ngoài phạm vi repo này**, ghi lại để không quên.

#### 3.5. Rủi ro còn lại — edge "kẹt vĩnh viễn" sau khi daemon restart, và giải pháp seed định kỳ

Cơ chế ở 3.3 (bước 3) chỉ hoạt động đúng vì daemon **tự nhớ trong RAM** danh sách "edge nào đang active" (biến `active_edges`). Danh sách này **không được lưu lại** — nếu daemon bị restart (crash, `docker restart`, deploy lại), biến này reset về rỗng.

Nếu ngay trước lúc restart có edge đang mang tốc độ thật (chưa kịp bị coi stale), và ngay sau restart camera đó ngừng gửi dữ liệu luôn → daemon mới khởi động **không hề biết** edge đó "từng active" để so sánh/xoá — tốc độ cũ đó **kẹt vĩnh viễn** trong `traffic.tar` cho tới khi chính edge_id đó tình cờ có dữ liệu mới ghi đè (có thể không bao giờ xảy ra nếu camera đã chết hẳn).

**Giải pháp đề xuất (chưa triển khai):** lên lịch chạy `valhalla_traffic_demo_utils --seed-live-traffic-all` (reset toàn bộ về UNKNOWN) **định kỳ, ví dụ 3h sáng mỗi ngày** — giờ ít xe để giảm ảnh hưởng của khoảng "toàn bộ UNKNOWN" ngắn ngay sau đó; daemon sẽ tự điền lại dữ liệu thật từ Redis trong vài chu kỳ poll tiếp theo (~15-30s) nhờ cơ chế ở 3.3. Việc này thuộc về hạ tầng `valhalla-hcm-traffic` (cần thêm cron/scheduled task trong container hoặc host chạy nó), **nằm ngoài phạm vi repo `stream-pipeline`** — ghi lại ở đây để không quên khi có dịp làm việc bên đó.

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

## Kết quả kiểm thử (chạy full 611 camera, 2026-07-06)

So sánh trực tiếp với kiến trúc Kafka+Flink+S3 cũ (đã kiểm thử ngay trước khi chuyển sang Redis queue):

| Chỉ số | Kafka + Flink + S3 (cũ) | Redis queue (mới) |
|---|---|---|
| Số edge có tốc độ trong Redis | Dao động thất thường: 553 → 1152 → 603 (tối đa 72%, không ổn định) | Tăng đều rồi ổn định ở **1567/1609 (97.4%)** |
| Độ trễ dữ liệu (freshness) | ~12 giờ (backlog Kafka `raw_images` tồn ~149 000 message, tăng liên tục) | **~6 giây** |
| `traffic_updater.py` ghi vào Valhalla | `written=0` mọi lần — **100% dữ liệu bị coi stale**, Valhalla không nhận traffic nào | `written=1513, stale=0` — ghi thành công liên tục |
| Lỗi trong log worker/consumer | 1 lỗi warm-up (tự phục hồi), còn lại sạch | **0 lỗi** trên cả 3 instance sau nhiều phút chạy |

**Nguyên nhân kiến trúc cũ thất bại:** `consumer_ai.py` (1 process, `CONCURRENCY=4`) không xử lý kịp lượng ảnh của 611 camera → backlog Kafka phình to → dữ liệu tới Redis luôn mang timestamp cũ hàng giờ → bị `traffic_updater.py` tự động loại bỏ vì vượt `SPEED_TTL_SECONDS=900`. Kiến trúc Redis queue giải quyết bằng cách **chạy nhiều `worker.py` song song** (throughput scale ngang theo số instance, không bị giới hạn bởi 1 process duy nhất) và **bỏ qua bước upload/download S3** (giảm độ trễ mỗi camera).

**Ghi nhận nhỏ, chưa xử lý:** `valhalla-traffic-daemon` log thêm cảnh báo `N edge(s) not found in traffic.tar index` (khoảng 54/1567, ~3.4%) — một số edge publish lên không khớp với bản đồ Valhalla đang chạy (có thể do build đồ thị khác thời điểm với `cameras_with_zones_merged.json`). Không liên quan tới lỗi throughput đã khắc phục, nằm ngoài phạm vi lần sửa này.

---

## Giới hạn hiện tại (đã biết, chưa xử lý)

| # | Vấn đề | Ảnh hưởng |
|---|---|---|
| 1 | `MEU_max` chỉ hiệu chuẩn phẳng theo camera, không tách theo giờ trong ngày như thiết kế gốc muốn | 610/611 camera dùng `MEU_MAX_DEFAULT=200.0` — density có thể không phản ánh đúng thực tế theo khung giờ cao/thấp điểm |
| 2 | `camera_thresholds.json` chỉ có hiệu chuẩn cho `camera_001` | Tương tự #1 — cần thu thập dữ liệu lịch sử để hiệu chuẩn thêm |
| 3 | `congestion:streak:{edge_id}` chưa có consumer nào đọc | Dữ liệu được ghi nhưng chưa dùng vào việc gì cụ thể (cảnh báo, dashboard...) |
| 4 | ~3.4% edge publish lên không khớp `traffic.tar` index của Valhalla | Traffic của các edge đó không được áp dụng dù publish thành công về phía Redis |
| 5 | `old-pipeline/consumer_db.py` | Không chạy được — xem mục 5 ở Phần 2 |
| 6 | Số instance `worker.py` cần chạy tay theo cấu hình máy, chưa có auto-scaling | Vận hành thủ công phải tự theo dõi độ dài `camera_queue` để quyết định thêm/bớt instance |
| 7 | Daemon Valhalla (`/app/traffic_updater.py`) nhớ "edge nào đang active" trong RAM, mất hết khi daemon restart | Edge có tốc độ thật ngay trước lúc restart, rồi camera chết ngay sau đó → tốc độ cũ **kẹt vĩnh viễn** trong `traffic.tar`, không tự xoá được nữa (xem Phần 2, mục 3.5). Đề xuất: seed lại toàn bộ định kỳ (vd 3h sáng), **chưa triển khai** — thuộc hạ tầng `valhalla-hcm-traffic`, ngoài phạm vi repo này |
| 8 | `traffic.tar` hiện chỉ có 1 tile trong index cache của daemon (phát hiện lúc test mục 3.4), oob tăng từ ~3.4% lên ~67% (1046/1567) | Phần lớn edge publish lên không được áp dụng vào Valhalla dù Redis/daemon đều hoạt động đúng — nghi do các lần seed-live-traffic-all của script cũ trước khi bị tắt (mục 3.2), cần rebuild lại `traffic.tar` bên `valhalla-hcm-traffic`, ngoài phạm vi repo này |

## Lưu ý vận hành

- **Console Windows** mặc định dùng codepage không hỗ trợ Unicode — `queue_feeder.py`/`worker.py` đã tự reconfigure `stdout`/`stderr` sang UTF-8 khi phát hiện encoding khác UTF-8.
- Sửa `config.py`/`cameras_with_zones_merged.json`/`default_traffic.json` trong lúc `queue_feeder.py`/`worker.py` đang chạy — phải Ctrl+C và chạy lại **tất cả**, Python không tự reload code.
- Kiến trúc Kafka+Flink+S3 cũ vẫn cần rebuild/resubmit thủ công như trước nếu có ai chủ động quay lại dùng (xem mục "Kiến trúc cũ").
- Đã tắt daemon Pub/Sub cũ (`python3 /scripts/traffic_updater.py`) trong container `valhalla-hcm` — sửa trực tiếp `smart-transport/valhalla-hcm-traffic/docker-compose.yml`, gỡ dòng chạy script đó khỏi `command:` (chi tiết + lý do ở Phần 2, mục 3.2). Nếu ai đó `git pull`/rebuild lại project `valhalla-hcm-traffic` từ nơi khác mà không có thay đổi này, xung đột 2 daemon có thể quay lại.
