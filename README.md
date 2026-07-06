# stream-pipeline — Smart Traffic Stream Pipeline

Hệ thống thu thập ảnh camera giao thông TP.HCM → phân tích mật độ bằng AI (YOLO + chỉ số TWF) → xử lý stream real-time bằng Apache Flink → publish tốc độ từng edge vào Redis cho hệ thống định tuyến Valhalla (`smart-transport/valhalla-hcm-traffic`).

---

## Phần 1 — Cách chạy từ đầu

### 0. Yêu cầu

- Python 3.12 (kiểm tra `python -c "import sys; print(sys.executable)"` để chắc chắn dùng đúng bản nếu máy có nhiều Python).
- Docker Desktop đang chạy.
- Repo `smart-transport` (cùng cấp thư mục với `stream-pipeline`) — cung cấp Kafka, Redis, Postgres, Valhalla, và các UI theo dõi (Kafka UI, Redis Commander).
- File `.env` ở root project đã điền:
  ```
  AWS_REGION=...
  AWS_ACCESS_KEY=...
  AWS_SECRET_KEY=...
  BUCKET_NAME=...
  KAFKA_BROKER=localhost:9092   # chạy trên host, KHÔNG dùng kafka:29092 (chỉ dùng nội bộ container)
  DB_URL=...                    # chưa dùng được, xem mục "Giới hạn hiện tại"
  CONFIDENCE=0.3
  ```

### 1. Cài dependency Python

```powershell
python -m pip install ultralytics opencv-python numpy pillow aiohttp aioboto3 aiokafka asyncpg redis python-dotenv
```

### 2. Kiểm tra AWS key còn sống (không bị AWS tự quarantine)

```powershell
python -c "from dotenv import load_dotenv; load_dotenv(); import boto3, os; s3=boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY'), aws_secret_access_key=os.getenv('AWS_SECRET_KEY'), region_name=os.getenv('AWS_REGION')); print(s3.list_objects_v2(Bucket=os.getenv('BUCKET_NAME'), MaxKeys=1))"
```
Nếu báo `AccessDenied ... AWSCompromisedKeyQuarantineV3` — access key đã bị AWS khoá vì phát hiện lộ công khai. Phải tạo **IAM user mới hẳn** (tạo key mới cho user cũ KHÔNG đủ — policy quarantine gắn vào cả user, không phải riêng từng key) rồi cập nhật `.env`.

### 3. Lên hạ tầng

```powershell
# Kafka, Redis, Postgres, Valhalla...
cd C:\Users\VIET\Desktop\smart-transport
docker compose up -d

# Flink jobmanager/taskmanager (build image riêng cho project này)
cd C:\Users\VIET\Desktop\stream-pipeline
docker compose up -d --build
```
Đợi `docker ps` thấy Kafka ở trạng thái `healthy`.

### 4. Tạo Kafka topic (chỉ cần 1 lần / cluster mới)

Kafka bên `smart-transport` set `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — phải tạo tay:
```powershell
docker exec smart-transport-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic raw_images --partitions 3 --replication-factor 1
docker exec smart-transport-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic traffic_events --partitions 3 --replication-factor 1
docker exec smart-transport-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic traffic_realtime --partitions 3 --replication-factor 1
docker exec smart-transport-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic traffic_agg --partitions 3 --replication-factor 1
```

### 5. Submit Flink job

```powershell
docker exec flink-jobmanager bin/flink run -py /opt/flink/jobs/vehicle_count_agg_job.py
```
Kiểm tra `localhost:8082` → job `traffic_dual_pipeline` ở trạng thái `RUNNING`.

> Mỗi khi sửa code trong `jobs/vehicle_count_agg_job.py` hoặc `cameras_with_zones_merged.json`, phải `docker compose up -d --build` lại rồi cancel job cũ (`bin/flink cancel <JOB_ID>`) + submit lại — Docker image là snapshot tại lúc build, không tự đồng bộ với file trên máy.

### 6. Chạy 2 service Python — **đúng thứ tự, mỗi cái 1 cửa sổ PowerShell riêng**

Consumer phải sống **trước** producer vì dùng `auto_offset_reset="latest"` — chạy sai thứ tự sẽ bỏ lỡ message sinh ra trước khi consumer kịp start.

**Cửa sổ 1 — `consumer_ai.py`:**
```powershell
cd C:\Users\VIET\Desktop\stream-pipeline
$env:CAMERA_LIMIT = "3"    # giới hạn số camera để test nhẹ; bỏ biến này = bật hết 611 camera
python consumer_ai.py
```

**Cửa sổ 2 — `ingestion.py`:**
```powershell
cd C:\Users\VIET\Desktop\stream-pipeline
$env:CAMERA_LIMIT = "3"    # PHẢI giống giá trị ở cửa sổ 1
python ingestion.py
```

> Lưu ý PowerShell: biến môi trường set bằng `$env:TÊN = "giá_trị"`, không dùng cú pháp `VAR=value command` như bash. Đóng cửa sổ là mất biến, phải set lại mỗi lần mở terminal mới.
>
> Nếu sửa `config.py`/`cameras_with_zones_merged.json` trong lúc 2 process đang chạy — **phải Ctrl+C và chạy lại cả 2**, Python không tự reload code.

### 7. Kiểm tra kết quả

| Muốn xem | Lệnh |
|---|---|
| `consumer_ai.py` tính TWF/speed đúng không | log của chính nó — tìm dòng `TWF=... -> speed=...km/h` |
| Message trong `raw_images` / `traffic_events` | Kafka UI `localhost:8090` |
| Flink job có chạy ổn không | Flink UI `localhost:8082` → tab Checkpoints phải `COMPLETED` |
| Output Flink (`realtime`/`agg`/batch) | `docker logs -f flink-taskmanager` |
| Kết quả cuối trong Redis | `docker exec smart-transport-redis redis-cli HGETALL traffic:speeds` |
| Redis publish sự kiện real-time | `docker exec smart-transport-redis redis-cli SUBSCRIBE traffic:events` |

### 8. Dừng lại

```powershell
# Ctrl+C ở 2 cửa sổ consumer_ai.py / ingestion.py

docker exec flink-jobmanager bin/flink list                # lấy JOB_ID
docker exec flink-jobmanager bin/flink cancel <JOB_ID>

cd C:\Users\VIET\Desktop\stream-pipeline
docker compose down
cd C:\Users\VIET\Desktop\smart-transport
docker compose down
```

---

## Phần 2 — Pipeline chi tiết & logic từng thành phần

### Sơ đồ tổng thể

```
cameras_with_zones_merged.json (611 camera thật, 1609 edge_id thật từ Valhalla graph)
        │
        ▼
┌─────────────────────┐
│ 1. ingestion.py       │  asyncio, mỗi 10s/chu kỳ (30s ban đêm 22h–6h)
└──────────┬────────────┘
           │ Kafka: raw_images  {s3_key, cam_id, timestamp}
           ▼
┌─────────────────────┐
│ 2. consumer_ai.py     │  group "ai-detector" — dùng module AI/
└──────────┬────────────┘
           │ Kafka: traffic_events  {source_type:"gps", edge_id, speed_kmh, confidence}
           ▼
┌───────────────────────────────────────────────────┐
│ 3. Flink: jobs/vehicle_count_agg_job.py               │
│    Parse → Dedup → ┬─ EWMA ──→ traffic_realtime         │
│                     │           └─ RedisBatchPublisher   │
│                     └─ SlidingWindow(5min/90s) → traffic_agg │
└───────────────────────────────────────────────────┘
           │
           ▼
   Redis HASH "traffic:speeds" + PUBLISH "traffic:events"
           │
           ▼
   traffic_updater.py (bên valhalla-hcm-traffic — không thuộc repo này)
           │
           ▼
   Valhalla live traffic → routing
```

### 1. `ingestion.py` — thu thập ảnh

- Đọc danh sách camera từ `config.CAMERAS` (nguồn: `cameras_with_zones_merged.json`, xem mục 5).
- Vòng lặp mỗi `INTERVAL_SECONDS=10s`: mỗi camera được gọi với **delay rải** (stagger theo index + jitter ngẫu nhiên) để tránh dồn request cùng lúc, giới hạn đồng thời bằng `Semaphore(MAX_CONCURRENT_REQUESTS=30)`.
- Với mỗi camera: GET ảnh JPEG (kèm cookie phiên + header giả lập trình duyệt) → validate JPEG hợp lệ (magic bytes + size tối thiểu) → dedup bằng MD5 hash so với ảnh lần trước (bỏ qua nếu trùng, tránh xử lý lại ảnh không đổi) → **upload S3** (`raw_images/{cam_id}/{ts_ms}.jpg`) → publish **chỉ metadata** (không kèm bytes ảnh) lên Kafka `raw_images`.
- Tự động refresh cookie phiên mỗi 1h; nếu toàn bộ camera cùng lúc trả HTTP 403 (dấu hiệu cookie hết hạn) → force refresh ngay, retry với exponential backoff (base 3s, nhân đôi mỗi lần, tối đa 5 lần).
- Xử lý shutdown sạch qua `SIGINT`/`SIGTERM`.

### 2. `consumer_ai.py` — phân tích AI, tính tốc độ

Đọc `raw_images` (Kafka consumer group `ai-detector`, batch tối đa `CONCURRENCY=4` frame xử lý song song), với mỗi frame:

1. **Tải ảnh từ S3** bằng `s3_key` trong message → decode `PIL.Image` → `np.ndarray`.
2. **YOLO predict** (`AI/src/core/detector.py`, model `AI/weights/best.pt`, ngưỡng `CONFIDENCE=0.3`) → bounding box từng xe.
3. **Heatmap tích luỹ** (`AI/src/core/metrics.py:updateHeatmapByCameraId`) — cộng dồn vị trí xe vào `AI/storage/camera_matrices/{cam_id}_heatmap.npy`, tồn tại xuyên suốt qua nhiều lần chạy.
4. **Sinh Road Mask** (`generateAndSaveRoadMask(cam_id, threshold=1)`) — nhị phân hoá heatmap thành "vùng nào là mặt đường thật" (loại nhiễu vỉa hè/bãi xe). **Gọi lại mỗi frame** thay vì định kỳ hàng tháng như thiết kế gốc — chấp nhận được ở giai đoạn hiện tại, xem mục "Giới hạn".
5. **Tính 2 loại occupancy:**
   - `preciseOccupancyRatio` — % diện tích xe đè lên đúng Road Mask.
   - `rawOccupancyRatio` — % diện tích bbox thô/tổng ảnh (gồm cả overlap xe đè nhau).
6. **MEU** (Motorcycle Equivalent Unit) — quy đổi số xe theo hệ số từng loại (`AI/config/meu_coefficients.json`, key = class ID YOLO: 0=motorcycle, 1=car, 2=bus, 3=large_vehicle).
7. **HCI** (Hybrid Congestion Index) = `w1×(MEU/meuMax) + w2×(occupancy/orMax)` — `meuMax`/`orMax` tra theo `cam_id` trong `AI/config/camera_thresholds.json` (chỉ có `camera_001` demo, camera thật dùng mặc định `1.0` — xem "Giới hạn").
8. **TWF** (Traffic Weight Factor) — hàm 2 tầng trong `calculateTrafficWeightFactor`:
   - Tiền điều kiện: `preciseOccupancyRatio < 85%` → TWF = 1.0 (thông thoáng).
   - Nếu ≥ 85%: so `HCI/hciMax` → ≥0.9 → TWF=0.1 (kẹt nặng, phạt chi phí route ×10); ≥0.8 → TWF=0.5 (ùn nhẹ, ×2); còn lại → TWF=1.0.
9. **`speed_kmh = TWF × base_speed_kmh`** — tính **riêng cho từng edge** của camera (không dùng chung 1 giá trị cho cả camera nữa):
   - `base_speed_kmh` = tốc độ mặc định (free-flow) của **chính edge đó**, tra trong `default_traffic.json` bằng đúng `edge_id` (xem điểm quan trọng bên dưới về định dạng `edge_id`).
   - Nếu edge không có trong `default_traffic.json` → fallback về `TWF_MAX_SPEED_KMH` (mặc định 50km/h, cấu hình qua env). Với dữ liệu hiện tại, toàn bộ 1609/1609 edge đều có mặt trong `default_traffic.json` nên nhánh fallback gần như không kích hoạt — vẫn giữ lại làm lưới an toàn cho camera/edge mới thêm sau này chưa kịp cập nhật vào `default_traffic.json`.
   - Vì mỗi edge của cùng 1 camera có thể thuộc `road_class` khác nhau (ví dụ 1 hướng là đường chính tốc độ 90km/h, hướng kia là hẻm 40km/h), 2 edge của cùng 1 frame/1 TWF có thể ra `speed_kmh` khác nhau dù cùng mức độ kẹt.

**Điểm quan trọng — định dạng `edge_id`:** mỗi zone trong `cameras_with_zones_merged.json` có field `mapped_edge` — edge do Valhalla map-matching trả về, đồng nhất 1 schema ở mọi zone (khác với `zone["edge_id"]`/`zone["edge_id_full"]` ở cấp trên, vốn không đồng nhất giữa các zone: ~68% là dict lồng, ~32% là số ngắn). `config.py:_extract_edge_id` lấy `zone["mapped_edge"]["edge_id"]["value"]` làm `edge_id` xuyên suốt toàn bộ pipeline (Kafka, Redis, Flink) — đây là GraphId đầy đủ (gộp `tile_id+level+id`, duy nhất toàn cục), KHÔNG dùng `id` ngắn (chỉ là số thứ tự cục bộ trong 1 tile bản đồ, không xác định duy nhất 1 con đường toàn thành phố). Khớp đúng định dạng `default_traffic.json` dùng (đã verify khớp 1609/1609 khi đối chiếu 2 file).
10. Với mỗi `edge_id` thuộc camera đó (1 camera có thể phủ nhiều edge): publish lên `traffic_events` với `source_type: "gps"` (mượn nhánh EWMA-speed sẵn có của Flink, vì hệ thống chưa có nguồn GPS thật), `confidence: 1.0` (cố định, placeholder).

### 3. Flink job — `jobs/vehicle_count_agg_job.py`

Chạy trên cluster Docker riêng (`Dockerfile` build từ `flink:1.18-scala_2.12` + PyFlink 1.18 + connector Kafka + `redis-py`), dùng **DataStream API** (streaming thật, không phải batch job).

- **Kafka Source** đọc `traffic_events`, offset `latest`, watermark cho phép trễ tối đa 10s (`out-of-orderness`), idle 1 phút.
- **`ParseAndFilterMessage`** (FlatMap) — chuẩn hoá schema camera/gps về 1 dạng chung, parse JSON đúng 1 lần, sinh `dedup_key`.
- **`DeduplicateFunction`** (`key_by(edge_id)`, `MapState` TTL 60s) — loại message trùng do Kafka retry; state gom theo edge (bounded), không phải theo từng fingerprint (tránh OOM).
- **Nhánh trái — `AdaptiveEwmaFunction`** (`key_by(edge_id)`): cập nhật EWMA tức thì mỗi event, alpha thích ứng theo lưu lượng mẫu gần đây (`ALPHA_MIN=0.1` → `ALPHA_MAX=0.5`, ngưỡng riêng GPS=20/phút vs Camera=6/phút — camera-based speed hiện đang dùng ngưỡng GPS dù tần suất thực tế giống camera hơn, xem "Giới hạn"). Emit mỗi `EMIT_INTERVAL_MS=30s`/edge qua processing-time timer → Kafka `traffic_realtime`.
- **`RedisBatchPublisher`** (`key_by("ALL")`, 1 instance, `set_parallelism(1)`): gom nhiều edge vào 1 `MapState` trước khi ghi Redis, publish khi **đủ `TOTAL_EDGES`** edge khác nhau (tính từ toàn bộ `cameras_with_zones_merged.json` qua `mapped_edge.edge_id.value`, hiện là 1609) **HOẶC** hết `BATCH_WINDOW_MS` (mặc định 300s/5 phút) — tuỳ điều kiện nào tới trước (thực tế hầu như luôn là timeout vì 1609 hiếm khi đủ trong 1 batch — không phải camera/edge nào cũng có dữ liệu mới liên tục). Khi flush: `DELETE` sạch `traffic:speeds` rồi `HSET` lại toàn bộ batch (dọn dẹp + ghi mới trong 1 pipeline atomic), `PUBLISH "update"` đúng 1 lần trên `traffic:events`.
- **Nhánh phải — Sliding Window**: `key_by(edge_id)`, cửa sổ event-time 5 phút trượt mỗi 90s (`SlidingEventTimeWindows`), `SumReduce` gộp incremental + `AttachWindow` gắn `window_start`/`window_end` → Kafka `traffic_agg`. Độc lập với nhánh speed/Redis, phục vụ thống kê/báo cáo.
- Checkpoint mỗi 60s (`min_pause=30s`, `timeout=300s`), state backend RocksDB, TTL state 2h/edge (tự dọn edge không hoạt động).

### 4. Redis + `traffic_updater.py` (bên `valhalla-hcm-traffic`, không thuộc repo này)

- `traffic:speeds` là **1 Redis HASH duy nhất**: field = `edge_id`, value = chuỗi `"{speed_kmh}:{timestamp_giây}"`.
- `traffic_updater.py` subscribe channel `traffic:events`; mỗi khi nhận tín hiệu `"update"` (hoặc fallback poll mỗi 300s nếu lâu không có tín hiệu): `HGETALL traffic:speeds` (đọc **toàn bộ** hash — đây chính là cơ chế "list toàn bộ edge", không cần Flink emit mảng) → lọc field quá hạn (`SPEED_TTL_SECONDS=900`) hoặc ngoài khoảng hợp lệ (`5–120 km/h`) → ghi CSV `edge_id,speed_kph` → gọi `valhalla_traffic_demo_utils --seed-live-traffic-all` (reset toàn bộ về UNKNOWN) rồi `--update-live-traffic-from-csv` (ghi đè toàn bộ bằng dữ liệu vừa lọc) → Valhalla dùng để tính route.

### 5. `consumer_db.py` — **chưa hoạt động được**

Dự định ghi kết quả xuống TimescaleDB, nhưng hiện `import TOPIC_COUNTS` từ `config.py` (biến không tồn tại) → crash ngay khi chạy. Schema cũng không khớp bất kỳ topic nào đang có (`traffic_realtime`/`traffic_agg` đều theo `edge_id`, không phải `camera_id` phẳng như file này mong đợi). Nằm ngoài phạm vi các lần sửa vừa qua — cần làm lại riêng nếu muốn dùng.

---

## Giới hạn hiện tại (đã biết, chưa xử lý)

| # | Vấn đề | Ảnh hưởng |
|---|---|---|
| 1 | `confidence` của speed suy từ camera luôn cố định `1.0` | Chưa phản ánh độ tin cậy thật của phép đo |
| 2 | Camera-based speed gắn `source_type:"gps"`, dùng nhầm `GPS_VOLUME_THRESHOLD=20/phút` thay vì `CAM_VOLUME_THRESHOLD=6/phút` | EWMA mượt hơn cần thiết, phản hồi chậm hơn kỳ vọng |
| 3 | Road Mask tính lại mỗi frame thay vì định kỳ (thiết kế gốc: hàng tháng) | Tốn CPU khi scale lên nhiều camera cùng lúc |
| 4 | `camera_thresholds.json` chỉ có `meuMax`/`orMax`/`hciMax` cho `camera_001` demo | 610/611 camera thật dùng ngưỡng mặc định `1.0` → TWF chỉ còn hoạt động như công tắc 2 trạng thái (1.0 hoặc 0.1), mức trung gian 0.5 gần như không bao giờ kích hoạt |
| 5 | 1 camera phủ nhiều edge nhưng **TWF** (mức độ kẹt) chỉ tính 1 lần/frame trên toàn ảnh (không dùng field `points` khoanh vùng riêng từng zone) — `base_speed_kmh` thì đã tách riêng theo từng edge qua `default_traffic.json` | Các edge của cùng 1 camera có `speed_kmh` khác nhau nếu `road_class` khác nhau, nhưng vẫn cùng chung 1 mức TWF — nếu 1 hướng kẹt còn hướng kia thông thoáng, hệ thống không phân biệt được |
| 6 | `consumer_db.py` | Không chạy được — xem mục 5 ở trên |
| 7 | Chạy 611 camera cùng lúc | Rủi ro rate-limit/chặn IP từ `giaothong.hochiminhcity.gov.vn`, chi phí S3/compute tăng đáng kể so với lúc test |

## Lưu ý vận hành

- **Không commit `.env`** — nếu AWS key từng bị lộ (đã từng xảy ra), AWS sẽ tự động gắn policy `AWSCompromisedKeyQuarantineV3` vào IAM user, chặn toàn bộ request dù tạo key mới cho cùng user. Phải tạo IAM user mới hoàn toàn trong trường hợp đó.
- **Console Windows** mặc định dùng codepage không hỗ trợ Unicode (`━`, tiếng Việt có dấu) — cả 3 entry point (`ingestion.py`, `consumer_ai.py`, `consumer_db.py`) đã tự reconfigure `stdout`/`stderr` sang UTF-8 khi phát hiện encoding khác UTF-8, không cần chỉnh `chcp` thủ công.
- Sau khi sửa `jobs/vehicle_count_agg_job.py` hoặc `cameras_with_zones_merged.json`, luôn `docker compose up -d --build` + cancel/resubmit job Flink — image không tự đồng bộ với file trên máy host.
