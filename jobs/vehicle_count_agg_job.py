    import json
    import os
    from datetime import datetime, timezone, timedelta

    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaSource, KafkaOffsetsInitializer,
        KafkaSink, KafkaRecordSerializationSchema,
    )
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    from pyflink.common import WatermarkStrategy, Duration, Types
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.datastream.window import SlidingEventTimeWindows, Time
    from pyflink.datastream.functions import (
        FlatMapFunction, ReduceFunction, ProcessWindowFunction, KeyedProcessFunction,
    )
    from pyflink.datastream.state import (
        ValueStateDescriptor, MapStateDescriptor, StateTtlConfig,
    )
    from pyflink.common.time import Time as StateTime

    # ── Config ────────────────────────────────────────────────────────────────
    KAFKA_BROKER          = os.getenv("KAFKA_BROKER",           "localhost:9092")
    TRAFFIC_EVENTS        = os.getenv("TRAFFIC_EVENTS",         "traffic_events")
    REALTIME_TOPIC        = os.getenv("REALTIME_TOPIC",         "traffic_realtime")
    AGG_TOPIC             = os.getenv("AGG_TOPIC",              "traffic_agg")

    # EWMA params
    ALPHA_MIN             = float(os.getenv("ALPHA_MIN",          "0.1"))
    ALPHA_MAX             = float(os.getenv("ALPHA_MAX",          "0.5"))
    # [FIX #2] Threshold riêng cho từng source: GPS ~20 ping/min, Camera ~6 frame/min
    GPS_VOLUME_THRESHOLD  = int(os.getenv("GPS_VOLUME_THRESHOLD", "20"))
    CAM_VOLUME_THRESHOLD  = int(os.getenv("CAM_VOLUME_THRESHOLD", "6"))
    VOLUME_WINDOW_MS      = int(os.getenv("VOLUME_WINDOW_MS",     str(60_000)))   # 60s
    EMIT_INTERVAL_MS      = int(os.getenv("EMIT_INTERVAL_MS",     str(30_000)))   # 30s

    # State TTL: xoá state nếu edge_id không có event nào trong 2 tiếng
    STATE_TTL_HOURS       = int(os.getenv("STATE_TTL_HOURS",      "2"))

    # Dedup TTL: giữ fingerprint 60s để bắt Kafka retry muộn
    DEDUP_TTL_MS          = int(os.getenv("DEDUP_TTL_MS",         str(60_000)))

    # [FIX #5] Checkpoint config hợp lệ:
    #   interval=60s, min_pause=30s → effective interval ≥ 90s dưới tải nặng
    #   timeout=300s (5×interval) → checkpoint có 5 phút để hoàn thành trước khi
    #   bị huỷ. Trước đây timeout=60s < interval*2 → checkpoint mới bắt đầu trước
    #   khi cái cũ xong → Flink huỷ liên tục → job không bao giờ có snapshot hợp lệ.
    CHECKPOINT_INTERVAL_MS  = int(os.getenv("CHECKPOINT_INTERVAL_MS",  str(60_000)))
    CHECKPOINT_MIN_PAUSE_MS = int(os.getenv("CHECKPOINT_MIN_PAUSE_MS", str(30_000)))
    CHECKPOINT_TIMEOUT_MS   = int(os.getenv("CHECKPOINT_TIMEOUT_MS",   str(300_000)))

    VN_TZ = timezone(timedelta(hours=7))


    def ms_to_str(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, tz=VN_TZ).strftime("%H:%M:%S")


    # [FIX #2] Nhận threshold làm tham số → GPS và Camera có ngưỡng riêng
    def compute_alpha(sample_count: int, threshold: int) -> float:
        ratio = min(sample_count / threshold, 1.0)
        return ALPHA_MIN + (ALPHA_MAX - ALPHA_MIN) * ratio


    # ── 1. Timestamp Assigner ─────────────────────────────────────────────────
    class EventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            try:
                return int(json.loads(value).get("timestamp", 0))
            except Exception:
                return 0


    # ── 2. ParseAndFilterMessage ──────────────────────────────────────────────
    #
    #  [FIX #4] Dùng FlatMapFunction thay vì MapFunction + filter(json.loads(...)).
    #  Trước đây: ParseMessage.map() parse JSON lần 1 → emit sentinel string →
    #  filter lambda parse JSON lần 2 để đọc __drop__. Mỗi event bị parse 2 lần.
    #  Giờ: FlatMapFunction parse 1 lần, yield nếu hợp lệ, im lặng nếu không.
    #  Không cần sentinel, không cần parse lần 2.
    #
    class ParseAndFilterMessage(FlatMapFunction):

        def flat_map(self, value: str):
            try:
                obj      = json.loads(value)
                edge_id  = obj.get("edge_id")
                ts       = obj.get("timestamp")
                src_type = obj.get("source_type")

                if not edge_id or not ts or src_type not in ("camera", "gps"):
                    return  # drop silently, không yield gì

                dedup_key = (
                    str(obj.get("event_id"))
                    if obj.get("event_id")
                    else f"{edge_id}_{ts}_{src_type}"
                )

                base = {
                    "edge_id":        str(edge_id),
                    "timestamp":      int(ts),
                    "way_id":         obj.get("way_id"),
                    "source_type":    src_type,
                    "dedup_key":      dedup_key,
                    "vehicle_counts": {},
                    "frame_count":    0,
                    "speed_sum":      0.0,
                    "confidence_sum": 0.0,
                    "gps_count":      0,
                }

                if src_type == "camera":
                    counts = obj.get("vehicle_counts") or {}
                    base["vehicle_counts"] = counts if isinstance(counts, dict) else {}
                    base["frame_count"]    = 1
                else:
                    speed      = obj.get("speed_kmh")
                    confidence = obj.get("confidence")
                    if speed is None or confidence is None:
                        return  # drop silently
                    base["speed_sum"]      = float(speed)
                    base["confidence_sum"] = float(confidence)
                    base["gps_count"]      = 1

                yield json.dumps(base)

            except Exception:
                return  # drop silently


    # ── 3. DeduplicateFunction ────────────────────────────────────────────────
    #
    #  [FIX #6] Trước đây key_by(dedup_key) → mỗi event fingerprint là 1 Flink key
    #  riêng biệt → hàng chục ngàn key nhỏ li ti → state backend bị phân mảnh,
    #  OOM dưới tải cao vì mỗi key có overhead riêng (metadata, RocksDB column).
    #
    #  Giờ: key_by(edge_id) → mỗi edge là 1 key → dùng MapState<dedup_key, bool>
    #  với TTL per-entry. Mỗi edge tối đa ~26 fingerprint trong 60s → bounded.
    #  Tổng state = số_edge × 26 thay vì throughput × 60s (unbounded với key cũ).
    #
    class DeduplicateFunction(KeyedProcessFunction):

        def open(self, runtime_context):
            ttl = (
                StateTtlConfig
                .new_builder(StateTime.milliseconds(DEDUP_TTL_MS))
                # OnWriteOnly: TTL chạy từ lần ghi đầu tiên, không reset khi đọc.
                # Đảm bảo fingerprint tự xoá sau DEDUP_TTL_MS kể từ lúc event gốc
                # được nhận, bất kể có bao nhiêu duplicate đến sau.
                .set_update_type(StateTtlConfig.UpdateType.OnWriteOnly)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            desc = MapStateDescriptor("seen_keys", Types.STRING(), Types.BOOLEAN())
            desc.enable_time_to_live(ttl)
            self.seen_keys = runtime_context.get_map_state(desc)

        def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
            msg       = json.loads(value)
            dedup_key = msg["dedup_key"]

            if self.seen_keys.contains(dedup_key):
                return  # duplicate → drop
            self.seen_keys.put(dedup_key, True)
            yield value


    # ══════════════════════════════════════════════════════════════════════════
    #  LUỒNG TRÁI — Adaptive EWMA (traffic_realtime)
    # ══════════════════════════════════════════════════════════════════════════

    class AdaptiveEwmaFunction(KeyedProcessFunction):
        """
        Cập nhật EWMA state tức thì sau mỗi event, emit mỗi EMIT_INTERVAL_MS.

        bucket_counts_gps / bucket_counts_cam lưu riêng số event theo từng source
        trong cửa sổ VOLUME_WINDOW_MS (60s) → compute_alpha độc lập với threshold
        riêng (GPS=20, Camera=6). Gộp chung sẽ làm alpha Camera bị đội bởi volume
        GPS, mất tính độc lập của từng nguồn.
        """

        def _make_ttl_config(self) -> StateTtlConfig:
            return (
                StateTtlConfig
                .new_builder(StateTime.hours(STATE_TTL_HOURS))
                .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )

        def open(self, runtime_context):
            ttl = self._make_ttl_config()

            ewma_desc = ValueStateDescriptor("ewma_state", Types.STRING())
            ewma_desc.enable_time_to_live(ttl)
            self.ewma_state = runtime_context.get_state(ewma_desc)

            timer_desc = ValueStateDescriptor("timer_registered", Types.BOOLEAN())
            timer_desc.enable_time_to_live(ttl)
            self.timer_registered = runtime_context.get_state(timer_desc)

        def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
            msg      = json.loads(value)
            ts       = msg["timestamp"]
            src_type = msg["source_type"]

            raw   = self.ewma_state.value()
            prior = json.loads(raw) if raw else {
                "ewma_speed":        None,
                "ewma_counts":       {},
                "ewma_confidence":   None,
                "bucket_counts_gps": {},
                "bucket_counts_cam": {},
                "way_id":            msg.get("way_id"),
            }

            # [FIX #1] cutoff phải được căn chỉnh về bucket boundary, không dùng
            # exact timestamp. Trước đây: cutoff = ts - 60000 (timestamp thô) so
            # sánh với bucket_key (đã floor về bội số 60000) → lệch hệ quy chiếu,
            # có thể giữ bucket quá cũ hoặc xoá bucket còn hiệu lực.
            # Đúng: floor cả cutoff về cùng bucket grid để so sánh táo với táo.
            bucket_key     = str(ts // VOLUME_WINDOW_MS * VOLUME_WINDOW_MS)
            cutoff_key     = str((ts - VOLUME_WINDOW_MS) // VOLUME_WINDOW_MS * VOLUME_WINDOW_MS)

            def update_bucket(buckets: dict) -> dict:
                buckets = dict(buckets)
                buckets[bucket_key] = buckets.get(bucket_key, 0) + 1
                # So sánh bucket_key (str int) với cutoff_key (str int) — cùng hệ
                return {k: v for k, v in buckets.items() if int(k) >= int(cutoff_key)}

            ewma_speed      = prior["ewma_speed"]
            ewma_counts     = prior["ewma_counts"]
            ewma_confidence = prior["ewma_confidence"]
            bc_gps          = prior["bucket_counts_gps"]
            bc_cam          = prior["bucket_counts_cam"]

            if src_type == "gps":
                bc_gps     = update_bucket(bc_gps)
                # [FIX #2] GPS dùng GPS_VOLUME_THRESHOLD
                alpha_gps  = compute_alpha(sum(bc_gps.values()), GPS_VOLUME_THRESHOLD)
                speed      = msg["speed_sum"]
                confidence = msg["confidence_sum"]
                ewma_speed = (
                    alpha_gps * speed + (1 - alpha_gps) * ewma_speed
                    if ewma_speed is not None else speed
                )
                ewma_confidence = (
                    alpha_gps * confidence + (1 - alpha_gps) * ewma_confidence
                    if ewma_confidence is not None else confidence
                )
            else:  # camera
                bc_cam    = update_bucket(bc_cam)
                # [FIX #2] Camera dùng CAM_VOLUME_THRESHOLD
                alpha_cam = compute_alpha(sum(bc_cam.values()), CAM_VOLUME_THRESHOLD)
                new_counts = msg["vehicle_counts"]
                all_keys   = set(ewma_counts.keys()) | set(new_counts.keys())
                ewma_counts = {
                    k: (
                        alpha_cam * new_counts.get(k, 0) + (1 - alpha_cam) * ewma_counts.get(k, 0)
                        if k in ewma_counts
                        else float(new_counts.get(k, 0))
                    )
                    for k in all_keys
                }

            self.ewma_state.update(json.dumps({
                "ewma_speed":        ewma_speed,
                "ewma_counts":       ewma_counts,
                "ewma_confidence":   ewma_confidence,
                "bucket_counts_gps": bc_gps,
                "bucket_counts_cam": bc_cam,
                "way_id":            prior.get("way_id") or msg.get("way_id"),
                "last_timestamp":    ts,
            }))

            if not self.timer_registered.value():
                next_tick = (
                    ctx.timer_service().current_processing_time()
                    // EMIT_INTERVAL_MS + 1
                ) * EMIT_INTERVAL_MS
                ctx.timer_service().register_processing_time_timer(next_tick)
                self.timer_registered.update(True)

        def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
            raw = self.ewma_state.value()
            if not raw:
                self.timer_registered.clear()
                return

            s = json.loads(raw)

            rounded_counts   = {k: round(v) for k, v in s["ewma_counts"].items()}
            total_vehicles   = sum(rounded_counts.values())
            gps_sample_count = sum(s["bucket_counts_gps"].values())
            cam_sample_count = sum(s["bucket_counts_cam"].values())

            # freshness_seconds = pipeline lag (processing_time - event_time).
            # Đo backlog của Flink, KHÔNG phải sensor offline.
            # Staleness do sensor offline được xử lý tại downstream qua Redis TTL:
            #   SET edge:<id> <payload> EX 120
            last_ts = s.get("last_timestamp")
            freshness_seconds = (
                round((timestamp - last_ts) / 1000, 1)
                if last_ts else None
            )

            yield json.dumps({
                "edge_id":           ctx.get_current_key(),
                "way_id":            s.get("way_id"),
                "timestamp":         last_ts,
                "timestamp_s":       ms_to_str(last_ts) if last_ts else None,
                "freshness_seconds": freshness_seconds,
                "camera": {
                    "vehicle_counts": rounded_counts,
                    "total_vehicles": total_vehicles,
                } if rounded_counts else None,
                "gps": {
                    "avg_speed_kmh":  round(s["ewma_speed"], 2)      if s["ewma_speed"]      is not None else None,
                    "avg_confidence": round(s["ewma_confidence"], 4) if s["ewma_confidence"] is not None else None,
                } if s["ewma_speed"] is not None else None,
                # [FIX #2] alpha riêng từng source để downstream monitor
                "alpha_gps":         round(compute_alpha(gps_sample_count, GPS_VOLUME_THRESHOLD), 3),
                "alpha_cam":         round(compute_alpha(cam_sample_count, CAM_VOLUME_THRESHOLD), 3),
                "gps_sample_count":  gps_sample_count,
                "cam_sample_count":  cam_sample_count,
            })

            self.timer_registered.clear()


    # ══════════════════════════════════════════════════════════════════════════
    #  LUỒNG PHẢI — Sliding Window Mean (traffic_agg)
    # ══════════════════════════════════════════════════════════════════════════

    class SumReduce(ReduceFunction):
        def reduce(self, a: str, b: str) -> str:
            # [FIX #3] Bỏ `isinstance(b, str)` — dead branch không bao giờ đúng.
            # ReduceFunction nhận 2 phần tử cùng type từ cùng 1 stream (str).
            # Flink không bao giờ truyền dict vào đây. Branch `else b` là chết.
            a = json.loads(a)
            b = json.loads(b)
            all_keys = set(a["vehicle_counts"].keys()) | set(b["vehicle_counts"].keys())
            return json.dumps({
                "edge_id":        a["edge_id"],
                "timestamp":      max(a["timestamp"], b["timestamp"]),
                "way_id":         a.get("way_id"),
                "vehicle_counts": {
                    k: a["vehicle_counts"].get(k, 0) + b["vehicle_counts"].get(k, 0)
                    for k in all_keys
                },
                "frame_count":    a["frame_count"]           + b["frame_count"],
                "speed_sum":      a.get("speed_sum", 0.0)    + b.get("speed_sum", 0.0),
                "confidence_sum": a.get("confidence_sum", 0.0) + b.get("confidence_sum", 0.0),
                "gps_count":      a.get("gps_count", 0)      + b.get("gps_count", 0),
            })


    class AttachWindow(ProcessWindowFunction):
        def process(self, key: str, context, elements):
            acc    = json.loads(list(elements)[0])
            window = context.window()
            frames = acc["frame_count"]
            n_gps  = acc.get("gps_count", 0)

            avg_counts = (
                {k: round(v / frames) for k, v in acc["vehicle_counts"].items()}
                if frames > 0 else {}
            )

            yield json.dumps({
                "edge_id":        key,
                "way_id":         acc.get("way_id"),
                "window_start":   window.start,
                "window_end":     window.end,
                "window_start_s": ms_to_str(window.start),
                "window_end_s":   ms_to_str(window.end),
                "camera": {
                    "vehicle_counts":   avg_counts,
                    "total_vehicles":   sum(avg_counts.values()),
                    "processed_frames": frames,
                } if frames > 0 else None,
                "gps": {
                    "avg_speed_kmh":  round(acc["speed_sum"] / n_gps, 2),
                    "avg_confidence": round(acc["confidence_sum"] / n_gps, 4),
                    "gps_ping_count": n_gps,
                } if n_gps > 0 else None,
            })


    # ── Helper ────────────────────────────────────────────────────────────────
    def build_kafka_sink(topic: str) -> KafkaSink:
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BROKER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )


    # ── Main ──────────────────────────────────────────────────────────────────
    def main():
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))

        # [FIX #5] Checkpoint config hợp lệ:
        #   interval=60s, min_pause=30s, timeout=300s.
        #   Trước đây: interval=30s, timeout=60s → nếu checkpoint mất 35s (hợp lệ
        #   vì < timeout 60s), Flink chờ xong + min_pause 10s = 45s → ngay lập tức
        #   trigger checkpoint mới (45s > interval 30s). Dưới tải nặng, checkpoint
        #   kéo dài liên tục, job không bao giờ đạt trạng thái ổn định.
        #   Giờ: timeout (300s) >> interval (60s) → checkpoint có đủ thời gian hoàn
        #   thành; min_pause (30s) đảm bảo job được nghỉ giữa các checkpoint.
        env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
        env.get_checkpoint_config().set_min_pause_between_checkpoints(CHECKPOINT_MIN_PAUSE_MS)
        env.get_checkpoint_config().set_checkpoint_timeout(CHECKPOINT_TIMEOUT_MS)
        env.set_state_backend(EmbeddedRocksDBStateBackend())

        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BROKER)
            .set_topics(TRAFFIC_EVENTS)
            .set_group_id("flink-traffic-pipeline")
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema())
            .set_property("request.timeout.ms", "30000")
            .set_property("metadata.max.age.ms", "10000")
            .build()
        )

        watermark_strategy = (
            WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(EventTimestampAssigner())
            .with_idleness(Duration.of_minutes(1))
        )

        # [FIX #4] flat_map thay cho map + filter → parse JSON 1 lần duy nhất
        parsed = (
            env
            .from_source(source, watermark_strategy, "Kafka vehicle_counts")
            .flat_map(ParseAndFilterMessage(), output_type=Types.STRING())
        )

        # [FIX #6] Dedup key_by(edge_id) + MapState → bounded state per edge.
        # Trước đây key_by(dedup_key) tạo 1 Flink key per fingerprint → unbounded
        # cardinality → OOM. Giờ mỗi edge có 1 key với MapState tối đa ~26 entry.
        deduped = (
            parsed
            .key_by(lambda x: json.loads(x)["edge_id"])
            .process(DeduplicateFunction(), output_type=Types.STRING())
        )

        # ── Luồng trái: Adaptive EWMA ─────────────────────────────────────────
        realtime = (
            deduped
            .key_by(lambda x: json.loads(x)["edge_id"])
            .process(AdaptiveEwmaFunction(), output_type=Types.STRING())
        )
        realtime.sink_to(build_kafka_sink(REALTIME_TOPIC))
        realtime.print()

        # ── Luồng phải: Sliding Window Mean ──────────────────────────────────
        agg = (
            deduped
            .key_by(lambda x: json.loads(x)["edge_id"])
            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(90)))
            .reduce(SumReduce(), window_function=AttachWindow(), output_type=Types.STRING())
        )
        agg.sink_to(build_kafka_sink(AGG_TOPIC))
        agg.print()

        env.execute("traffic_dual_pipeline")


    if __name__ == "__main__":
        main()