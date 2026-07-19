"""
ETL 1 lần (không realtime) cho Geometry Nguồn A — component 02 trong kiến trúc
bản đồ mật độ giao thông realtime.

Đọc cameras_with_zones_merged.json (8MB, đủ trường camera/AI/threshold không
cần cho việc vẽ bản đồ), decode sẵn field mapped_edge.edge_info.shape (polyline
Valhalla đã encode, chưa được config.py đụng tới) thành toạ độ, rồi xuất ra 1
file gọn edge_id -> geometry. File gọn này được copy/commit thủ công sang
smart-transport (resource của BE) — BE chỉ đọc 1 lần lúc khởi động, không đụng
lại file JSON gốc hay decode lại mỗi request.

Thứ tự toạ độ trong output: mỗi điểm là [lat, lon] (đúng quy ước Valhalla) —
NGƯỢC với thứ tự [lon, lat] của GeoJSON, cần lưu ý khi BE/FE tiêu thụ file này.

Chạy: python extract_geometry.py
"""
import json
import sys
from pathlib import Path

if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).parent
CAMERAS_FILE = ROOT / "cameras_with_zones_merged.json"
OUTPUT_FILE = ROOT / "edge_geometry.json"

# Bbox nới rộng quanh HCM — chỉ để cảnh báo sớm nếu lỡ decode sai precision
# (Valhalla dùng precision 6, không phải 5 kiểu Google Polyline chuẩn), không
# phải kiểm tra chặt.
HCMC_LAT_RANGE = (9.5, 11.5)
HCMC_LON_RANGE = (105.5, 107.5)


def decode_polyline(encoded: str, precision: int = 6) -> list[list[float]]:
    """Decode chuỗi polyline kiểu Valhalla (delta-encoded, precision mặc định
    6 chữ số thập phân — KHÁC Google Polyline chuẩn dùng precision 5) thành
    danh sách điểm [lat, lon]. Giữ nguyên accumulator ở dạng số nguyên trong
    lúc decode, chỉ nhân với 10^-precision lúc xuất điểm cuối để tránh cộng
    dồn sai số float qua nhiều điểm.
    """
    inv = 10 ** -precision
    decoded: list[list[float]] = []
    previous = [0, 0]
    i = 0
    length = len(encoded)
    while i < length:
        ll = [0, 0]
        for j in range(2):
            shift = 0
            byte = 0x20
            while byte >= 0x20:
                byte = ord(encoded[i]) - 63
                i += 1
                ll[j] |= (byte & 0x1F) << shift
                shift += 5
            delta = ~(ll[j] >> 1) if (ll[j] & 1) else (ll[j] >> 1)
            ll[j] = previous[j] + delta
            previous[j] = ll[j]
        decoded.append([round(previous[0] * inv, precision), round(previous[1] * inv, precision)])
    return decoded


def extract_edge_id(zone: dict) -> str | None:
    # Mirror đúng logic _extract_edge_id trong config.py — cùng 1 nguồn
    # edge_id (mapped_edge.edge_id.value = GraphId toàn cục), để edge_id ghi
    # ra file này khớp 100% với edge_id worker.py dùng để ghi Redis.
    mapped = zone.get("mapped_edge") or {}
    eid = mapped.get("edge_id") or {}
    value = eid.get("value")
    return str(value) if value is not None else None


def main() -> None:
    with open(CAMERAS_FILE, encoding="utf-8") as f:
        cameras = json.load(f)

    edge_geometry: dict[str, list[list[float]]] = {}
    zones_seen = 0
    skipped_no_edge_id = 0
    skipped_no_shape = 0
    duplicate_edge_id = 0
    out_of_range = 0

    for cam in cameras:
        for zone in cam.get("zones", {}).values():
            zones_seen += 1

            edge_id = extract_edge_id(zone)
            if edge_id is None:
                skipped_no_edge_id += 1
                continue

            shape = ((zone.get("mapped_edge") or {}).get("edge_info") or {}).get("shape")
            if not shape:
                skipped_no_shape += 1
                continue

            if edge_id in edge_geometry:
                # Nhiều camera/zone có thể cùng phủ 1 edge_id — giữ bản decode
                # đầu tiên, chỉ đếm để biết mức độ trùng lặp.
                duplicate_edge_id += 1
                continue

            points = decode_polyline(shape)
            if points:
                lat0, lon0 = points[0]
                if not (HCMC_LAT_RANGE[0] <= lat0 <= HCMC_LAT_RANGE[1]
                        and HCMC_LON_RANGE[0] <= lon0 <= HCMC_LON_RANGE[1]):
                    out_of_range += 1

            edge_geometry[edge_id] = points

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(edge_geometry, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    print(f"Camera đọc được          : {len(cameras)}")
    print(f"Zone duyệt qua           : {zones_seen}")
    print(f"Bỏ qua (thiếu edge_id)   : {skipped_no_edge_id}")
    print(f"Bỏ qua (thiếu shape)     : {skipped_no_shape}")
    print(f"Trùng edge_id (bỏ qua)   : {duplicate_edge_id}")
    print(f"Edge_id ghi ra file      : {len(edge_geometry)}")
    if out_of_range:
        print(f"CẢNH BÁO: {out_of_range} edge có điểm đầu decode ra ngoài "
              f"phạm vi HCM — nghi ngờ sai precision polyline, kiểm tra lại "
              f"trước khi dùng file output.")
    print(f"Output                   : {OUTPUT_FILE} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
