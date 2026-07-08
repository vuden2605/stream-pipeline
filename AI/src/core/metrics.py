import numpy as np
import cv2
import os

# Đường dẫn vật lý lưu trữ ma trận file .npy — neo tuyệt đối vào thư mục AI/
# (cùng lý do với CONFIG_DIR trong config_loader.py: module này được import
# từ consumer_ai.py chạy ở root repo, khác cwd với AI/).
_AI_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MATRIX_STORAGE_DIR = os.path.join(_AI_ROOT, "storage", "camera_matrices")
if not os.path.exists(MATRIX_STORAGE_DIR):
    os.makedirs(MATRIX_STORAGE_DIR)

def updateHeatmapByCameraId(r, cameraId):
    """
    Hàm cập nhật tích lũy vị trí xe bằng cách đọc/ghi trực tiếp từ file .npy.
    - Nếu chưa tồn tại file: Khởi tạo mảng 0 theo kích thước ảnh thật rồi cộng dồn.
    - Nếu đã tồn tại file: Tải mảng cũ lên để tiếp tục tích lũy.
    """
    if getattr(r, "boxes", None) is None:
        return None

    imgH, imgW = r.orig_shape
    filePath = os.path.join(MATRIX_STORAGE_DIR, f"{cameraId}_heatmap.npy")
    
    # 1. Kiểm tra file heatmap đã tồn tại chưa để nạp hoặc khởi tạo mới
    if os.path.exists(filePath):
        heatmapMatrix = np.load(filePath)
    else:
        print(f"[Heat Mask] Khởi tạo ma trận Heatmap mới cho {cameraId} kích thước ({imgH}, {imgW})")
        heatmapMatrix = np.zeros((imgH, imgW), dtype=np.int32)

    # 2. Duyệt qua các bounding box để tích lũy vị trí xe
    boxes = r.boxes.xyxy.cpu().numpy()
    for x1, y1, x2, y2 in boxes:
        x1Int = max(0, min(int(round(x1)), imgW - 1))
        x2Int = max(0, min(int(round(x2)), imgW - 1))
        y1Int = max(0, min(int(round(y1)), imgH - 1))
        y2Int = max(0, min(int(round(y2)), imgH - 1))

        if x2Int > x1Int and y2Int > y1Int:
            heatmapMatrix[y1Int:y2Int, x1Int:x2Int] += 1
            
    # 3. Ghi đè cập nhật lại vào file vật lý
    np.save(filePath, heatmapMatrix)
    return heatmapMatrix


def generateAndSaveRoadMask(cameraId, threshold=100):
    """
    Hàm chuyển đổi ma trận heatmap lịch sử thành mặt nạ mặt đường nhị phân (Road Mask).
    Pixel nào có lượt xe đè lên >= threshold sẽ được coi là mặt đường (giá trị 1), ngược lại là 0.
    """
    heatmapPath = os.path.join(MATRIX_STORAGE_DIR, f"{cameraId}_heatmap.npy")
    maskPath = os.path.join(MATRIX_STORAGE_DIR, f"{cameraId}_road_mask.npy")
    
    if not os.path.exists(heatmapPath):
        print(f"[Lỗi] Chưa có file heatmap của {cameraId} để trích xuất Road Mask.")
        return None
        
    heatmapMatrix = np.load(heatmapPath)
    
    # Thực hiện ngưỡng hóa nhị phân bằng numpy
    roadMaskMatrix = np.where(heatmapMatrix >= threshold, 1, 0).astype(np.uint8)
    
    # Lưu mặt nạ đường đi xuống ổ cứng
    np.save(maskPath, roadMaskMatrix)
    print(f"[Heat Mask] Đã trích xuất và lưu Road Mask cho {cameraId} với ngưỡng >= {threshold}")
    return roadMaskMatrix


def getRoadMaskByCameraId(cameraId, origShape):
    """
    Hàm lấy mặt nạ mặt đường từ file npy của camera cụ thể.
    Nếu chưa chạy đủ lâu để sinh file Mask, tạm thời coi toàn bộ ảnh là mặt đường (mảng toàn số 1).
    """
    filePath = os.path.join(MATRIX_STORAGE_DIR, f"{cameraId}_road_mask.npy")
    if os.path.exists(filePath):
        return np.load(filePath)
        
    # Phương án dự phòng (Fallback): Trả về mảng toàn 1 nếu chưa trích xuất mask lịch sử
    imgH, imgW = origShape
    return np.ones((imgH, imgW), dtype=np.uint8)


def calculatePreciseOccupancyRatio(r, cameraId):
    """
    Hàm tính tỷ lệ phần trăm chiếm dụng thực tế của xe CHỈ TRÊN VÙNG MẶT ĐƯỜNG (Road Mask).
    Giải quyết triệt để lỗi xe đè nhau (Overlap) và không gian nhiễu ngoài vỉa hè.
    """
    if getattr(r, "boxes", None) is None or len(r.boxes) == 0:
        return 0.0

    imgH, imgW = r.orig_shape
    
    # 1. Lấy mặt nạ mặt đường chuẩn của riêng camera này
    roadMaskMatrix = getRoadMaskByCameraId(cameraId, (imgH, imgW))
    totalRoadPixels = np.sum(roadMaskMatrix)
    
    if totalRoadPixels <= 0:
        return 0.0

    # 2. Tạo một mặt nạ rỗng cho frame hiện tại để vẽ các xe đang xuất hiện lên
    currentFrameVehicleMask = np.zeros((imgH, imgW), dtype=np.uint8)
    boxes = r.boxes.xyxy.cpu().numpy()

    for x1, y1, x2, y2 in boxes:
        x1Int = max(0, min(int(round(x1)), imgW - 1))
        x2Int = max(0, min(int(round(x2)), imgW - 1))
        y1Int = max(0, min(int(round(y1)), imgH - 1))
        y2Int = max(0, min(int(round(y2)), imgH - 1))
        
        if x2Int > x1Int and y2Int > y1Int:
            # Tô pixel vùng có xe bằng 1 (Dù xe máy đè sát nhau thì pixel đó vẫn chỉ nhận giá trị 1)
            currentFrameVehicleMask[y1Int:y2Int, x1Int:x2Int] = 1

    # 3. Sử dụng phép toán logic AND ảnh để lọc bỏ các xe nằm ngoài lòng đường (xe trên vỉa hè, bãi xe)
    vehiclesOnRoadMask = cv2.bitwise_and(currentFrameVehicleMask, roadMaskMatrix)
    
    # 4. Tính toán phần trăm diện tích chiếm dụng thực tế
    occupiedRoadPixels = np.sum(vehiclesOnRoadMask)
    occupancyRatio = (occupiedRoadPixels / totalRoadPixels) * 100.0
    return occupancyRatio

def calculateOccupancyRatio(r):
    """Tính toán tỷ lệ phần trăm diện tích bounding box chiếm dụng trên tổng ảnh."""
    if getattr(r, "boxes", None) is None or len(r.boxes) == 0:
        return 0.0

    imgH, imgW = r.orig_shape
    if imgH <= 0 or imgW <= 0:
        return 0.0

    boxes = r.boxes.xyxy.cpu().numpy()
    totalArea = 0.0
    for x1, y1, x2, y2 in boxes:
        x1 = max(0.0, min(float(x1), imgW))
        x2 = max(0.0, min(float(x2), imgW))
        y1 = max(0.0, min(float(y1), imgH))
        y2 = max(0.0, min(float(y2), imgH))
        w = x2 - x1
        h = y2 - y1
        if w > 0 and h > 0:
            totalArea += w * h

    return (totalArea / (imgW * imgH)) * 100.0


def calculateMotorcycleEquivalentUnit(r, meuCoefficients):
    """Quy đổi tổng số phương tiện phát hiện được về Đơn vị xe máy tương đương (MEU)."""
    if getattr(r, "boxes", None) is None or len(r.boxes) == 0:
        return 0.0

    cls = r.boxes.cls.cpu().numpy().astype(int)
    meu = 0.0
    for classId in cls:
        meu += meuCoefficients.get(int(classId), 0)
    return meu


def calculateHybridCongestionIndex(meu, occupancyRatio, meuMax, orMax, weights):
    """Tính toán chỉ số kẹt xe hỗn hợp HCI."""
    if meuMax <= 0 or orMax <= 0:
        return 0.0
    return (weights["w1"] * (meu / meuMax)) + (weights["w2"] * (occupancyRatio / orMax))


# def calculateTrafficWeightFactor(preciseOccupancyRatio, r, cameraId, meuCoefficients, hciWeights, cameraThresholds):
#     """
#     Hàm tính toán hệ số trọng số đường đi (Traffic Weight Factor) ứng dụng tiền điều kiện Road Mask.
#     - Tiền điều kiện: preciseOccupancyRatio phải >= 85% mới xét phạt.
#     - Dưới 85%: Bỏ qua hoàn toàn, trả về trọng số bình thường (1.0).
#     """
#     # --------------------------------------------------------------------------
#     # BƯỚC 1: KIỂM TRA TIỀN ĐIỀU KIỆN (GATEKEEPER)
#     # --------------------------------------------------------------------------
#     if preciseOccupancyRatio < 85.0:
#         print(f"[Thuật toán] Mật độ đường thực tế ({preciseOccupancyRatio:.2f}%) < 85%. Tuyến đường thông thoáng -> Bỏ qua phạt.")
#         return 1.0  # Trả về ngay trọng số bình thường, thuật toán định tuyến giữ nguyên chi phí đường

#     print(f"[Thuật toán] 🚨 CẢNH BÁO: Mật độ đường thực tế ({preciseOccupancyRatio:.2f}%) >= 85%!")
#     print(f"[Thuật toán] Kích hoạt Tầng 2: Tính toán trọng số phạt dựa trên diện tích thô và tải trọng MEU...")

#     # --------------------------------------------------------------------------
#     # BƯỚC 2: TÍNH TOÁN KHI ĐÃ ĐẠT TIỀN ĐIỀU KIỆN
#     # --------------------------------------------------------------------------
#     # 2.1 Lấy lại hàm tính diện tích Bounding Box thô (bao gồm cả overlap xe đè nhau) từ file gốc của em
#     rawOccupancyRatio = calculateOccupancyRatio(r) 
    
#     # 2.2 Tính tổng tải trọng xe quy đổi (MEU)
#     meu = calculateMotorcycleEquivalentUnit(r, meuCoefficients)
    
#     # 2.3 Lấy các ngưỡng cấu hình lịch sử của camera
#     meuMax = cameraThresholds.get("meuMax", 1.0)
#     orMax = cameraThresholds.get("orMax", 1.0)
#     maxHci = cameraThresholds.get("hciMax", 1.0)
    
#     # 2.4 Tính chỉ số kẹt xe hỗn hợp HCI (Sử dụng rawOccupancyRatio của diện tích thô)
#     currentHci = calculateHybridCongestionIndex(meu, rawOccupancyRatio, meuMax, orMax, hciWeights)
    
#     if maxHci <= 0:
#         return 1.0

#     # 2.5 Quyết định mức độ phạt dựa trên tỷ lệ vượt ngưỡng HCI
#     ratio = currentHci / maxHci
#     if ratio >= 0.9:
#         return 0.1  # Kẹt xe rất nghiêm trọng, tăng chi phí đường lên gấp 10 lần để né đường này
#     if ratio >= 0.8:
#         return 0.5  # Ùn ứ nhẹ, tăng chi phí đường gấp đôi
        
#     return 1.0

def calculateTrafficWeightFactor(preciseOccupancyRatio, r, cameraId, meuCoefficients, cameraThresholds):
    """
    Hàm tính toán hệ số trọng số đường đi (Traffic Weight Factor) ứng dụng tiền điều kiện Road Mask.
    - Tiền điều kiện: preciseOccupancyRatio phải >= 80% mới xét phạt.
    - Dưới 80%: Bỏ qua hoàn toàn, trả về trọng số bình thường (0.0).
    """
    # --------------------------------------------------------------------------
    # BƯỚC 1: KIỂM TRA TIỀN ĐIỀU KIỆN (GATEKEEPER)
    # --------------------------------------------------------------------------
    if preciseOccupancyRatio < 80.0:
        print(f"[Thuật toán] Mật độ đường thực tế ({preciseOccupancyRatio:.2f}%) < 80%. Tuyến đường thông thoáng -> Bỏ qua phạt.")
        return 0.0  # Trả về ngay trọng số bình thường, thuật toán định tuyến giữ nguyên chi phí đường

    print(f"[Thuật toán] 🚨 CẢNH BÁO: Mật độ đường thực tế ({preciseOccupancyRatio:.2f}%) >= 80%!")
    print(f"[Thuật toán] Kích hoạt Tầng 2: Tính toán trọng số phạt dựa trên tải trọng MEU...")

    # --------------------------------------------------------------------------
    # BƯỚC 2: TÍNH TOÁN KHI ĐÃ ĐẠT TIỀN ĐIỀU KIỆN
    # --------------------------------------------------------------------------
    
    # Tính tổng tải trọng xe quy đổi (MEU)
    meu = calculateMotorcycleEquivalentUnit(r, meuCoefficients)
    
    # 2.3 Lấy các ngưỡng cấu hình lịch sử của camera
    meuMax = cameraThresholds.get("meuMax", 1.0)
    
    return meu / meuMax if meuMax > 0 else 0.0