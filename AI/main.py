import cv2
import os
from src.core.detector import TrafficDetector
from src.core import metrics
from src.data import config_loader

def main():
    # 1. Khởi tạo cấu hình và tham số đầu vào
    cameraId = "camera_001"
    modelPath = os.path.join("weights", "best.pt")
    imageSource = os.path.join("test_images", "Untitled.png")

    # 2. Tải dữ liệu cấu hình từ file JSON (Mock DB)
    meuCoefficients = config_loader.getMeuCoefficients()
    cameraThresholds = config_loader.getCameraThresholds(cameraId)

    # 3. Khởi tạo bộ detector YOLO và chạy nhận diện vật thể
    detector = TrafficDetector(modelPath)
    r0 = detector.predict(imageSource, conf=0.3, save=True)

    # In thông tin chi tiết các bounding box ra màn hình terminal để kiểm tra
    detector.printResultsDetails(r0)

    # ================= KHÔNG GIAN XỬ LÝ HEAT MASK =================
    # Bước A: Tích lũy vị trí các xe vừa phát hiện vào file heatmap .npy của camera này
    metrics.updateHeatmapByCameraId(r0, cameraId)

    # Bước B: [Mô phỏng quy trình tạo mặt nạ định kỳ] 
    # Thực tế hàm này sẽ được gọi sau 1 tháng chạy hệ thống. 
    # Ở đây ta gọi luôn để hệ thống tự sinh ra file `camera_001_road_mask.npy` phục vụ bước tính toán bên dưới.
    metrics.generateAndSaveRoadMask(cameraId, threshold=1) # Tạm thời để ngưỡng = 1 để nhận diện ngay vùng có đường đi

    # Bước C: Tính toán tỷ lệ chiếm dụng thực tế dựa trên Road Mask vừa sinh ra
    preciseOccupancyRatio = metrics.calculatePreciseOccupancyRatio(r0, cameraId)
    # ==============================================================

    # 4. Tính toán các chỉ số giao thông mở rộng
    meu = metrics.calculateMotorcycleEquivalentUnit(r0, meuCoefficients)
    rawOccupancyRatio = metrics.calculateOccupancyRatio(r0) # Tính thêm diện tích thô để in báo cáo
    
    meuMax = cameraThresholds.get("meuMax", 1.0)
    orMax = cameraThresholds.get("orMax", 1.0)
    hciWeights = config_loader.getHciWeights()
    
    # Chỉ số HCI tổng hợp hiển thị trên báo cáo
    hci = metrics.calculateHybridCongestionIndex(meu, rawOccupancyRatio, meuMax, orMax, hciWeights)

    # GỌI HÀM TÍNH TRỌNG SỐ PHẠT VỚI TIỀN ĐIỀU KIỆN ROAD MASK
    trafficWeightFactor = metrics.calculateTrafficWeightFactor(
        preciseOccupancyRatio=preciseOccupancyRatio,
        r=r0,
        cameraId=cameraId,
        meuCoefficients=meuCoefficients,
        hciWeights=hciWeights,
        cameraThresholds=cameraThresholds
    )

    # ================= TẦNG LƯU TRỮ LỊCH SỬ =================
    # Gọi hàm lưu trữ lại toàn bộ telemetry phục vụ refresh max theo tháng 
    config_loader.logTrafficMetrics(
        cameraId=cameraId,
        preciseOccupancy=preciseOccupancyRatio,
        rawOccupancy=rawOccupancyRatio,
        meu=meu,
        hci=hci,
        twf=trafficWeightFactor
    )
    # ======================================================================

    # 5. Xuất báo cáo số liệu thu thập được từ camera ra console
    print("\n" + "="*50)
    print("      HỆ THỐNG ITS - BÁO CÁO MẬT ĐỘ GIAO THÔNG CẢI TIẾN")
    print("="*50)
    print(f"Mã số Camera                    : {cameraId}")
    print(f"Mật độ lòng đường (Road Mask)   : {preciseOccupancyRatio:.2f}% (Tiền điều kiện)")
    print(f"Tỷ lệ diện tích thô (Bbox thô)  : {rawOccupancyRatio:.2f}% (Bao gồm overlap)")
    print(f"Tổng tải trọng quy đổi (MEU)     : {meu:.2f}")
    print(f"Chỉ số ùn tắc tổng hợp (HCI)     : {hci:.2f}")
    print(f"--------------------------------------------------")
    print(f"TRỌNG SỐ ĐIỀU HƯỚNG ĐỊNH TUYẾN (TWF): {trafficWeightFactor:.2f}")
    print("="*50)

    # 6. Vẽ bounding box và hiển thị kết quả trực quan bằng cửa sổ OpenCV
    resImage = r0.plot()
    cv2.imshow('ITS - Traffic Analysis Platform', resImage)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()