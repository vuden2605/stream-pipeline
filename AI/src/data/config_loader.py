import json
import os
import csv
from datetime import datetime

# Neo tuyệt đối vào thư mục AI/ — bắt buộc vì module này được import từ
# consumer_ai.py chạy ở root repo (cwd khác AI/), đường dẫn tương đối sẽ vỡ.
CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "config")

def loadJsonConfig(fileName):
    """Đọc dữ liệu thô từ file cấu hình JSON."""
    filePath = os.path.join(CONFIG_DIR, fileName)
    if not os.path.exists(filePath):
        return {}
    with open(filePath, 'r', encoding='utf-8') as f:
        return json.load(f)

def getMeuCoefficients():
    """Lấy hệ số MEU và ép kiểu Key sang kiểu int để khớp với lớp của YOLO."""
    data = loadJsonConfig("meu_coefficients.json")
    return {int(k): v for k, v in data.items()}

def getHciWeights():
    """Lấy trọng số w1, w2 phục vụ tính chỉ số HCI."""
    return loadJsonConfig("hci_weights.json")

def getCameraThresholds(cameraId):
    """Lấy các giá trị ngưỡng kịch trần (meuMax, orMax, hciMax) của một camera cụ thể."""
    data = loadJsonConfig("camera_thresholds.json")
    # Giá trị mặc định dự phòng nếu không tìm thấy cameraId trong file cấu hình
    defaultThresholds = {"meuMax": 1.0, "orMax": 1.0, "hciMax": 1.0}
    return data.get(cameraId, defaultThresholds)


def logTrafficMetrics(cameraId, preciseOccupancy, rawOccupancy, meu, hci, twf):
    """
    Hàm lưu trữ lại các chỉ số giao thông sau mỗi lần predict vào file CSV lịch sử.
    Tự động tạo file và chèn hàng tiêu đề (Header) nếu file chạy lần đầu tiên.
    """
    logFilePath = os.path.join(CONFIG_DIR, "traffic_history_logs.csv")
    
    # Kiểm tra xem file đã tồn tại chưa để quyết định có viết hàng tiêu đề hay không
    fileExists = os.path.exists(logFilePath)
    
    # Lấy timestamp thời gian thực hiện tại của hệ thống
    currentTimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Chuẩn bị dòng dữ liệu để ghi
    logData = {
        "timestamp": currentTimestamp,
        "cameraId": cameraId,
        "preciseOccupancy": round(preciseOccupancy, 2),
        "rawOccupancy": round(rawOccupancy, 2),
        "meu": round(meu, 2),
        "hci": round(hci, 2),
        "twf": round(twf, 2)
    }
    
    fieldnames = ["timestamp", "cameraId", "preciseOccupancy", "rawOccupancy", "meu", "hci", "twf"]
    
    # Mở file ở chế độ 'a' (Append - ghi tiếp vào cuối file), mã hóa utf-8
    with open(logFilePath, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # Nếu file mới tinh, tiến hành ghi hàng tiêu đề cột trước
        if not fileExists:
            writer.writeheader()
            
        # Ghi dòng dữ liệu chỉ số vào file
        writer.writerow(logData)
        
    print(f"[Dữ liệu] Đã lưu log lịch sử thành công vào file: {logFilePath}")