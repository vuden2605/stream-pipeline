from ultralytics import YOLO
import cv2

# 1. Load file best.pt của bạn
model = YOLO(r'best.pt')

# 2. Nhận diện ảnh bất kỳ
results = model.predict(
    source=r'image/cam_09_00509.jpg',
    conf=0.3,
    save=True
)

# 3. Hiện ảnh kết quả ngay lập tức
res_image = results[0].plot()

# Hiển thị ảnh trên máy local
cv2.imshow('Result', res_image)
cv2.waitKey(0)  # Đợi bạn nhấn phím bất kỳ để đóng ảnh
cv2.destroyAllWindows()