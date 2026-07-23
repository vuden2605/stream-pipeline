from ultralytics import YOLO


# 1. Load file best.pt của bạn
model = YOLO(r'weights\best.pt')

# 2. Chạy nhận diện cho toàn bộ ảnh trong thư mục nguồn
sourceDir = r'image_of_accident'
outputDir = r'accident_output'

model.predict(
	source=sourceDir,
	conf=0.3,
	save=True,
	project=outputDir,
	name='predict',
	exist_ok=True
)

