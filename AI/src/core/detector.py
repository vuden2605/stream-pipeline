from ultralytics import YOLO

class TrafficDetector:
    def __init__(self, modelPath):
        """Khởi tạo mô hình YOLO nhận diện phương tiện giao thông."""
        self.model = YOLO(modelPath)

    def predict(self, imageSource, conf=0.3, save=True):
        """Thực hiện nhận diện vật thể trên nguồn ảnh đầu vào."""
        results = self.model.predict(source=imageSource, conf=conf, save=save)
        return results[0]

    def predict_batch(self, images: list, conf: float = 0.3):
        """Batch inference — trả list Results cùng thứ tự input."""
        if not images:
            return []
        return self.model.predict(source=images, conf=conf, save=False)

    @staticmethod
    def printResultsDetails(r):
        """In thông tin chi tiết các trường dữ liệu và bounding box thu được."""
        print("\n=== Results fields ===")
        commonFields = ["path", "orig_shape", "names", "save_dir", "speed", "boxes", "masks", "keypoints", "probs", "obb"]
        for f in commonFields:
            if hasattr(r, f):
                v = getattr(r, f)
                if f == "names" and isinstance(v, dict):
                    print(f"{f}: dict (num_classes={len(v)})")
                elif f == "boxes" and v is not None:
                    print(f"{f}: {type(v).__name__} (num_boxes={len(v)})")
                else:
                    print(f"{f}: {type(v).__name__}")

        if getattr(r, "boxes", None) is None or len(r.boxes) == 0:
            print("\n=== Detections ===\nNo detections")
            return

        boxes = r.boxes
        xyxy = boxes.xyxy.cpu().numpy()
        conf = boxes.conf.cpu().numpy()
        cls = boxes.cls.cpu().numpy().astype(int)

        print("\n=== Detections ===")
        for i in range(len(boxes)):
            classId = int(cls[i])
            className = r.names.get(classId, str(classId)) if isinstance(getattr(r, "names", None), dict) else str(classId)
            x1, y1, x2, y2 = xyxy[i]
            print(f"[{i}] cls={classId} ({className}) conf={conf[i]:.3f} xyxy=({x1:.1f}, {y1:.1f}, {x2:.1f}, {y2:.1f})")