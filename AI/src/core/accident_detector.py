from ultralytics import YOLO

INCIDENT_CLASSES = {"human_incident", "vehicle_incident"}


class AccidentDetector:
    def __init__(self, modelPath):
        """Khởi tạo mô hình YOLO nhận diện tai nạn (human_incident/vehicle_incident)."""
        self.model = YOLO(modelPath)

    def predict(self, imageSource, conf=0.4, save=False):
        """Thực hiện nhận diện vật thể trên nguồn ảnh đầu vào."""
        results = self.model.predict(source=imageSource, conf=conf, save=save)
        return results[0]

    @staticmethod
    def extractIncidentBoxes(result, incidentClasses=INCIDENT_CLASSES):
        """Lọc các box có class thuộc nhóm incident (human_incident/vehicle_incident),
        bỏ qua human_normal/vehicle_normal."""
        boxes = getattr(result, "boxes", None)
        if boxes is None or len(boxes) == 0:
            return []

        names = result.names
        confs = boxes.conf.cpu().numpy()
        clsIds = boxes.cls.cpu().numpy().astype(int)

        xyxy = boxes.xyxy.cpu().numpy()

        incidents = []
        for i in range(len(boxes)):
            className = names.get(int(clsIds[i]), str(clsIds[i]))
            if className in incidentClasses:
                x1, y1, x2, y2 = xyxy[i]
                incidents.append({
                    "className": className,
                    "conf": float(confs[i]),
                    "bbox": [float(x1), float(y1), float(x2), float(y2)],
                })
        return incidents
