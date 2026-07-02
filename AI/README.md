# Intelligent Transportation System (ITS) - Smart Traffic Density Analysis & Routing Optimization

An advanced Intelligent Transportation System (ITS) tailored for highly dynamic urban traffic environments (such as Ho Chi Minh City, Vietnam). This project leverages State-of-the-Art (SOTA) Object Detection models (`YOLO`) to monitor real-time vehicle loads, extract precise spatial metrics, and dynamically calculate routing weight penalization factors to alleviate urban congestion.

---

## 📌 Project Overview & Core Features

This system goes beyond basic object detection by translating raw bounding box coordinates into deterministic traffic-engineering indices:

- **Dynamic Route Optimization:** Calculates adaptive traffic weight factors to feed graph-routing algorithms (e.g., Dijkstra, A*), helping vehicles bypass bottlenecks.
- **Precise Road Masking (Heatmap-Driven):** Filters out background noise (sidewalks, buildings) and resolves bounding box overlap errors via a time-accumulated pixel historical heatmap.
- **Motorcycle Equivalent Unit (MEU) Conversion:** Standardizes mixed traffic flows by converting various vehicle types into equivalent motorcycle units, matching local traffic characteristics.
- **Hybrid Congestion Index (HCI):** Synthesizes physical space occupation and volumetric vehicular payload to score real-time congestion levels.

---

## 🛠 Project Architecture & Directory Structure

The system is built on a modular, decoupled architecture (**Separation of Concerns**), making it seamless to transition from local file-based configurations to production-grade databases (e.g., PostgreSQL, MongoDB) in the future.

```text
traffic_its_project/
│
├── config/                         # Configuration layer (Mock Database)
│   ├── meu_coefficients.json       # MEU vehicle conversion factors
│   ├── hci_weights.json            # Weighted balance between payload and space (w1, w2)
│   └── camera_thresholds.json      # Historical maximum capacities per Camera ID
│
├── storage/                        # Persistent file storage
│   └── camera_matrices/            # Accumulated .npy binary matrices (Heatmaps & Road Masks)
│
├── src/                            # System Source Code
│   ├── __init__.py
│   ├── core/                       # Computer Vision & Mathematical Engine
│   │   ├── __init__.py
│   │   ├── detector.py             # YOLO wrapper class for inferencing and telemetry
│   │   └── metrics.py              # Computational formulas (MEU, Road Masking, HCI, TWF)
│   │
│   └── data/                       # Data Access Layer
│       ├── __init__.py
│       └── config_loader.py        # I/O handler for configuration profiles
│
├── weights/                        # Deep Learning Model Weights
│   └── best.pt                     # Customized trained YOLO model weights
│
├── test_images/                    # Sandbox directory for pipeline validation
│   └── Untitled.png
│
└── main.py                         # Application Entry Point & Orchestrator
```

---

## 🧠 Algorithmic Framework & Methodology

The core engine deploys a dual-tier cascade validation process to determine traffic routing penalties accurately.

### Mathematical Formulation & Logic Tier

| Algorithmic Stage | Formula / Logical Implementation | Engineering Significance |
| --- | --- | --- |
| **1. Spatial Accumulation** | $Heatmap_{(x,y)} = \sum_{t=1}^{T} Bbox_{(x,y,t)}$ | Continuously registers pixels occupied by vehicles to isolate active driving lanes over time. |
| **2. Road Mask Extraction** | $Mask_{(x,y)} = \begin{cases} 1 & \text{if } Heatmap_{(x,y)} \ge \text{threshold} \\ 0 & \text{otherwise} \end{cases}$ | Generates a binary mask of actual road surfaces, eliminating false positives on sidewalks or parking lots. |
| **3. Precise Occupancy** | $OR_{precise} = \frac{\sum (CurrentVehicleMask \cap Mask_{road})}{\sum Mask_{road}} \times 100\%$ | **Tier 1 (Gatekeeper Control):** Measures exact road coverage. If $OR_{precise} < 85\%$, traffic flows freely; routing penalties are skipped ($TWF = 1.0$). |
| **4. Volumetric Payload** | $MEU = \sum_{i=1}^{N} Coefficient_{(Class\_ID_i)}$ | Converts diverse vehicle classes into a standardized unit based on actual road space displacement. |
| **5. Hybrid Congestion** | $HCI = w_1 \cdot \left(\frac{MEU}{MEU_{max}}\right) + w_2 \cdot \left(\frac{OR_{raw}}{OR_{max}}\right)$ | **Tier 2 (Triggered at $\ge 85\%$):** Evaluates overlapping bounding boxes ($OR_{raw}$) alongside MEU to gauge precise bumper-to-bumper density. |

### Traffic Penalty Mapping (Traffic Weight Factor - TWF)

When the road mask occupancy threshold hits the **$\ge 85\%$ critical gatekeeper barrier**, the final routing cost modifier ($TWF$) is generated based on the normalized HCI ratio ($HCI / HCI_{max}$):

- $\text{Ratio} \ge 0.9$ (Severe Gridlock): $TWF = 0.1$ (Signals graph algorithms to multiply routing cost by 10× to bypass this edge).
- $\text{Ratio} \ge 0.8$ (Moderate Congestion): $TWF = 0.5$ (Doubles the routing cost).
- $\text{Ratio} < 0.8$ (Normal/Stable Flow): $TWF = 1.0$ (Standard edge cost).

---

## 🚀 Getting Started & Execution Guide

### 1. Prerequisites & Environment Setup

Ensure you have Python **3.9+** installed. Clone this repository and install the official dependencies via terminal:

```bash
pip install ultralytics opencv-python numpy
```

### 2. Configure Mock Database (Local Profiles)

Before running the system, declare your parameters inside `config/camera_thresholds.json` corresponding to your camera metadata:

```json
{
  "camera_001": {
    "meuMax": 200.0,
    "orMax": 35.0,
    "hciMax": 1.0
  }
}
```

### 3. Running the Pipeline

Place your target test image inside `test_images/` and configure your target file paths in `main.py`.

Execute the system entry point:

```bash
python main.py
```

### 4. What Happens Under the Hood

1. **Inference:** The system initializes `src/core/detector.py`, loading the optimized `weights/best.pt` file to evaluate the traffic frame.
2. **Matrix I/O:** It checks `storage/camera_matrices/` for existing spatial configurations. If absent, it creates a new matrix map natively synchronized with the target resolution.
3. **Telemetry Report:** A standardized real-time ITS structural telemetry log outputs directly onto your CLI console.

```text
==================================================
      HỆ THỐNG ITS - BÁO CÁO MẬT ĐỘ GIAO THÔNG CẢI TIẾN
==================================================
Mã số Camera                    : camera_001
Mật độ lòng đường (Road Mask)   : 87.42% (Tiền điều kiện)
Tỷ lệ diện tích thô (Bbox thô)  : 94.15% (Bao gồm overlap)
Tổng tải trọng quy đổi (MEU)     : 142.36
Chỉ số ùn tắc tổng hợp (HCI)     : 0.89
--------------------------------------------------
TRỌNG SỐ ĐIỀU HƯỚNG ĐỊNH TUYẾN (TWF): 0.50
==================================================
```

---

## 🔮 Future Database Integration Roadmap

To upgrade this system to a centralized server structure, modify only the data access interface layer (`src/data/config_loader.py`):

```python
# Change this JSON parsing layer:
def loadJsonConfig(fileName):
    ...

# Into an active database ORM session connector:
def loadDatabaseConfig(cameraId):
    # return db.query(CameraSchema).filter_by(id=cameraId).first()
    pass
```

No modification will be required across any of the core logical scripts inside `src/core/`, preserving system integrity.
