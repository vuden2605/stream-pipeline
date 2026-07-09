# Intelligent Transportation System (ITS) - Smart Traffic Density Analysis & Routing Optimization

An advanced Intelligent Transportation System (ITS) tailored for highly dynamic urban traffic environments (such as Ho Chi Minh City, Vietnam). This project leverages State-of-the-Art (SOTA) Object Detection models (`YOLO`) to monitor real-time vehicle loads, extract precise spatial metrics, and dynamically calculate routing weight penalization factors to alleviate urban congestion.

---

## 📌 Project Overview & Core Features

This system goes beyond basic object detection by translating raw bounding box coordinates into deterministic traffic-engineering indices:

- **Dynamic Route Optimization:** Calculates adaptive traffic weight factors to feed graph-routing algorithms (e.g., Dijkstra, A*), helping vehicles bypass bottlenecks.
- **Precise Road Masking (Heatmap-Driven):** Filters out background noise (sidewalks, buildings) and resolves bounding box overlap errors via a time-accumulated pixel historical heatmap.
- **Motorcycle Equivalent Unit (MEU) Conversion:** Standardizes mixed traffic flows by converting various vehicle types into equivalent motorcycle units, matching local traffic characteristics.
- **Traffic Weight Factor (TWF):** Occupancy-gated, ramp-smoothed density factor fed directly into a Greenshields speed model — anchors the congestion decision to image-measured road occupancy rather than historically-calibrated `MEU_max`, so an under-calibrated `MEU_max` (a road that has never actually jammed in the calibration data) can no longer trigger false congestion alarms.

---

## 🛠 Project Architecture & Directory Structure

The system is built on a modular, decoupled architecture (**Separation of Concerns**), making it seamless to transition from local file-based configurations to production-grade databases (e.g., PostgreSQL, MongoDB) in the future.

```text
traffic_its_project/
│
├── config/                         # Configuration layer (Mock Database)
│   ├── meu_coefficients.json       # MEU vehicle conversion factors
│   ├── hci_weights.json            # Weighted balance between payload and space (w1, w2) — legacy, unused by TWF
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
│   │   └── metrics.py              # Computational formulas (MEU, Road Masking, TWF; HCI kept as unused legacy helper)
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

The core engine anchors the congestion decision to **image-measured road occupancy** (via the Road Mask), not to historically-calibrated `MEU_max` — this is what prevents false congestion alarms when `MEU_max` under-represents a road's true capacity (e.g. a road that never actually jammed during calibration).

### Mathematical Formulation & Logic Tier

| Algorithmic Stage | Formula / Logical Implementation | Engineering Significance |
| --- | --- | --- |
| **1. Spatial Accumulation** | $Heatmap_{(x,y)} = \sum_{t=1}^{T} Bbox_{(x,y,t)}$ | Continuously registers pixels occupied by vehicles to isolate active driving lanes over time. |
| **2. Road Mask Extraction** | $Mask_{(x,y)} = \begin{cases} 1 & \text{if } Heatmap_{(x,y)} \ge \text{threshold} \\ 0 & \text{otherwise} \end{cases}$ | Generates a binary mask of actual road surfaces, eliminating false positives on sidewalks or parking lots. |
| **3. Precise Occupancy** | $OR_{precise} = \frac{\sum (CurrentVehicleMask \cap Mask_{road})}{\sum Mask_{road}} \times 100\%$ | **Gatekeeper:** Measures exact road coverage from the image itself — independent of `MEU_max` calibration. Drives the TWF ramp below. |
| **4. Volumetric Payload** | $MEU = \sum_{i=1}^{N} Coefficient_{(Class\_ID_i)}$ | Converts diverse vehicle classes into a standardized unit; only evaluated once $OR_{precise} > 70\%$. |
| **5. Density (clamped)** | $Density = \min\left(\frac{MEU}{MEU_{max}}, 1.0\right)$ | Clamped to 1.0 so an under-calibrated `MEU_max` can't push density above 1.0 (which would make Greenshields' raw speed negative). |

### Traffic Weight Factor (TWF) — occupancy gate + linear ramp

`calculateTrafficWeightFactor` (`src/core/metrics.py`) maps `OR_precise` to `TWF` with a buffered transition around the 70–90% band, instead of a hard step, so `raw_speed` doesn't jitter when occupancy oscillates slightly across frames:

- $OR_{precise} \le 70\%$ (free-flowing): $TWF = 0.0$.
- $OR_{precise} \ge 90\%$ (fully saturated): $TWF = Density = \min(MEU / MEU_{max}, 1.0)$.
- $70\% < OR_{precise} < 90\%$ (buffer zone): $TWF = rampFactor \times Density$, where $rampFactor = (OR_{precise} - 70) / 20$.

`TWF` is then plugged **directly as density** into a Greenshields speed model (see `worker.py` in the repo root): $raw\_speed = free\_flow \times (1 - TWF^{N})$ — it is not used as a routing-cost multiplier. The old HCI-based tiered cost mapping (0.1× / 0.5× / 1.0×) has been retired; `calculateHybridCongestionIndex` remains in `metrics.py` as an unused legacy helper.

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
Tổng tải trọng quy đổi (MEU)     : 142.36
--------------------------------------------------
TRỌNG SỐ ĐIỀU HƯỚNG ĐỊNH TUYẾN (TWF): 0.87
==================================================
```

(Sample TWF above assumes `occupancy=87.42%` falls in the 70–90% ramp band, so `TWF = rampFactor × min(MEU/MEU_max, 1.0)` rather than the raw MEU/MEU_max ratio.)

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
