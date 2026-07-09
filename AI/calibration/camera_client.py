import time
import logging

import requests
import urllib3

from calibration_config import BROWSER_HEADERS

# Site camera dùng chứng chỉ không hợp lệ — tắt cảnh báo lặp lại của urllib3
# cho mỗi request (giống hành vi worker.py của pipeline chính).
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("calibration.camera_client")


def is_valid_jpeg(data: bytes) -> bool:
    return len(data) > 5_000 and data[:2] == b"\xff\xd8" and data[-2:] == b"\xff\xd9"


def refresh_cookie(session: requests.Session) -> None:
    try:
        resp = session.get(
            "https://giaothong.hochiminhcity.gov.vn/",
            headers=BROWSER_HEADERS, timeout=15, verify=False,
        )
        log.info("Session cookie refreshed (HTTP %d)", resp.status_code)
    except Exception as e:
        log.warning("Cookie refresh failed: %r", e)


def fetch_snapshot(session: requests.Session, camera: dict) -> bytes | None:
    """GET snapshot trực tiếp, giữ bytes trong RAM — không upload S3."""
    url = f"{camera['url']}&t={int(time.time() * 1000)}"
    try:
        resp = session.get(url, headers=BROWSER_HEADERS, timeout=10, verify=False)
    except Exception as e:
        log.warning("[%s] fetch lỗi: %r", camera["id"], e)
        return None

    if resp.status_code != 200:
        log.warning("[%s] HTTP %d", camera["id"], resp.status_code)
        return None

    data = resp.content
    if not is_valid_jpeg(data):
        log.warning("[%s] Not JPEG", camera["id"])
        return None
    return data
