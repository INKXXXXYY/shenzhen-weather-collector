# collect_weather.py
import os
import csv
import time
import requests
from datetime import datetime
from dateutil import tz

# ===== 基本配置 =====
# 深圳市中心坐标（可改成你家/公司附近坐标）
LAT, LON = 22.543096, 114.057865
LOCATION_NAME = os.getenv("LOCATION_NAME", "深圳市").strip()

CSV_PATH = "weather_log.csv"
CN_TZ = tz.gettz("Asia/Shanghai")

# ===== 和风天气配置（key 必填才会写入；location 支持“经度,纬度”或 LocationID）=====
QWEATHER_API_KEY = os.getenv("QWEATHER_API_KEY", "").strip()
# 若未显式提供，则默认使用上面 LAT/LON 组合成 “经度,纬度”
QWEATHER_LOCATION = os.getenv("QWEATHER_LOCATION", "").strip() or f"{LON},{LAT}"
QWEATHER_LANG = os.getenv("QWEATHER_LANG", "zh").strip()
QWEATHER_UNIT = os.getenv("QWEATHER_UNIT", "m").strip()   # m=公制（默认），i=英制

# ===== Open-Meteo WMO 天气代码 → 中文描述 =====
WMO_WEATHER_CODES = {
    0: "晴", 1: "基本晴", 2: "局部多云", 3: "多云",
    45: "雾", 48: "雾凇",
    51: "小毛毛雨", 53: "中毛毛雨", 55: "大毛毛雨",
    56: "小冻毛毛雨", 57: "大冻毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    66: "小冻雨", 67: "大冻雨",
    71: "小雪", 73: "中雪", 75: "大雪",
    77: "雪粒",
    80: "小阵雨", 81: "中阵雨", 82: "大阵雨",
    85: "小阵雪", 86: "大阵雪",
    95: "雷阵雨", 96: "雷阵雨伴轻微冰雹", 99: "雷阵雨伴大冰雹",
}

# ---------- 工具函数 ----------
def now_cn_iso():
    """返回当前北京时间 ISO8601 格式"""
    return datetime.now(tz=CN_TZ).replace(microsecond=0).isoformat()

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def decode_weather(code):
    """WMO 数字代码转中文描述"""
    try:
        code_int = int(code)
        return WMO_WEATHER_CODES.get(code_int, f"未知({code_int})")
    except Exception:
        return str(code) if code is not None else ""

def ensure_csv_header(path: str, fieldnames: list):
    new_file = not os.path.exists(path)
    if new_file:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

def write_csv_row(row_dict, fieldnames):
    ensure_csv_header(CSV_PATH, fieldnames)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow({k: row_dict.get(k, "") for k in fieldnames})

# ---------- 数据源 1：Open-Meteo（无需密钥） ----------
def fetch_open_meteo(lat, lon, retries=3, timeout=12):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,precipitation,relative_humidity_2m,"
        "wind_speed_10m,wind_direction_10m,pressure_msl,weather_code"
        "&timezone=Asia%2FShanghai"
    )
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            cur = data.get("current", {})
            row = {
                "ts_iso": now_cn_iso(),              # 采集时间
                "ts_obs_iso": "",                    # Open-Meteo 无 obsTime，这里留空
                "location_name": LOCATION_NAME,
                "provider": "open-meteo",
                "temp_c": safe_float(cur.get("temperature_2m")),
                "precip_mm_1h": safe_float(cur.get("precipitation")),  # 过去1小时
                "humidity_pct": safe_float(cur.get("relative_humidity_2m")),
                "wind_speed_mps": safe_float(cur.get("wind_speed_10m")),
                "wind_dir_deg": safe_float(cur.get("wind_direction_10m")),
                "pressure_hpa": safe_float(cur.get("pressure_msl")),
                "weather_code_or_text": str(cur.get("weather_code")),
                "weather_desc": decode_weather(cur.get("weather_code")),
            }
            return row
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(2)

# ---------- 数据源 2：和风天气（支持经纬度 + obsTime） ----------
def fetch_qweather(api_key, location, retries=3, timeout=12):
    """和风 Now API：用 obsTime；失败时打印 code 便于排查。"""
    if not api_key:
        print("[SKIP] qweather: QWEATHER_API_KEY is empty")
        return None

    # 和风建议最多两位小数，这里做个规整
    try:
        loc_lon, loc_lat = location.split(",")
        location = f"{float(loc_lon):.2f},{float(loc_lat):.2f}"
    except Exception:
        # 不是经纬度就当作 LocationID 使用
        pass

    url = (
        "https://devapi.qweather.com/v7/weather/now"
        f"?key={api_key}&location={location}&lang={QWEATHER_LANG}&unit={QWEATHER_UNIT}"
    )

    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            status = r.status_code
            if status != 200:
                print(f"[ERR] qweather http {status}")
                if i == retries - 1:
                    return None
                time.sleep(2)
                continue

            j = r.json()
            code = j.get("code")
            if code != "200":
                # 常见：401/402/403/429 等会以 code 体现
                print(f"[ERR] qweather api code={code}, location={location}")
                return None

            now = j.get("now", {})
            def kmh_to_mps(x):
                try:
                    return float(x) / 3.6
                except Exception:
                    return None

            row = {
                "ts_iso": now_cn_iso(),
                "ts_obs_iso": now.get("obsTime", ""),
                "location_name": LOCATION_NAME,
                "provider": "qweather",
                "temp_c": safe_float(now.get("temp")),
                "precip_mm_1h": safe_float(now.get("precip")),
                "humidity_pct": safe_float(now.get("humidity")),
                "wind_speed_mps": kmh_to_mps(now.get("windSpeed")),
                "wind_dir_deg": safe_float(now.get("wind360")),
                "pressure_hpa": safe_float(now.get("pressure")),
                "weather_code_or_text": now.get("icon") or now.get("text"),
                "weather_desc": now.get("text"),
            }
            return row
        except Exception as e:
            if i == retries - 1:
                print(f"[ERR] qweather exception: {e}")
                return None
            time.sleep(2)


def main():
    # 统一表头（新增 ts_obs_iso、location_name、weather_desc）
    fieldnames = [
        "ts_iso", "ts_obs_iso", "location_name", "provider",
        "temp_c", "precip_mm_1h", "humidity_pct",
        "wind_speed_mps", "wind_dir_deg", "pressure_hpa",
        "weather_code_or_text", "weather_desc",
    ]
    # 1) Open-Meteo
    try:
        row1 = fetch_open_meteo(LAT, LON)
        write_csv_row(row1, fieldnames)
        print(f"[OK] open-meteo {row1['ts_iso']} {row1['location_name']} {row1['weather_desc']} precip={row1['precip_mm_1h']}")
    except Exception as e:
        print(f"[ERR] open-meteo: {e}")

    # 2) QWeather（可选：需 key）
    row2 = fetch_qweather(QWEATHER_API_KEY, QWEATHER_LOCATION)
    if row2:
        write_csv_row(row2, fieldnames)
        print(f"[OK] qweather {row2['ts_iso']} obs={row2['ts_obs_iso']} {row2['location_name']} {row2['weather_desc']} precip={row2['precip_mm_1h']}")

if __name__ == "__main__":
    main()
