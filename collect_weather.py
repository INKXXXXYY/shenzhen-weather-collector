# collect_weather.py
import os
import csv
import time
import requests
from datetime import datetime
from dateutil import tz

# ===== 配置部分 =====
LAT, LON = 22.529020, 114.022160  # 深圳市中心坐标
LOCATION_NAME = "深圳下沙"           # 新增字段：地点名
CSV_PATH = "weather_log.csv"
CN_TZ = tz.gettz("Asia/Shanghai")

QWEATHER_API_KEY = os.getenv("QWEATHER_API_KEY", "").strip()
QWEATHER_LOCATION = os.getenv("QWEATHER_LOCATION", "440300").strip()

# ===== Open-Meteo 天气代码映射表 =====
WMO_WEATHER_CODES = {
    0: "晴",
    1: "基本晴",
    2: "局部多云",
    3: "多云",
    45: "雾",
    48: "雾凇",
    51: "小毛毛雨",
    53: "中毛毛雨",
    55: "大毛毛雨",
    56: "小冻毛毛雨",
    57: "大冻毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    66: "小冻雨",
    67: "大冻雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    77: "雪粒",
    80: "小阵雨",
    81: "中阵雨",
    82: "大阵雨",
    85: "小阵雪",
    86: "大阵雪",
    95: "雷阵雨",
    96: "雷阵雨伴轻微冰雹",
    99: "雷阵雨伴大冰雹",
}

def now_cn_iso():
    """返回当前北京时间 ISO8601 格式"""
    return datetime.now(tz=CN_TZ).replace(microsecond=0).isoformat()

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def decode_weather(code):
    """数字代码转中文描述"""
    try:
        code_int = int(code)
        return WMO_WEATHER_CODES.get(code_int, f"未知({code_int})")
    except Exception:
        return str(code)

def write_csv_row(row_dict):
    fieldnames = [
        "ts_iso",
        "location_name",
        "provider",
        "temp_c",
        "precip_mm_1h",
        "humidity_pct",
        "wind_speed_mps",
        "wind_dir_deg",
        "pressure_hpa",
        "weather_code_or_text",
        "weather_desc",
    ]
    new_file = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        writer.writerow({k: row_dict.get(k, "") for k in fieldnames})

def fetch_open_meteo(lat, lon, retries=3, timeout=12):
    """调用 open-meteo API 获取实时天气"""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,precipitation,relative_humidity_2m,wind_speed_10m,wind_direction_10m,pressure_msl,weather_code"
        "&timezone=Asia%2FShanghai"
    )
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            cur = data.get("current", {})
            row = {
                "ts_iso": now_cn_iso(),
                "location_name": LOCATION_NAME,
                "provider": "open-meteo",
                "temp_c": safe_float(cur.get("temperature_2m")),
                "precip_mm_1h": safe_float(cur.get("precipitation")),
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

def fetch_qweather(api_key, location, retries=3, timeout=12):
    """调用和风天气（可选）"""
    if not api_key:
        return None
    url = f"https://devapi.qweather.com/v7/weather/now?key={api_key}&location={location}"
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            if j.get("code") != "200":
                return None
            now = j.get("now", {})
            row = {
                "ts_iso": now_cn_iso(),
                "location_name": LOCATION_NAME,
                "provider": "qweather",
                "temp_c": safe_float(now.get("temp")),
                "precip_mm_1h": safe_float(now.get("precip")),
                "humidity_pct": safe_float(now.get("humidity")),
                "wind_speed_mps": safe_float(now.get("windSpeed")),
                "wind_dir_deg": safe_float(now.get("windDir")),
                "pressure_hpa": safe_float(now.get("pressure")),
                "weather_code_or_text": now.get("text"),
                "weather_desc": now.get("text"),  # 和风自带中文
            }
            return row
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(2)

def main():
    try:
        row1 = fetch_open_meteo(LAT, LON)
        write_csv_row(row1)
        print(f"[OK] open-meteo {row1['ts_iso']} {row1['location_name']} {row1['weather_desc']} precip={row1['precip_mm_1h']}")
    except Exception as e:
        print(f"[ERR] open-meteo: {e}")

    row2 = fetch_qweather(QWEATHER_API_KEY, QWEATHER_LOCATION)
    if row2:
        write_csv_row(row2)
        print(f"[OK] qweather {row2['ts_iso']} {row2['location_name']} {row2['weather_desc']} precip={row2['precip_mm_1h']}")

if __name__ == "__main__":
    main()
