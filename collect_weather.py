# collect_weather.py
import os
import csv
import time
import json
import sqlite3
from datetime import datetime
from dateutil import tz
import requests

# ---- 可配置：深圳坐标（市中心大致坐标）----
LAT, LON = 22.543096, 114.057865  # 深圳市民中心附近
CSV_PATH = "weather_log.csv"
USE_SQLITE = False
SQLITE_PATH = "weather_log.sqlite3"

# ---- 可选：QWeather（和风天气）----
QWEATHER_API_KEY = os.getenv("QWEATHER_API_KEY", "").strip()
# 深圳行政区adcode（和风支持行政区编码定位）：440300
QWEATHER_LOCATION = os.getenv("QWEATHER_LOCATION", "440300").strip()

# ---- 时区设定：统一写 Asia/Shanghai ----
CN_TZ = tz.gettz("Asia/Shanghai")

# ---- 工具函数 ----
def now_cn_iso():
    return datetime.now(tz=CN_TZ).replace(microsecond=0).isoformat()

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def write_csv_row(row_dict):
    # 固定列顺序，方便后续分析
    fieldnames = [
        "ts_iso",                     # 记录时间（Asia/Shanghai）
        "provider",                   # open-meteo / qweather
        "temp_c", "precip_mm_1h",
        "humidity_pct", "wind_speed_mps", "wind_dir_deg",
        "pressure_hpa",
        "weather_code_or_text"        # 代码或现象描述
    ]
    new_file = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        writer.writerow({k: row_dict.get(k) for k in fieldnames})

def init_sqlite():
    if not USE_SQLITE:
        return
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_log(
        ts_iso TEXT,
        provider TEXT,
        temp_c REAL,
        precip_mm_1h REAL,
        humidity_pct REAL,
        wind_speed_mps REAL,
        wind_dir_deg REAL,
        pressure_hpa REAL,
        weather_code_or_text TEXT
    )
    """)
    conn.commit()
    conn.close()

def write_sqlite_row(row_dict):
    if not USE_SQLITE:
        return
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weather_log
        (ts_iso, provider, temp_c, precip_mm_1h, humidity_pct,
         wind_speed_mps, wind_dir_deg, pressure_hpa, weather_code_or_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row_dict.get("ts_iso"), row_dict.get("provider"),
        row_dict.get("temp_c"), row_dict.get("precip_mm_1h"),
        row_dict.get("humidity_pct"), row_dict.get("wind_speed_mps"),
        row_dict.get("wind_dir_deg"), row_dict.get("pressure_hpa"),
        row_dict.get("weather_code_or_text"),
    ))
    conn.commit()
    conn.close()

# ---- 数据源1：Open-Meteo（无需密钥）----
def fetch_open_meteo(lat, lon, retries=3, timeout=12):
    # 当前天气 + 当前小时降水量（过去1小时累积）
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
                "provider": "open-meteo",
                "temp_c": safe_float(cur.get("temperature_2m")),
                "precip_mm_1h": safe_float(cur.get("precipitation")),  # 过去1小时
                "humidity_pct": safe_float(cur.get("relative_humidity_2m")),
                "wind_speed_mps": safe_float(cur.get("wind_speed_10m")),  # m/s
                "wind_dir_deg": safe_float(cur.get("wind_direction_10m")),
                "pressure_hpa": safe_float(cur.get("pressure_msl")),
                "weather_code_or_text": str(cur.get("weather_code")),
            }
            return row
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(2)

# ---- 数据源2（可选）：QWeather 实况 ----
def fetch_qweather(api_key, location, retries=3, timeout=12):
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
            # QWeather的降水“precip”为“分钟级降水量(毫米) *近1小时*”？官方文档会调整，这里记录原字段，分析时自定义口径
            row = {
                "ts_iso": now_cn_iso(),
                "provider": "qweather",
                "temp_c": safe_float(now.get("temp")),
                "precip_mm_1h": safe_float(now.get("precip")),  # 语义以官方文档为准
                "humidity_pct": safe_float(now.get("humidity")),
                "wind_speed_mps": safe_float(now.get("windSpeed")),  # km/h 也常见，留意口径
                "wind_dir_deg": safe_float(now.get("windDir")),     # 若为方位名则不可转，为简洁先原样放 text 字段
                "pressure_hpa": safe_float(now.get("pressure")),
                "weather_code_or_text": now.get("text"),
            }
            return row
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(2)

def main():
    init_sqlite()

    # 1) 先用 Open-Meteo
    try:
        row1 = fetch_open_meteo(LAT, LON)
        write_csv_row(row1)
        write_sqlite_row(row1)
        print(f"[OK] open-meteo {row1['ts_iso']} precip={row1['precip_mm_1h']}")
    except Exception as e:
        print(f"[ERR] open-meteo: {e}")

    # 2) 可选：再写一行 QWeather（若配置了密钥）
    row2 = fetch_qweather(QWEATHER_API_KEY, QWEATHER_LOCATION)
    if row2:
        write_csv_row(row2)
        write_sqlite_row(row2)
        print(f"[OK] qweather {row2['ts_iso']} precip={row2['precip_mm_1h']}")

if __name__ == "__main__":
    main()
