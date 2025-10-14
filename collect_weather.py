# collect_weather.py
import os
import csv
import time
import requests
from datetime import datetime
from dateutil import tz

# ====== 基本配置（可用环境变量覆盖）======
LAT  = float(os.getenv("LAT", 22.543096))         # 纬度
LON  = float(os.getenv("LON", 114.057865))        # 经度
LOCATION_NAME = os.getenv("LOCATION_NAME", "深圳市").strip()

CSV_PATH = "weather_log.csv"
CN_TZ = tz.gettz("Asia/Shanghai")

# ====== Open-Meteo（无需密钥）======
# WMO 天气代码 → 中文描述
WMO_WEATHER_CODES = {
    0:"晴",1:"基本晴",2:"局部多云",3:"多云",45:"雾",48:"雾凇",
    51:"小毛毛雨",53:"中毛毛雨",55:"大毛毛雨",56:"小冻毛毛雨",57:"大冻毛毛雨",
    61:"小雨",63:"中雨",65:"大雨",66:"小冻雨",67:"大冻雨",
    71:"小雪",73:"中雪",75:"大雪",77:"雪粒",
    80:"小阵雨",81:"中阵雨",82:"大阵雨",
    85:"小阵雪",86:"大阵雪",
    95:"雷阵雨",96:"雷阵雨伴轻微冰雹",99:"雷阵雨伴大冰雹",
}

# ====== 和风天气（开发者 or 商用皆可，通过 host+header 切换）======
QWEATHER_API_KEY  = os.getenv("QWEATHER_API_KEY", "").strip()
QWEATHER_API_HOST = os.getenv("QWEATHER_API_HOST").strip()  # 开发者默认
# location：优先读 env；否则使用经纬度（和风要求经度在前、纬度在后）
_q_loc_env = os.getenv("QWEATHER_LOCATION", "").strip()
if _q_loc_env:
    QWEATHER_LOCATION = _q_loc_env
else:
    QWEATHER_LOCATION = f"{LON:.2f},{LAT:.2f}"  # 两位小数
QWEATHER_LANG = os.getenv("QWEATHER_LANG", "zh").strip()
QWEATHER_UNIT = os.getenv("QWEATHER_UNIT", "m").strip()  # m=公制, i=英制

# ---------- 工具 ----------
def now_cn_iso():
    return datetime.now(tz=CN_TZ).replace(microsecond=0).isoformat()

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def decode_weather(code):
    try:
        return WMO_WEATHER_CODES.get(int(code), f"未知({int(code)})")
    except Exception:
        return str(code) if code is not None else ""

def ensure_csv(path, fieldnames):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

def write_row(row, fieldnames):
    ensure_csv(CSV_PATH, fieldnames)
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow({k: row.get(k, "") for k in fieldnames})

# ---------- Open-Meteo ----------
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
            cur = r.json().get("current", {})
            return {
                "ts_iso": now_cn_iso(),          # 采集时间
                "ts_obs_iso": "",                # Open-Meteo 无 obsTime
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
        except Exception:
            if i == retries - 1: raise
            time.sleep(2)

# ---------- QWeather（Header 鉴权 + 可配置 Host + obsTime）----------
def fetch_qweather(api_key, host, location, retries=3, timeout=12):
    if not api_key:
        print("[SKIP] qweather: QWEATHER_API_KEY missing")
        return None

    # 规整经纬度格式（两位小数），非经纬度则当作 LocationID
    try:
        lon, lat = map(float, location.split(","))
        location = f"{lon:.2f},{lat:.2f}"
    except Exception:
        pass

    url = f"https://{host}/v7/weather/now?location={location}&lang={QWEATHER_LANG}&unit={QWEATHER_UNIT}"
    headers = {"X-QW-Api-Key": api_key}

    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code != 200:
                print(f"[ERR] qweather http {r.status_code}")
                time.sleep(2)
                continue

            j = r.json()
            if j.get("code") != "200":
                print(f"[ERR] qweather api code={j.get('code')}")
                return None

            now = j.get("now", {})
            def kmh_to_mps(x): 
                try: return float(x)/3.6
                except: return None

            return {
                "ts_iso": now_cn_iso(),                     # 采集时间
                "ts_obs_iso": now.get("obsTime", ""),       # 观测时间（准确时刻）
                "location_name": LOCATION_NAME,
                "provider": "qweather",
                "temp_c": safe_float(now.get("temp")),
                "precip_mm_1h": safe_float(now.get("precip")),   # 过去1小时降水(mm)
                "humidity_pct": safe_float(now.get("humidity")),
                "wind_speed_mps": kmh_to_mps(now.get("windSpeed")),  # km/h -> m/s
                "wind_dir_deg": safe_float(now.get("wind360")),
                "pressure_hpa": safe_float(now.get("pressure")),
                "weather_code_or_text": now.get("icon") or now.get("text"),
                "weather_desc": now.get("text"),
            }
        except Exception as e:
            if i == retries - 1:
                print(f"[ERR] qweather exception: {e}")
                return None
            time.sleep(2)

def main():
    fieldnames = [
        "ts_iso", "ts_obs_iso", "location_name", "provider",
        "temp_c", "precip_mm_1h", "humidity_pct",
        "wind_speed_mps", "wind_dir_deg", "pressure_hpa",
        "weather_code_or_text", "weather_desc",
    ]

    # 1) Open-Meteo
    try:
        row1 = fetch_open_meteo(LAT, LON)
        write_row(row1, fieldnames)
        print(f"[OK] open-meteo {row1['ts_iso']} {row1['location_name']} {row1['weather_desc']} precip={row1['precip_mm_1h']}")
    except Exception as e:
        print(f"[ERR] open-meteo: {e}")

    # 2) QWeather（若配置了 key）
    row2 = fetch_qweather(QWEATHER_API_KEY, QWEATHER_API_HOST, QWEATHER_LOCATION)
    if row2:
        write_row(row2, fieldnames)
        print(f"[OK] qweather {row2['ts_iso']} obs={row2['ts_obs_iso']} {row2['location_name']} {row2['weather_desc']} precip={row2['precip_mm_1h']}")

if __name__ == "__main__":
    main()
