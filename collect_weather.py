# collect_weather.py
import os
import csv
import time
import requests
from datetime import datetime
from dateutil import tz

# ====== 基本配置（可用环境变量覆盖）======
LAT           = float(os.getenv("LAT", 22.543096))
LON           = float(os.getenv("LON", 114.057865))
LOCATION_NAME = os.getenv("LOCATION_NAME", "深圳市").strip()

CSV_PATH = "weather_log.csv"
CN_TZ    = tz.gettz("Asia/Shanghai")

# ====== WMO 天气代码 → 中文描述 ======
WMO_WEATHER_CODES = {
    0: "晴", 1: "基本晴", 2: "局部多云", 3: "多云",
    45: "雾", 48: "雾凇",
    51: "小毛毛雨", 53: "中毛毛雨", 55: "大毛毛雨",
    56: "小冻毛毛雨", 57: "大冻毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    66: "小冻雨", 67: "大冻雨",
    71: "小雪", 73: "中雪", 75: "大雪", 77: "雪粒",
    80: "小阵雨", 81: "中阵雨", 82: "大阵雨",
    85: "小阵雪", 86: "大阵雪",
    95: "雷阵雨", 96: "雷阵雨伴轻微冰雹", 99: "雷阵雨伴大冰雹",
}

# WMO 代码 → 降水强度分级（分析用）0=无 1=毛毛雨 2=小雨 3=中雨 4=大雨/雷雨
WMO_RAIN_LEVEL = {
    51: 1, 53: 1, 55: 1, 56: 1, 57: 1,
    61: 2, 80: 2,
    63: 3, 81: 3,
    65: 4, 66: 4, 67: 4, 82: 4, 95: 4, 96: 4, 99: 4,
}

# ====== 和风天气配置 ======
QWEATHER_API_KEY  = os.getenv("QWEATHER_API_KEY", "").strip()
QWEATHER_API_HOST = os.getenv("QWEATHER_API_HOST", "devapi.qweather.com").strip() or "devapi.qweather.com"
_q_loc_env        = os.getenv("QWEATHER_LOCATION", "").strip()
QWEATHER_LOCATION = _q_loc_env if _q_loc_env else f"{LON:.2f},{LAT:.2f}"
QWEATHER_LANG     = os.getenv("QWEATHER_LANG", "zh").strip()
QWEATHER_UNIT     = os.getenv("QWEATHER_UNIT", "m").strip()

# ====== 固定 schema（新旧通用，写入时补齐所有列）======
FIELDNAMES = [
    "ts_iso", "ts_obs_iso", "location_name", "provider",
    "temp_c", "precip_mm_1h", "humidity_pct",
    "wind_speed_mps", "wind_dir_deg", "pressure_hpa",
    "weather_code", "weather_desc",
    # 分析辅助列（写入时算好，分析脚本无需再 parse）
    "hour_cst", "weekday", "is_workday", "is_work_hour", "is_commute", "rain_level",
]


def now_cn():
    return datetime.now(tz=CN_TZ).replace(microsecond=0)


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def decode_weather(code):
    try:
        return WMO_WEATHER_CODES.get(int(code), f"未知({int(code)})")
    except Exception:
        return str(code) if code is not None else ""


def rain_level_from_wmo(code, precip):
    """WMO 代码 + 实测降水量综合判级，取较高值"""
    level = 0
    try:
        level = max(level, WMO_RAIN_LEVEL.get(int(code), 0))
    except Exception:
        pass
    if precip is not None:
        if precip >= 2.0:
            level = max(level, 4)
        elif precip >= 0.5:
            level = max(level, 3)
        elif precip > 0.1:
            level = max(level, 2)
        elif precip > 0:
            level = max(level, 1)
    return level


def make_aux(ts, wmo_code, precip):
    h = ts.hour
    wd = ts.weekday()  # 0=Mon
    return {
        "hour_cst":     h,
        "weekday":      wd,
        "is_workday":   1 if wd < 5 else 0,
        "is_work_hour": 1 if 9 <= h <= 17 else 0,
        "is_commute":   1 if h in (7, 8, 18, 19) else 0,
        "rain_level":   rain_level_from_wmo(wmo_code, precip),
    }


def ensure_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()


def write_row(row):
    ensure_csv()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writerow({k: row.get(k, "") for k in FIELDNAMES})


def fetch_open_meteo(retries=3, timeout=12):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        "&current=temperature_2m,precipitation,relative_humidity_2m,"
        "wind_speed_10m,wind_direction_10m,pressure_msl,weather_code"
        "&timezone=Asia%2FShanghai"
    )
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            cur  = r.json().get("current", {})
            ts   = now_cn()
            code = cur.get("weather_code")
            prec = safe_float(cur.get("precipitation"))
            row  = {
                "ts_iso":         ts.isoformat(),
                "ts_obs_iso":     "",
                "location_name":  LOCATION_NAME,
                "provider":       "open-meteo",
                "temp_c":         safe_float(cur.get("temperature_2m")),
                "precip_mm_1h":   prec,
                "humidity_pct":   safe_float(cur.get("relative_humidity_2m")),
                "wind_speed_mps": safe_float(cur.get("wind_speed_10m")),
                "wind_dir_deg":   safe_float(cur.get("wind_direction_10m")),
                "pressure_hpa":   safe_float(cur.get("pressure_msl")),
                "weather_code":   str(code) if code is not None else "",
                "weather_desc":   decode_weather(code),
            }
            row.update(make_aux(ts, code, prec))
            return row
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2)


def fetch_qweather(retries=3, timeout=12):
    if not QWEATHER_API_KEY:
        print("[SKIP] qweather: QWEATHER_API_KEY missing")
        return None
    location = QWEATHER_LOCATION
    try:
        ln, lt = map(float, location.split(","))
        location = f"{ln:.2f},{lt:.2f}"
    except Exception:
        pass
    url     = f"https://{QWEATHER_API_HOST}/v7/weather/now?location={location}&lang={QWEATHER_LANG}&unit={QWEATHER_UNIT}"
    headers = {"X-QW-Api-Key": QWEATHER_API_KEY}
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
            nd = j.get("now", {})

            def kmh_to_mps(x):
                try:
                    return round(float(x) / 3.6, 2)
                except Exception:
                    return None

            ts   = now_cn()
            prec = safe_float(nd.get("precip"))
            icon = nd.get("icon") or nd.get("text")
            row  = {
                "ts_iso":         ts.isoformat(),
                "ts_obs_iso":     nd.get("obsTime", ""),
                "location_name":  LOCATION_NAME,
                "provider":       "qweather",
                "temp_c":         safe_float(nd.get("temp")),
                "precip_mm_1h":   prec,
                "humidity_pct":   safe_float(nd.get("humidity")),
                "wind_speed_mps": kmh_to_mps(nd.get("windSpeed")),
                "wind_dir_deg":   safe_float(nd.get("wind360")),
                "pressure_hpa":   safe_float(nd.get("pressure")),
                "weather_code":   icon,
                "weather_desc":   nd.get("text"),
            }
            row.update(make_aux(ts, None, prec))
            return row
        except Exception as e:
            if i == retries - 1:
                print(f"[ERR] qweather exception: {e}")
                return None
            time.sleep(2)


def main():
    # 1) Open-Meteo（无需密钥，主数据源）
    try:
        row = fetch_open_meteo()
        write_row(row)
        print(f"[OK] open-meteo  {row['ts_iso']}  {row['weather_desc']}  "
              f"precip={row['precip_mm_1h']}mm  rain_level={row['rain_level']}")
    except Exception as e:
        print(f"[ERR] open-meteo: {e}")

    # 2) QWeather（若配置了 key）
    row = fetch_qweather()
    if row:
        write_row(row)
        print(f"[OK] qweather    {row['ts_iso']}  obs={row['ts_obs_iso']}  "
              f"{row['weather_desc']}  precip={row['precip_mm_1h']}mm  rain_level={row['rain_level']}")


if __name__ == "__main__":
    main()
