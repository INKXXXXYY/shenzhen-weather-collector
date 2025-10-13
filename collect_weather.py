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
