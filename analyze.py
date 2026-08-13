#!/usr/bin/env python3
"""
analyze.py — 深圳天气数据分析
验证"上班时间更容易下雨"是幸存者偏差还是客观规律

关键方法：
  1. 控制"雨季偏差"——各小时采集起始日期不同，若不对齐会把只在雨季
     采集的时段（如 08:xx 仅从 2026-04 起）误判为高降雨。这里统一裁到
     所有小时都有覆盖的公共窗口再比较。
  2. 分三档降水强度（毛毛雨 / 像样的雨 / 大雨暴雨），因为"折磨打工人"
     的是大雨而非毛毛雨。

用法:
    python analyze.py              # 分析 weather_log.csv
    python analyze.py --csv path   # 指定 CSV 路径
"""
import csv
import sys
import argparse
from datetime import datetime
from collections import defaultdict, Counter

# WMO 代码 → 降水强度分级（与 collect_weather.py 保持一致）
WMO_RAIN_LEVEL = {
    51: 1, 53: 1, 55: 1, 56: 1, 57: 1,
    61: 2, 80: 2,
    63: 3, 81: 3,
    65: 4, 66: 4, 67: 4, 82: 4, 95: 4, 96: 4, 99: 4,
}

BLOCK = chr(9608)  # █
LIGHT = chr(9617)  # ░


def _rain_level(code, precip):
    """无 rain_level 列时的回退推算（兼容旧数据）"""
    level = 0
    try:
        level = max(level, WMO_RAIN_LEVEL.get(int(code), 0))
    except Exception:
        pass
    if precip >= 2.0:
        level = max(level, 4)
    elif precip >= 0.5:
        level = max(level, 3)
    elif precip > 0.1:
        level = max(level, 2)
    elif precip > 0:
        level = max(level, 1)
    return level


def parse_csv(path):
    """兼容 9 / 11 / 12 / 18 列四种历史 schema，只取 open-meteo"""
    records = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row or row[0].startswith("ts_iso"):
                continue
            n = len(row)
            try:
                if n >= 18:      # 新 schema（带辅助列）
                    ts_raw, provider, prec_raw, code_raw, desc, rl_raw = \
                        row[0], row[3], row[5], row[10], row[11], row[17]
                elif n == 12:    # 12 列过渡格式
                    ts_raw, provider, prec_raw, code_raw, desc, rl_raw = \
                        row[0], row[3], row[5], row[10], row[11], ""
                elif n == 11:    # 11 列
                    ts_raw, provider, prec_raw, code_raw, desc, rl_raw = \
                        row[0], row[2], row[4], row[9], row[10], ""
                elif n == 9:     # 9 列最旧格式
                    ts_raw, provider, prec_raw, code_raw, desc, rl_raw = \
                        row[0], row[1], row[3], row[8], "", ""
                else:
                    continue
            except IndexError:
                continue

            if provider.strip() != "open-meteo":
                continue
            try:
                ts = datetime.fromisoformat(ts_raw.strip())
            except Exception:
                continue
            try:
                prec = float(prec_raw) if prec_raw.strip() else 0.0
            except ValueError:
                prec = 0.0
            try:
                rl = int(rl_raw)
            except (ValueError, TypeError):
                rl = _rain_level(code_raw.strip(), prec)

            records.append({
                "ts": ts, "hour": ts.hour, "weekday": ts.weekday(),
                "precip": prec, "code": code_raw.strip(),
                "desc": desc.strip(), "rain_level": rl,
            })
    return records


def fair_start(records):
    """各小时均有覆盖的最晚起始日期（控制雨季偏差的关键）"""
    by_hour = defaultdict(list)
    for r in records:
        by_hour[r["hour"]].append(r["ts"].date())
    starts = {h: min(dates) for h, dates in by_hour.items() if dates}
    if len(starts) < 24:
        return min(r["ts"].date() for r in records)
    return max(starts.values())


def bar(pct, width=20):
    filled = int(pct / 100 * width)
    return BLOCK * filled + LIGHT * (width - filled)


def sec(title):
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def analyze(records):
    all_dates = sorted(set(r["ts"].date() for r in records))
    fs = fair_start(records)
    win = [r for r in records if r["ts"].date() >= fs]

    print(f"\n{'=' * 60}")
    print("  深圳天气 · 上班时间下雨分析报告")
    print(f"{'=' * 60}")
    print(f"记录总量  : {len(records)} 条 (open-meteo)")
    print(f"日期范围  : {min(all_dates)} -> {max(all_dates)}  ({len(all_dates)} 天)")
    print(f"公平窗口  : {fs} 起（各小时均覆盖）  {len(win)} 条")

    # 1. 采集均匀度诊断
    sec("1. 各小时采集样本量（全年，诊断采集均匀度）")
    hc = Counter(r["hour"] for r in records)
    dh = Counter((r["ts"].date(), r["hour"]) for r in records)
    print(f"  {'时段':>5}  {'全年样本':>7}  {'缺失天':>8}  {'缺失%':>6}")
    for h in range(24):
        miss = sum(1 for d in all_dates if dh[(d, h)] == 0)
        pct = miss / len(all_dates) * 100
        flag = "  <- 异常稀疏" if pct > 60 else ""
        print(f"  {h:02d}:xx  {hc[h]:7d}  {miss:5d}/{len(all_dates)}  {pct:5.1f}%{flag}")

    # 2. 各小时降雨概率（同窗口，三档阈值）
    sec("2. 各小时降雨概率（同窗口对比，控制雨季偏差）")
    thresholds = [
        ("含毛毛雨 rain_level>=1", lambda r: r["rain_level"] >= 1),
        ("像样的雨 rain_level>=3", lambda r: r["rain_level"] >= 3),
        ("大雨暴雨 rain_level>=4", lambda r: r["rain_level"] >= 4),
    ]
    for label, fn in thresholds:
        ht = defaultdict(int)
        hr = defaultdict(int)
        for r in win:
            ht[r["hour"]] += 1
            if fn(r):
                hr[r["hour"]] += 1

        print(f"\n  [{label}]  窗口 {len(win)} 条")
        print(f"  {'时段':>5}  {'n':>5}  {'雨':>4}  {'概率':>6}  分布图                 备注")
        for h in range(24):
            t = ht[h]
            c = hr[h]
            pct = c / t * 100 if t else 0
            note = ""
            if h == 8:
                note = "<- 8点通勤高峰"
            elif 9 <= h <= 17:
                note = "<- 上班"
            elif h in (7, 18, 19):
                note = "<- 通勤边缘"
            elif t < 20:
                note = "<- 样本少"
            print(f"  {h:02d}:xx  {t:5d}  {c:4d}  {pct:5.1f}%  {bar(pct)}  {note}")

        def agg(hours):
            t = sum(ht[h] for h in hours)
            c = sum(hr[h] for h in hours)
            return t, c, c / t * 100 if t else 0

        ct, cc, cp = agg([7, 8, 18, 19])
        wt, wc, wp = agg(list(range(9, 18)))
        ot, oc, op = agg(list(range(0, 7)) + list(range(20, 24)))
        print(f"\n  汇总 [{label}]")
        print(f"    通勤 (07-08,18-19)  n={ct:4d}  {cp:.1f}%")
        print(f"    上班 (09-17)        n={wt:4d}  {wp:.1f}%")
        print(f"    夜间/闲时           n={ot:4d}  {op:.1f}%")
        print(f"    通勤 vs 夜间  {cp - op:+.1f}pp  |  上班 vs 夜间  {wp - op:+.1f}pp")

    # 3. 工作日 vs 周末
    sec("3. 工作日 vs 周末（同窗口，含毛毛雨）")
    for lbl, cond in [("工作日", lambda r: r["weekday"] < 5),
                      ("周末  ", lambda r: r["weekday"] >= 5)]:
        sub = [r for r in win if cond(r)]
        rain = sum(1 for r in sub if r["rain_level"] >= 1)
        pct = rain / len(sub) * 100 if sub else 0
        print(f"  {lbl}  n={len(sub):5d}  {rain:4d}次雨  {pct:.1f}%")

    # 4. 月度降水量
    sec("4. 月度累计降水量（全年）")
    mp = defaultdict(float)
    for r in records:
        mp[r["ts"].strftime("%Y-%m")] += r["precip"]
    for k in sorted(mp):
        print(f"  {k}  {mp[k]:7.1f}mm  {BLOCK * int(mp[k] / 8)}")

    # 5. 极端降水 TOP10
    sec("5. 单小时最大降水 TOP10")
    wd_names = ["一", "二", "三", "四", "五", "六", "日"]
    work_h = set(range(9, 18))
    comm_h = {7, 8, 18, 19}
    for r in sorted(records, key=lambda x: x["precip"], reverse=True)[:10]:
        if r["hour"] in work_h:
            slot = "上班时间"
        elif r["hour"] in comm_h:
            slot = "通勤时段"
        else:
            slot = "夜间/闲时"
        print(f"  {r['ts'].strftime('%Y-%m-%d %H:%M')} 周{wd_names[r['weekday']]}  "
              f"{r['precip']:4.1f}mm  {(r['desc'] or r['code']):8s}  [{slot}]")

    # 6. 结论
    sec("6. 结论")
    ht6 = defaultdict(int)
    hr6 = defaultdict(int)
    for r in win:
        ht6[r["hour"]] += 1
        if r["rain_level"] >= 3:
            hr6[r["hour"]] += 1

    def agg6(hours):
        t = sum(ht6[h] for h in hours)
        c = sum(hr6[h] for h in hours)
        return c / t * 100 if t else 0

    cp6 = agg6([7, 8, 18, 19])
    wp6 = agg6(list(range(9, 18)))
    op6 = agg6(list(range(0, 7)) + list(range(20, 24)))
    uplift = wp6 - op6

    if abs(uplift) < 1.5:
        verdict = "幸存者偏差主导：白天与夜间降雨概率统计上无显著差异"
    elif uplift > 0:
        verdict = f"白天(工作时段)比夜间高 {uplift:.1f}pp，午后热对流 + 幸存者偏差叠加"
    else:
        verdict = "反向结果：夜间反而更容易下雨，更支持纯幸存者偏差解释"

    print(f"""
  基准：像样的雨(rain_level>=3，>=0.5mm 或 WMO 中雨以上)
  同雨季窗口（{fs} 起）

    通勤时段  {cp6:.1f}%
    工作时段  {wp6:.1f}%
    夜间/闲时 {op6:.1f}%
    工作 vs 夜间差值  {uplift:+.1f}pp

  结论: {verdict}

  物理解释：深圳午后热对流高峰集中在 10-15 时（正是工作时间中段），
  这是真实气候规律；情绪记忆进一步放大了被淋的感知频率。
  "上班 == 下雨" = 热对流客观规律 + 情绪记忆放大，两者共同作用。
""")


def main():
    parser = argparse.ArgumentParser(description="深圳天气数据分析")
    parser.add_argument("--csv", default="weather_log.csv", help="CSV 路径")
    args = parser.parse_args()

    records = parse_csv(args.csv)
    if not records:
        print("没有有效数据，退出")
        sys.exit(1)
    analyze(records)


if __name__ == "__main__":
    main()
