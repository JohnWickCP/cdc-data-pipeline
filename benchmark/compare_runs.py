#!/usr/bin/env python3
"""
compare_runs.py — So sánh các lần chạy benchmark từ history.jsonl

Usage:
    python benchmark/compare_runs.py              # So sánh tất cả
    python benchmark/compare_runs.py -n 5         # 5 lần gần nhất
    python benchmark/compare_runs.py --mode quick # Lọc theo mode
    python benchmark/compare_runs.py --detail     # Đọc full JSON từng run
"""

import json, argparse, sys
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
HISTORY_FILE = RESULTS_DIR / "history.jsonl"

C_BOLD  = "\033[1m"
C_GREEN = "\033[92m"
C_YELLOW= "\033[93m"
C_RED   = "\033[91m"
C_CYAN  = "\033[96m"
C_DIM   = "\033[2m"
C_X     = "\033[0m"

def color_tps(v):
    if v is None: return f"{C_DIM}—{C_X}"
    v = float(v)
    if v >= 300: return f"{C_GREEN}{v:.1f}{C_X}"
    if v >= 100: return f"{C_YELLOW}{v:.1f}{C_X}"
    return f"{C_RED}{v:.1f}{C_X}"

def color_ms(v):
    if v is None: return f"{C_DIM}—{C_X}"
    v = float(v)
    if v < 2000:   return f"{C_GREEN}{v:.0f}{C_X}"
    if v < 4000:   return f"{C_YELLOW}{v:.0f}{C_X}"
    return f"{C_RED}{v:.0f}{C_X}"

def color_lag(v):
    if v is None: return f"{C_DIM}—{C_X}"
    return f"{C_GREEN}✓{C_X}" if v is None else f"{C_YELLOW}{v}{C_X}"

def load_history(mode_filter=None, n=None):
    if not HISTORY_FILE.exists():
        print(f"[ERROR] {HISTORY_FILE} chưa có — chạy benchmark ít nhất 1 lần trước")
        sys.exit(1)
    entries = []
    for line in HISTORY_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line: continue
        try:
            e = json.loads(line)
            if mode_filter and e.get("mode") != mode_filter:
                continue
            entries.append(e)
        except json.JSONDecodeError:
            continue
    if n:
        entries = entries[-n:]
    return entries

def print_table(entries):
    if not entries:
        print("Không có dữ liệu phù hợp.")
        return

    cols = [
        ("Timestamp",      "ts",           22),
        ("Mode",           "mode",         10),
        ("Max E2E /s",     "max_e2e_tps",  11),
        ("Sus E2E /s",     "sus_tps",      11),
        ("Spark p50 ms",   "spark_p50_ms", 13),
        ("Spark p95 ms",   "spark_p95_ms", 13),
        ("Kafka /s",       "kafka_rate",   10),
        ("Parts",          "partitions",    6),
        ("Bottleneck",     "bottleneck",   18),
    ]

    # Header
    sep = "─" * (sum(w for _, _, w in cols) + len(cols) * 3 + 1)
    print(f"\n{C_BOLD}{sep}{C_X}")
    header = "  ".join(f"{C_BOLD}{name:<{w}}{C_X}" for name, _, w in cols)
    print(f"  {header}")
    print(f"{C_BOLD}{sep}{C_X}")

    # Rows
    for e in entries:
        row_vals = []
        for name, key, w in cols:
            v = e.get(key)
            if key == "max_e2e_tps": cell = color_tps(v)
            elif key == "sus_tps":   cell = color_tps(v)
            elif key in ("spark_p50_ms", "spark_p95_ms"): cell = color_ms(v)
            elif key == "bottleneck": cell = f"{C_RED}{v:<{w}}{C_X}" if v else f"{C_GREEN}{'none':<{w}}{C_X}"
            elif v is None:          cell = f"{C_DIM}{'—':<{w}}{C_X}"
            else:                    cell = f"{str(v):<{w}}"
            row_vals.append(cell)
        print("  " + "  ".join(row_vals))

    print(f"{C_BOLD}{sep}{C_X}")
    print(f"{C_DIM}  {len(entries)} lần chạy  ·  {HISTORY_FILE}{C_X}\n")

    # Best run summary
    valid = [e for e in entries if e.get("max_e2e_tps") is not None]
    if valid:
        best = max(valid, key=lambda x: x["max_e2e_tps"])
        print(f"{C_BOLD}Best run:{C_X} {C_CYAN}{best['ts']}{C_X} ({best['mode']}) "
              f"— Max E2E {C_GREEN}{best['max_e2e_tps']} rec/s{C_X}")

def print_detail(entries):
    """Đọc full JSON từ file .json cho từng entry."""
    for e in entries:
        fname = e.get("result_file")
        if not fname:
            continue
        fp = RESULTS_DIR / fname
        if not fp.exists():
            print(f"[WARN] {fp} không tồn tại, bỏ qua.")
            continue
        data = json.loads(fp.read_text(encoding="utf-8"))
        print(f"\n{'═' * 60}")
        print(f"{C_BOLD}{e['ts']}  mode={e['mode']}{C_X}")
        print(f"  Hardware : {e.get('hw')}")
        print(f"  Max E2E  : {data['summary']['max_e2e_tps']} rec/s")
        print(f"  Bottleneck: {data['summary']['bottleneck']}")
        sus = data.get('sustained', {})
        print(f"  Sustained: E2E={sus.get('e2e_tps')} rec/s  "
              f"Spark p50/p95={sus.get('spark_batch_avg_ms')}/{sus.get('spark_batch_p95_ms')}ms")
        print(f"  File     : {fp}")


def main():
    ap = argparse.ArgumentParser(description="So sánh các lần chạy benchmark")
    ap.add_argument("-n",       type=int,  default=None, help="N lần gần nhất")
    ap.add_argument("--mode",   type=str,  default=None, help="Lọc theo mode (quick/full/stress/...)")
    ap.add_argument("--detail", action="store_true",     help="In chi tiết từng run")
    args = ap.parse_args()

    entries = load_history(mode_filter=args.mode, n=args.n)

    if args.detail:
        print_detail(entries)
    else:
        print_table(entries)


if __name__ == "__main__":
    main()
