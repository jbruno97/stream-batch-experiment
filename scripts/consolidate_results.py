import argparse
import csv
import math
import statistics
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consolidate raw experiment CSV metrics")
    parser.add_argument("--raw-dir", default="results/raw")
    parser.add_argument("--out-dir", default="results/summary")
    return parser.parse_args()


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)


def to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def percentile(values: List[float], p: float) -> float:
    clean = sorted(v for v in values if not math.isnan(v))
    if not clean:
        return math.nan
    if len(clean) == 1:
        return clean[0]
    idx = (len(clean) - 1) * p
    low = int(math.floor(idx))
    high = int(math.ceil(idx))
    if low == high:
        return clean[low]
    frac = idx - low
    return clean[low] * (1 - frac) + clean[high] * frac


def summarize_by_scenario(rows: List[Dict[str, str]], scenario_col: str, metric_cols: List[str]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        if row.get("status") != "ok":
            continue
        grouped.setdefault(row.get(scenario_col, "unknown"), []).append(row)

    summaries: List[Dict[str, object]] = []
    for scenario, items in sorted(grouped.items()):
        summary: Dict[str, object] = {"scenario": scenario, "runs": len(items)}
        for metric in metric_cols:
            values = [to_float(item.get(metric, "")) for item in items]
            clean = [v for v in values if not math.isnan(v)]
            if not clean:
                summary[f"{metric}_mean"] = math.nan
                summary[f"{metric}_median"] = math.nan
                summary[f"{metric}_std"] = math.nan
                summary[f"{metric}_iqr"] = math.nan
                continue

            summary[f"{metric}_mean"] = statistics.mean(clean)
            summary[f"{metric}_median"] = statistics.median(clean)
            summary[f"{metric}_std"] = statistics.pstdev(clean) if len(clean) > 1 else 0.0
            q1 = percentile(clean, 0.25)
            q3 = percentile(clean, 0.75)
            summary[f"{metric}_iqr"] = q3 - q1
        summaries.append(summary)
    return summaries


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)

    batch_rows = read_csv_rows(raw_dir / "batch_runs.csv")
    stream_rows = read_csv_rows(raw_dir / "stream_runs.csv")

    batch_summary = summarize_by_scenario(
        batch_rows,
        "scenario",
        [
            "duration_sec",
            "throughput_rows_per_sec",
            "avg_cpu_pct",
            "avg_mem_mib",
        ],
    )
    stream_summary = summarize_by_scenario(
        stream_rows,
        "scenario",
        [
            "producer_achieved_rate_eps",
            "stream_throughput_rows_per_sec",
            "stream_avg_trigger_latency_ms",
            "avg_cpu_pct",
            "avg_mem_mib",
        ],
    )

    write_csv(out_dir / "batch_summary.csv", batch_summary)
    write_csv(out_dir / "stream_summary.csv", stream_summary)

    print(f"Batch summary: {out_dir / 'batch_summary.csv'}")
    print(f"Stream summary: {out_dir / 'stream_summary.csv'}")


if __name__ == "__main__":
    main()
