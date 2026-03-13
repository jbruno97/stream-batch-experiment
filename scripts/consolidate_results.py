import argparse
import csv
import math
import statistics
from pathlib import Path
from typing import Dict, Iterable, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consolidate scientific experiment results")
    parser.add_argument("--raw-dir", default="results/raw")
    parser.add_argument("--out-dir", default="results/summary")
    return parser.parse_args()


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def percentile(values: Iterable[float], p: float) -> float:
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


def summarize_numeric(values: List[float], prefix: str) -> Dict[str, float]:
    # Esses agregados são a base para plots, intervalos de confiança e discussão textual posterior.
    clean = [v for v in values if not math.isnan(v)]
    if not clean:
        return {f"{prefix}_{suffix}": math.nan for suffix in ["mean", "median", "std", "min", "max", "iqr", "ci95"]}
    std = statistics.stdev(clean) if len(clean) > 1 else 0.0
    ci95 = 1.96 * std / math.sqrt(len(clean)) if len(clean) > 1 else 0.0
    q1 = percentile(clean, 0.25)
    q3 = percentile(clean, 0.75)
    return {
        f"{prefix}_mean": statistics.mean(clean),
        f"{prefix}_median": statistics.median(clean),
        f"{prefix}_std": std,
        f"{prefix}_min": min(clean),
        f"{prefix}_max": max(clean),
        f"{prefix}_iqr": q3 - q1,
        f"{prefix}_ci95": ci95,
    }


def summarize_runs(rows: List[Dict[str, str]], metrics: List[str], fixed_cols: List[str]) -> List[Dict[str, object]]:
    # Os sumários são produzidos no nível do cenário porque a repetição é a unidade estatística do experimento.
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        if row.get("status") != "ok":
            continue
        grouped.setdefault(row.get("scenario", "unknown"), []).append(row)

    summaries: List[Dict[str, object]] = []
    for scenario, items in sorted(grouped.items()):
        summary: Dict[str, object] = {"scenario": scenario, "runs": len(items)}
        for col in fixed_cols:
            summary[col] = items[0].get(col, "")
        for metric in metrics:
            summary.update(summarize_numeric([to_float(item.get(metric, "")) for item in items], metric))
        summaries.append(summary)
    return summaries


def summarize_container_metrics(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    # Os sumários por container mantêm o comportamento da infraestrutura separado por cenário e serviço.
    grouped: Dict[tuple[str, str], List[Dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault((row.get("scenario", "unknown"), row.get("container", "unknown")), []).append(row)

    summaries: List[Dict[str, object]] = []
    metrics = ["cpu_pct", "mem_mib", "mem_pct", "net_rx_bytes", "net_tx_bytes", "disk_read_bytes", "disk_write_bytes"]
    for (scenario, container), items in sorted(grouped.items()):
        row: Dict[str, object] = {
            "scenario": scenario,
            "container": container,
            "scenario_category": items[0].get("scenario_category", ""),
            "mode": items[0].get("mode", ""),
            "samples": len(items),
        }
        for metric in metrics:
            row.update(summarize_numeric([to_float(item.get(metric, "")) for item in items], metric))
        summaries.append(row)
    return summaries


def summarize_microbatches(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    # Os sumários de micro-batches expõem latência, backpressure e estabilidade de processamento no stream.
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row.get("scenario", "unknown"), []).append(row)

    metrics = [
        "num_input_rows",
        "input_rows_per_second",
        "processed_rows_per_second",
        "avg_latency_s",
        "max_latency_s",
        "duration_trigger_execution_ms",
        "duration_query_planning_ms",
        "duration_add_batch_ms",
        "backpressure_ms",
        "state_operator_rows_total",
    ]
    summaries: List[Dict[str, object]] = []
    for scenario, items in sorted(grouped.items()):
        row: Dict[str, object] = {
            "scenario": scenario,
            "scenario_category": items[0].get("scenario_category", ""),
            "micro_batches": len(items),
            "distinct_runs": len({item.get("run_id", "") for item in items}),
            "workers": items[0].get("workers", ""),
        }
        for metric in metrics:
            row.update(summarize_numeric([to_float(item.get(metric, "")) for item in items], metric))
        summaries.append(row)
    return summaries


def main() -> None:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)

    batch_rows = read_csv_rows(raw_dir / "batch_runs.csv")
    stream_rows = read_csv_rows(raw_dir / "stream_runs.csv")
    container_rows = read_csv_rows(raw_dir / "container_metrics.csv")
    microbatch_rows = read_csv_rows(raw_dir / "stream_microbatches.csv")

    batch_summary = summarize_runs(
        batch_rows,
        ["duration_sec", "row_count", "throughput_rows_per_sec", "avg_cpu_pct", "max_cpu_pct", "avg_mem_mib", "max_mem_mib"],
        ["mode", "scenario_category", "dataset_label", "workers", "spark_version"],
    )
    stream_summary = summarize_runs(
        stream_rows,
        [
            "configured_rate_eps",
            "producer_events_sent",
            "producer_achieved_rate_eps",
            "stream_total_input_rows",
            "stream_micro_batch_count",
            "stream_throughput_rows_per_sec",
            "stream_avg_input_rows_per_sec",
            "stream_avg_processed_rows_per_sec",
            "stream_avg_trigger_latency_ms",
            "stream_avg_event_latency_s",
            "stream_max_event_latency_s",
            "stream_avg_backpressure_ms",
            "avg_cpu_pct",
            "max_cpu_pct",
            "avg_mem_mib",
            "max_mem_mib",
        ],
        ["mode", "scenario_category", "dataset_label", "workers", "trigger_sec", "spark_version"],
    )
    container_summary = summarize_container_metrics(container_rows)
    microbatch_summary = summarize_microbatches(microbatch_rows)

    write_csv(out_dir / "batch_summary.csv", batch_summary)
    write_csv(out_dir / "stream_summary.csv", stream_summary)
    write_csv(out_dir / "container_summary.csv", container_summary)
    write_csv(out_dir / "stream_microbatch_summary.csv", microbatch_summary)

    print(out_dir / "batch_summary.csv")
    print(out_dir / "stream_summary.csv")
    print(out_dir / "container_summary.csv")
    print(out_dir / "stream_microbatch_summary.csv")


if __name__ == "__main__":
    main()
