import argparse
import csv
import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect container metrics from Docker")
    parser.add_argument("--containers", nargs="+", required=True)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--duration-sec", type=float, default=30.0)
    parser.add_argument("--output", default="results/raw/container_metrics.csv")
    parser.add_argument("--scenario", default="manual")
    parser.add_argument("--run-id", default="manual-run")
    parser.add_argument("--mode", default="manual")
    return parser.parse_args()

def run_command(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True)


def parse_size_to_bytes(raw: str) -> float:
    value = raw.strip()
    if not value or value == "--":
        return 0.0
    normalized = value.replace(" ", "").replace("iB", "ib")
    suffixes = {
        "b": 1.0,
        "kb": 1000.0,
        "mb": 1000.0**2,
        "gb": 1000.0**3,
        "tb": 1000.0**4,
        "kib": 1024.0,
        "mib": 1024.0**2,
        "gib": 1024.0**3,
        "tib": 1024.0**4,
    }
    for suffix, multiplier in sorted(suffixes.items(), key=lambda item: len(item[0]), reverse=True):
        if normalized.lower().endswith(suffix):
            number = normalized[: -len(suffix)] or "0"
            return float(number) * multiplier
    return float(normalized)


def split_dual_metric(raw: str) -> tuple[float, float]:
    parts = [part.strip() for part in raw.split("/", maxsplit=1)]
    if len(parts) != 2:
        return 0.0, 0.0
    return parse_size_to_bytes(parts[0]), parse_size_to_bytes(parts[1])


def parse_percent(raw: str) -> float:
    return float(raw.replace("%", "").strip() or 0.0)


def fetch_container_stats(container_names: List[str]) -> Dict[str, Dict[str, object]]:
    result = run_command(
        ["docker", "stats", "--no-stream", "--format", "{{ json . }}", *container_names]
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Container monitor could not collect Docker stats. "
            f"Configured names: {', '.join(container_names)}. Error: {result.stderr or result.stdout}"
        )
    rows: Dict[str, Dict[str, object]] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        name = str(payload.get("Name", "")).strip()
        if name:
            rows[name] = payload
    return rows


def parse_container_stats(stats: Dict[str, object]) -> Dict[str, float]:
    cpu_pct = parse_percent(str(stats.get("CPUPerc", "0")))
    memory_usage_bytes, memory_limit_bytes = split_dual_metric(str(stats.get("MemUsage", "0 / 0")))
    memory_usage = memory_usage_bytes / (1024**2)
    memory_limit = memory_limit_bytes / (1024**2)
    memory_pct = parse_percent(str(stats.get("MemPerc", "0")))
    net_rx, net_tx = split_dual_metric(str(stats.get("NetIO", "0 / 0")))
    disk_read, disk_write = split_dual_metric(str(stats.get("BlockIO", "0 / 0")))

    return {
        "cpu_pct": cpu_pct,
        "mem_mib": memory_usage,
        "mem_limit_mib": memory_limit,
        "mem_pct": memory_pct,
        "net_rx_bytes": net_rx,
        "net_tx_bytes": net_tx,
        "disk_read_bytes": disk_read,
        "disk_write_bytes": disk_write,
    }


def collect_container_metrics(
    container_names: List[str],
    interval_sec: float,
    stop_event: threading.Event,
    run_id: str,
    scenario: str,
    scenario_category: str,
    mode: str,
) -> List[Dict[str, float | str]]:
    # A amostragem em intervalos fixos cria uma série temporal de recursos agregável por cenário e repetição.
    samples: List[Dict[str, float | str]] = []
    while not stop_event.is_set():
        timestamp = time.time()
        stats_by_name = fetch_container_stats(container_names)
        missing = [name for name in container_names if name not in stats_by_name]
        if missing:
            raise RuntimeError(
                "Container monitor could not find a required container. "
                f"Configured names: {', '.join(container_names)}. Missing stats for: {', '.join(missing)}"
            )
        for container_name in container_names:
            sample = parse_container_stats(stats_by_name[container_name])
            samples.append(
                {
                    "run_id": run_id,
                    "scenario": scenario,
                    "scenario_category": scenario_category,
                    "mode": mode,
                    "container": container_name,
                    "sample_timestamp_unix": timestamp,
                    **sample,
                }
            )
        time.sleep(interval_sec)
    return samples


def append_rows(path: Path, rows: List[Dict[str, float | str]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    stop_event = threading.Event()

    def stopper() -> None:
        time.sleep(args.duration_sec)
        stop_event.set()

    stopper_thread = threading.Thread(target=stopper, daemon=True)
    stopper_thread.start()
    rows = collect_container_metrics(
        container_names=args.containers,
        interval_sec=args.interval_sec,
        stop_event=stop_event,
        run_id=args.run_id,
        scenario=args.scenario,
        scenario_category="manual",
        mode=args.mode,
    )
    append_rows(Path(args.output), rows)
    print(args.output)


if __name__ == "__main__":
    main()
