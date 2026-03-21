import argparse
import csv
import threading
import time
from pathlib import Path
from typing import Dict, List

import docker


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


def _safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def parse_container_stats(container) -> Dict[str, float]:
    # O Docker expõe contadores cumulativos; aqui eles são normalizados nas métricas usadas no experimento.
    stats = container.stats(stream=False)
    cpu_total = float(stats["cpu_stats"]["cpu_usage"].get("total_usage", 0.0))
    pre_cpu_total = float(stats["precpu_stats"]["cpu_usage"].get("total_usage", 0.0))
    system_total = float(stats["cpu_stats"].get("system_cpu_usage", 0.0))
    pre_system_total = float(stats["precpu_stats"].get("system_cpu_usage", 0.0))
    online_cpus = float(stats["cpu_stats"].get("online_cpus") or 1.0)
    cpu_delta = cpu_total - pre_cpu_total
    system_delta = system_total - pre_system_total
    cpu_pct = _safe_div(cpu_delta, system_delta) * online_cpus * 100.0

    memory_usage = float(stats["memory_stats"].get("usage", 0.0)) / (1024**2)
    memory_limit = float(stats["memory_stats"].get("limit", 0.0)) / (1024**2)
    memory_pct = _safe_div(memory_usage, memory_limit) * 100.0

    networks = stats.get("networks", {})
    net_rx = sum(float(item.get("rx_bytes", 0.0)) for item in networks.values())
    net_tx = sum(float(item.get("tx_bytes", 0.0)) for item in networks.values())

    blkio_entries = stats.get("blkio_stats", {}).get("io_service_bytes_recursive", [])
    disk_read = sum(float(item.get("value", 0.0)) for item in blkio_entries if item.get("op") == "Read")
    disk_write = sum(float(item.get("value", 0.0)) for item in blkio_entries if item.get("op") == "Write")

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
    client = docker.from_env()
    samples: List[Dict[str, float | str]] = []
    while not stop_event.is_set():
        timestamp = time.time()
        try:
            containers = [client.containers.get(name) for name in container_names]
        except docker.errors.NotFound as exc:
            raise RuntimeError(
                "Container monitor could not find a required container. "
                f"Configured names: {', '.join(container_names)}. Original error: {exc}"
            ) from exc
        for container in containers:
            sample = parse_container_stats(container)
            samples.append(
                {
                    "run_id": run_id,
                    "scenario": scenario,
                    "scenario_category": scenario_category,
                    "mode": mode,
                    "container": container.name,
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
