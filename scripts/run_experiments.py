import argparse
import csv
import json
import os
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


METRICS_PREFIX = "METRICS_JSON:"
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch vs stream experiment scenarios")
    parser.add_argument("--batch-repetitions", type=int, default=5)
    parser.add_argument("--stream-repetitions", type=int, default=5)
    parser.add_argument("--warmup", action="store_true")
    parser.add_argument("--stream-duration-sec", type=int, default=30)
    parser.add_argument("--stream-trigger-sec", type=int, default=2)
    parser.add_argument("--stats-interval-sec", type=float, default=1.0)
    parser.add_argument("--python", default="")
    parser.add_argument("--topic", default="input-topic")
    return parser.parse_args()


def resolve_python(python_arg: str, base_dir: Path) -> str:
    if python_arg:
        return python_arg
    venv_python = base_dir / "venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        return str(venv_python)
    return "python"


def run_command(cmd: List[str], timeout: Optional[int] = None) -> CommandResult:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


def parse_metrics(output_text: str) -> Dict[str, object]:
    for line in output_text.splitlines():
        if line.startswith(METRICS_PREFIX):
            payload = line[len(METRICS_PREFIX) :].strip()
            return json.loads(payload)
    return {}


def parse_percent(value: str) -> float:
    clean = value.strip().strip('"').replace("%", "")
    try:
        return float(clean)
    except ValueError:
        return 0.0


def parse_size_to_mib(size_text: str) -> float:
    clean = size_text.strip().strip('"').upper()
    units = [
        ("TIB", 1024.0 * 1024.0),
        ("GIB", 1024.0),
        ("MIB", 1.0),
        ("KIB", 1 / 1024),
        ("TB", 1024.0 * 1024.0),
        ("GB", 1024.0),
        ("MB", 1.0),
        ("KB", 1 / 1024),
        ("B", 1 / (1024 * 1024)),
    ]

    for unit, factor in units:
        if clean.endswith(unit):
            number = clean[: -len(unit)].strip()
            try:
                return float(number) * factor
            except ValueError:
                return 0.0
    return 0.0


def collect_docker_stats(container_names: List[str], interval_sec: float, stop_event: threading.Event) -> List[Dict[str, float]]:
    samples: List[Dict[str, float]] = []
    if not container_names:
        return samples

    while not stop_event.is_set():
        cmd = [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}},{{.CPUPerc}},{{.MemUsage}}",
        ] + container_names
        result = run_command(cmd, timeout=30)
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                parts = line.strip().split(",")
                if len(parts) != 3:
                    continue
                name, cpu_text, mem_text = parts
                mem_used_text = mem_text.split("/")[0].strip()
                samples.append(
                    {
                        "container": name.strip().strip('"'),
                        "cpu_pct": parse_percent(cpu_text),
                        "mem_mib": parse_size_to_mib(mem_used_text),
                        "timestamp": time.time(),
                    }
                )
        time.sleep(interval_sec)
    return samples


def summarize_stats(samples: List[Dict[str, float]]) -> Dict[str, float]:
    if not samples:
        return {
            "avg_cpu_pct": 0.0,
            "max_cpu_pct": 0.0,
            "avg_mem_mib": 0.0,
            "max_mem_mib": 0.0,
        }

    cpu_values = [item["cpu_pct"] for item in samples]
    mem_values = [item["mem_mib"] for item in samples]
    return {
        "avg_cpu_pct": statistics.mean(cpu_values),
        "max_cpu_pct": max(cpu_values),
        "avg_mem_mib": statistics.mean(mem_values),
        "max_mem_mib": max(mem_values),
    }


def append_csv(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def ensure_topic(topic: str) -> None:
    cmd = [
        "docker",
        "exec",
        "stream-batch-experiment-kafka-1",
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--bootstrap-server",
        "localhost:29092",
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]
    result = run_command(cmd, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to ensure topic {topic}: {result.stderr or result.stdout}")


def ensure_compose_up() -> None:
    result = run_command(["docker", "compose", "up", "-d"], timeout=180)
    if result.returncode != 0:
        raise RuntimeError(f"docker compose up -d failed: {result.stderr or result.stdout}")


def local_path_to_container_data(local_path: Path) -> str:
    path_str = local_path.as_posix()
    idx = path_str.find("/data/")
    if idx == -1:
        raise ValueError(f"Dataset must be inside data/: {local_path}")
    return "/opt" + path_str[idx:]


def run_batch_once(
    scenario: str,
    dataset_path: Path,
    run_id: str,
    stats_interval_sec: float,
    output_csv: Path,
) -> None:
    started = datetime.now(timezone.utc)
    stop_event = threading.Event()
    stats_samples: List[Dict[str, float]] = []

    def stats_worker() -> None:
        stats_samples.extend(
            collect_docker_stats(
                ["spark-master", "spark-worker", "stream-batch-experiment-kafka-1"],
                stats_interval_sec,
                stop_event,
            )
        )

    thread = threading.Thread(target=stats_worker, daemon=True)
    thread.start()

    container_input = local_path_to_container_data(dataset_path)
    cmd = [
        "docker",
        "exec",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/opt/jobs/batch_job.py",
        "--input",
        container_input,
        "--scenario",
        scenario,
        "--run-id",
        run_id,
    ]
    result = run_command(cmd, timeout=300)

    stop_event.set()
    thread.join(timeout=5)
    stat_summary = summarize_stats(stats_samples)
    ended = datetime.now(timezone.utc)
    metrics = parse_metrics(result.stdout)

    row = {
        "run_id": run_id,
        "scenario": scenario,
        "dataset_path": str(dataset_path),
        "status": "ok" if result.returncode == 0 else "error",
        "row_count": metrics.get("row_count", 0),
        "duration_sec": metrics.get("duration_sec", 0.0),
        "throughput_rows_per_sec": metrics.get("throughput_rows_per_sec", 0.0),
        "avg_cpu_pct": stat_summary["avg_cpu_pct"],
        "max_cpu_pct": stat_summary["max_cpu_pct"],
        "avg_mem_mib": stat_summary["avg_mem_mib"],
        "max_mem_mib": stat_summary["max_mem_mib"],
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "error": (result.stderr or result.stdout).strip() if result.returncode != 0 else "",
    }
    append_csv(output_csv, row)


def run_stream_once(
    scenario: str,
    rate_eps: int,
    run_id: str,
    stream_duration_sec: int,
    stream_trigger_sec: int,
    stats_interval_sec: float,
    python_cmd: str,
    topic: str,
    output_csv: Path,
) -> None:
    started = datetime.now(timezone.utc)
    stop_event = threading.Event()
    stats_samples: List[Dict[str, float]] = []

    def stats_worker() -> None:
        stats_samples.extend(
            collect_docker_stats(
                ["spark-master", "spark-worker", "stream-batch-experiment-kafka-1"],
                stats_interval_sec,
                stop_event,
            )
        )

    thread = threading.Thread(target=stats_worker, daemon=True)
    thread.start()

    run_topic = f"{topic}-{run_id}".replace("_", "-")
    ensure_topic(run_topic)

    stream_cmd = [
        "docker",
        "exec",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.jars.ivy=/tmp/.ivy",
        "--packages",
        KAFKA_CONNECTOR_PACKAGE,
        "/opt/jobs/stream_job.py",
        "--bootstrap-servers",
        "kafka:9092",
        "--topic",
        run_topic,
        "--duration-sec",
        str(stream_duration_sec),
        "--trigger-sec",
        str(stream_trigger_sec),
        "--starting-offsets",
        "earliest",
        "--scenario",
        scenario,
        "--run-id",
        run_id,
    ]
    stream_proc = subprocess.Popen(stream_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    time.sleep(5)
    producer_cmd = [
        python_cmd,
        "producer/kafka_producer.py",
        "--bootstrap",
        "localhost:29092",
        "--topic",
        run_topic,
        "--rate",
        str(rate_eps),
        "--duration",
        str(stream_duration_sec),
        "--scenario",
        scenario,
        "--run-id",
        run_id,
        "--seed",
        "42",
    ]
    producer_result = run_command(producer_cmd, timeout=stream_duration_sec + 90)
    stream_stdout, stream_stderr = stream_proc.communicate(timeout=stream_duration_sec + 180)
    stream_return_code = stream_proc.returncode

    stop_event.set()
    thread.join(timeout=5)
    stat_summary = summarize_stats(stats_samples)
    ended = datetime.now(timezone.utc)
    stream_metrics = parse_metrics(stream_stdout)
    producer_metrics = parse_metrics(producer_result.stdout)

    status = "ok" if stream_return_code == 0 and producer_result.returncode == 0 else "error"
    error_text = ""
    if status != "ok":
        error_text = "\n".join([producer_result.stderr.strip(), stream_stderr.strip()]).strip()

    row = {
        "run_id": run_id,
        "scenario": scenario,
        "configured_rate_eps": rate_eps,
        "status": status,
        "producer_events_sent": producer_metrics.get("events_sent", 0),
        "producer_achieved_rate_eps": producer_metrics.get("achieved_rate_eps", 0.0),
        "stream_total_input_rows": stream_metrics.get("total_input_rows", 0),
        "stream_micro_batch_count": stream_metrics.get("micro_batch_count", 0),
        "stream_throughput_rows_per_sec": stream_metrics.get("throughput_rows_per_sec", 0.0),
        "stream_avg_input_rows_per_sec": stream_metrics.get("avg_input_rows_per_sec", 0.0),
        "stream_avg_processed_rows_per_sec": stream_metrics.get("avg_processed_rows_per_sec", 0.0),
        "stream_avg_trigger_latency_ms": stream_metrics.get("avg_trigger_latency_ms", 0.0),
        "avg_cpu_pct": stat_summary["avg_cpu_pct"],
        "max_cpu_pct": stat_summary["max_cpu_pct"],
        "avg_mem_mib": stat_summary["avg_mem_mib"],
        "max_mem_mib": stat_summary["max_mem_mib"],
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "error": error_text,
    }
    append_csv(output_csv, row)


def run_warmup(python_cmd: str, topic: str) -> None:
    warmup_run_id = "warmup-stream"
    warmup_topic = f"{topic}-{warmup_run_id}"
    ensure_topic(warmup_topic)
    stream_cmd = [
        "docker",
        "exec",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.jars.ivy=/tmp/.ivy",
        "--packages",
        KAFKA_CONNECTOR_PACKAGE,
        "/opt/jobs/stream_job.py",
        "--bootstrap-servers",
        "kafka:9092",
        "--topic",
        warmup_topic,
        "--duration-sec",
        "10",
        "--trigger-sec",
        "2",
        "--scenario",
        "warmup",
        "--run-id",
        warmup_run_id,
    ]
    proc = subprocess.Popen(stream_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(3)
    producer_result = run_command(
        [
            python_cmd,
            "producer/kafka_producer.py",
            "--bootstrap",
            "localhost:29092",
            "--topic",
            warmup_topic,
            "--rate",
            "50",
            "--duration",
            "8",
            "--scenario",
            "warmup",
            "--run-id",
            warmup_run_id,
            "--seed",
            "42",
        ],
        timeout=60,
    )
    proc.communicate(timeout=90)
    if producer_result.returncode != 0:
        raise RuntimeError(f"Warm-up producer failed: {producer_result.stderr or producer_result.stdout}")


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parents[1]
    os.chdir(base_dir)

    results_dir = base_dir / "results" / "raw"
    batch_csv = results_dir / "batch_runs.csv"
    stream_csv = results_dir / "stream_runs.csv"
    python_cmd = resolve_python(args.python, base_dir)

    batch_scenarios = {
        "small": base_dir / "data" / "small" / "input.csv",
        "medium": base_dir / "data" / "medium" / "input.csv",
    }
    stream_scenarios = [200, 500, 1000]

    ensure_compose_up()
    ensure_topic(args.topic)

    if args.warmup:
        run_warmup(python_cmd, args.topic)

    for scenario_name, dataset_path in batch_scenarios.items():
        if not dataset_path.exists():
            print(f"Skipping batch scenario '{scenario_name}' (file not found: {dataset_path})")
            continue
        for run_idx in range(1, args.batch_repetitions + 1):
            run_id = f"batch-{scenario_name}-r{run_idx}"
            print(f"Running {run_id}")
            run_batch_once(
                scenario=scenario_name,
                dataset_path=dataset_path,
                run_id=run_id,
                stats_interval_sec=args.stats_interval_sec,
                output_csv=batch_csv,
            )

    for rate in stream_scenarios:
        scenario_name = f"stream-{rate}eps"
        for run_idx in range(1, args.stream_repetitions + 1):
            run_id = f"{scenario_name}-r{run_idx}"
            print(f"Running {run_id}")
            run_stream_once(
                scenario=scenario_name,
                rate_eps=rate,
                run_id=run_id,
                stream_duration_sec=args.stream_duration_sec,
                stream_trigger_sec=args.stream_trigger_sec,
                stats_interval_sec=args.stats_interval_sec,
                python_cmd=python_cmd,
                topic=args.topic,
                output_csv=stream_csv,
            )

    print(f"Batch results: {batch_csv}")
    print(f"Stream results: {stream_csv}")


if __name__ == "__main__":
    main()
