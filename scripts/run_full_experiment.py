import argparse
import csv
import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import docker

from capture_environment import write_environment_file
from container_monitor import collect_container_metrics


METRICS_PREFIX = "METRICS_JSON:"
STREAM_PROGRESS_PREFIX = "STREAM_PROGRESS_JSON:"
STREAM_BATCH_METRICS_PREFIX = "STREAM_BATCH_METRICS_JSON:"
KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SPARK_MASTER_CONTAINER = "spark-master"
KAFKA_CONTAINER = "kafka"


@dataclass(frozen=True)
class BatchScenario:
    scenario_id: str
    category: str
    dataset_dir: str
    workers: int


@dataclass(frozen=True)
class StreamScenario:
    scenario_id: str
    category: str
    rate_eps: int
    trigger_sec: int
    workers: int
    data_path: str = "data/samples/200mb"


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


BATCH_SCENARIOS = [
    # Bloco de carga: varia somente o volume, com um worker fixo.
    BatchScenario("BL1", "carga", "data/samples/200mb", 1),
    BatchScenario("BL2", "carga", "data/samples/1gb", 1),
    BatchScenario("BL3", "carga", "data/samples/3gb", 1),
    BatchScenario("BL4", "carga", "data/samples/10gb", 1),
    # Bloco de escalabilidade: mantém a mesma carga e varia apenas a quantidade de workers.
    BatchScenario("BS1", "escalabilidade", "data/samples/200mb", 1),
    BatchScenario("BS2", "escalabilidade", "data/samples/200mb", 2),
    BatchScenario("BS3", "escalabilidade", "data/samples/200mb", 4),
    BatchScenario("BS4", "escalabilidade", "data/samples/1gb", 1),
    BatchScenario("BS5", "escalabilidade", "data/samples/1gb", 2),
    BatchScenario("BS6", "escalabilidade", "data/samples/1gb", 4),
    BatchScenario("BS7", "escalabilidade", "data/samples/3gb", 1),
    BatchScenario("BS8", "escalabilidade", "data/samples/3gb", 2),
    BatchScenario("BS9", "escalabilidade", "data/samples/3gb", 4),
    BatchScenario("BS10", "escalabilidade", "data/samples/10gb", 1),
    BatchScenario("BS11", "escalabilidade", "data/samples/10gb", 2),
    BatchScenario("BS12", "escalabilidade", "data/samples/10gb", 4),
]

STREAM_SCENARIOS = [
    # Bloco de carga: varia somente a taxa de entrada, com trigger e workers fixos.
    StreamScenario("SL1", "carga", 200, 1, 1),
    StreamScenario("SL2", "carga", 500, 1, 1),
    StreamScenario("SL3", "carga", 1000, 1, 1),
    StreamScenario("SL4", "carga", 2000, 1, 1),
    # Bloco de escalabilidade: mantém a mesma carga e trigger, variando apenas workers.
    StreamScenario("SS1", "escalabilidade", 200, 1, 1),
    StreamScenario("SS2", "escalabilidade", 200, 1, 2),
    StreamScenario("SS3", "escalabilidade", 200, 1, 4),
    StreamScenario("SS4", "escalabilidade", 500, 1, 1),
    StreamScenario("SS5", "escalabilidade", 500, 1, 2),
    StreamScenario("SS6", "escalabilidade", 500, 1, 4),
    StreamScenario("SS7", "escalabilidade", 1000, 1, 1),
    StreamScenario("SS8", "escalabilidade", 1000, 1, 2),
    StreamScenario("SS9", "escalabilidade", 1000, 1, 4),
    StreamScenario("SS10", "escalabilidade", 2000, 1, 1),
    StreamScenario("SS11", "escalabilidade", 2000, 1, 2),
    StreamScenario("SS12", "escalabilidade", 2000, 1, 4),
    # Bloco de trigger: mantém a mesma carga e varia apenas a cadência de micro-batch.
    StreamScenario("ST1", "trigger", 1000, 1, 1),
    StreamScenario("ST2", "trigger", 1000, 2, 1),
    StreamScenario("ST3", "trigger", 1000, 5, 1),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full scientific batch vs stream experiment")
    parser.add_argument("--batch-repetitions", type=int, default=30)
    parser.add_argument("--stream-repetitions", type=int, default=30)
    parser.add_argument("--stream-duration-sec", type=int, default=30)
    parser.add_argument("--stats-interval-sec", type=float, default=1.0)
    parser.add_argument("--warmup", action="store_true")
    parser.add_argument("--python", default="")
    parser.add_argument("--topic-prefix", default="taxi-topic")
    parser.add_argument("--skip-batch", action="store_true")
    parser.add_argument("--skip-stream", action="store_true")
    return parser.parse_args()


def resolve_python(python_arg: str, base_dir: Path) -> str:
    if python_arg:
        return python_arg
    active_venv = os.environ.get("VIRTUAL_ENV", "").strip()
    candidates = []
    if active_venv:
        candidates.extend(
            [
                Path(active_venv) / "bin" / "python",
                Path(active_venv) / "Scripts" / "python.exe",
            ]
        )
    candidates.extend(
        [
            base_dir / ".venv" / "bin" / "python",
            base_dir / ".venv" / "Scripts" / "python.exe",
            base_dir / "venv" / "bin" / "python",
            base_dir / "venv" / "Scripts" / "python.exe",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return "python"


def run_command(cmd: List[str], timeout: Optional[int] = None) -> CommandResult:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


def append_csv(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def append_csv_rows(path: Path, rows: List[Dict[str, object]]) -> None:
    for row in rows:
        append_csv(path, row)


def parse_metrics(output_text: str) -> Dict[str, object]:
    for line in output_text.splitlines():
        if line.startswith(METRICS_PREFIX):
            return json.loads(line[len(METRICS_PREFIX) :].strip())
    return {}


def parse_prefixed_json_lines(output_text: str, prefix: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for line in output_text.splitlines():
        if line.startswith(prefix):
            rows.append(json.loads(line[len(prefix) :].strip()))
    return rows


def summarize_container_samples(samples: List[Dict[str, object]]) -> Dict[str, float]:
    if not samples:
        return {
            "avg_cpu_pct": 0.0,
            "max_cpu_pct": 0.0,
            "avg_mem_mib": 0.0,
            "max_mem_mib": 0.0,
        }
    cpu_values = [float(sample.get("cpu_pct", 0.0)) for sample in samples]
    mem_values = [float(sample.get("mem_mib", 0.0)) for sample in samples]
    return {
        "avg_cpu_pct": sum(cpu_values) / len(cpu_values),
        "max_cpu_pct": max(cpu_values),
        "avg_mem_mib": sum(mem_values) / len(mem_values),
        "max_mem_mib": max(mem_values),
    }


def average_metric(rows: List[Dict[str, object]], key: str) -> float:
    values = [float(row.get(key, 0.0) or 0.0) for row in rows]
    return (sum(values) / len(values)) if values else 0.0


def get_git_commit() -> str:
    result = run_command(["git", "rev-parse", "HEAD"], timeout=15)
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip() or "unknown"


def ensure_compose_up(worker_count: int) -> None:
    result = run_command(["docker", "compose", "up", "-d", "--scale", f"spark-worker={worker_count}"], timeout=240)
    if result.returncode != 0:
        raise RuntimeError(f"docker compose up failed: {result.stderr or result.stdout}")


def list_worker_containers() -> List[str]:
    result = run_command(
        ["docker", "ps", "--filter", "label=com.docker.compose.service=spark-worker", "--format", "{{.Names}}"],
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list spark workers: {result.stderr or result.stdout}")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def experiment_container_names() -> List[str]:
    return [SPARK_MASTER_CONTAINER, *list_worker_containers(), KAFKA_CONTAINER]


def wait_for_named_containers(container_names: List[str], timeout_sec: float = 30.0, poll_sec: float = 1.0) -> None:
    client = docker.from_env()
    deadline = time.time() + timeout_sec
    missing = list(container_names)
    while time.time() < deadline:
        current_missing: List[str] = []
        for name in container_names:
            try:
                client.containers.get(name)
            except docker.errors.NotFound:
                current_missing.append(name)
        if not current_missing:
            return
        missing = current_missing
        time.sleep(poll_sec)
    raise RuntimeError(
        "Required Docker containers not found after waiting "
        f"{timeout_sec:.0f}s: {', '.join(missing)}. "
        "Run `docker compose up -d` and confirm the services are healthy."
    )


def ensure_kafka_ready() -> None:
    cmd = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "kafka-broker-api-versions",
        "--bootstrap-server",
        "kafka:9092",
    ]
    last_error = ""
    for _ in range(30):
        result = run_command(cmd, timeout=30)
        if result.returncode == 0:
            return
        last_error = result.stderr or result.stdout
        time.sleep(2)
    raise RuntimeError(f"Kafka broker not ready: {last_error}")


def ensure_topic(topic: str) -> None:
    ensure_kafka_ready()
    result = run_command(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--bootstrap-server",
            "kafka:9092",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to ensure topic {topic}: {result.stderr or result.stdout}")


def ensure_taxi_samples(base_dir: Path) -> None:
    # O desenho experimental depende das quatro amostras materializadas antes das execuções.
    required = ["200mb", "1gb", "3gb", "10gb"]
    if all(
        (base_dir / "data" / "samples" / folder).exists()
        and any((base_dir / "data" / "samples" / folder).rglob("*.parquet"))
        for folder in required
    ):
        return
    raw_dir = base_dir / "data" / "raw" / "nyc_taxi"
    if not raw_dir.exists():
        raise RuntimeError(f"Raw dataset not found: {raw_dir}")
    result = run_command(
        [
            "docker",
            "exec",
            SPARK_MASTER_CONTAINER,
            "/opt/spark/bin/spark-submit",
            "/opt/scripts/create_samples.py",
            "--input",
            "/opt/data/raw/nyc_taxi",
            "--output-root",
            "/opt/data/samples",
        ],
        timeout=2400,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create samples: {result.stderr or result.stdout}")


def local_path_to_container_data(path: Path) -> str:
    path_str = path.as_posix()
    idx = path_str.find("/data/")
    if idx == -1:
        raise ValueError(f"Dataset must be inside data/: {path}")
    return "/opt" + path_str[idx:]


def start_container_monitor(
    container_names: List[str],
    interval_sec: float,
    run_id: str,
    scenario: str,
    scenario_category: str,
    mode: str,
) -> tuple[threading.Event, List[Dict[str, object]], threading.Thread]:
    # A telemetria dos containers é coletada em paralelo para preservar a série temporal de cada repetição.
    stop_event = threading.Event()
    samples: List[Dict[str, object]] = []

    def monitor() -> None:
        samples.extend(
            collect_container_metrics(
                container_names,
                interval_sec,
                stop_event,
                run_id,
                scenario,
                scenario_category,
                mode,
            )
        )

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    return stop_event, samples, thread


def scale_workers(worker_count: int) -> List[str]:
    # Cada cenário controla explicitamente a quantidade de workers como variável independente do experimento.
    ensure_compose_up(worker_count)
    time.sleep(5)
    workers = list_worker_containers()
    if len(workers) < worker_count:
        raise RuntimeError(f"Expected {worker_count} spark workers, found {len(workers)}")
    wait_for_named_containers([SPARK_MASTER_CONTAINER, KAFKA_CONTAINER, *workers])
    return workers


def run_batch_once(
    base_dir: Path,
    scenario: BatchScenario,
    run_id: str,
    git_commit: str,
    stats_interval_sec: float,
    raw_dir: Path,
) -> None:
    # As execuções batch isolam os efeitos de volume e workers sobre tempo total e throughput.
    workers = scale_workers(scenario.workers)
    container_names = [SPARK_MASTER_CONTAINER, *workers, KAFKA_CONTAINER]
    stop_event, container_samples, thread = start_container_monitor(
        container_names, stats_interval_sec, run_id, scenario.scenario_id, scenario.category, "batch"
    )
    started = datetime.now(timezone.utc)
    dataset_path = base_dir / scenario.dataset_dir
    cmd = [
        "docker",
        "exec",
        SPARK_MASTER_CONTAINER,
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--conf",
        f"spark.executor.instances={scenario.workers}",
        "/opt/jobs/batch_job.py",
        "--input",
        local_path_to_container_data(dataset_path),
        "--scenario",
        scenario.scenario_id,
        "--run-id",
        run_id,
        "--workers",
        str(scenario.workers),
    ]
    result = run_command(cmd, timeout=3600)
    stop_event.set()
    thread.join(timeout=10)
    ended = datetime.now(timezone.utc)
    metrics = parse_metrics(result.stdout)
    resource_summary = summarize_container_samples(container_samples)
    append_csv_rows(raw_dir / "container_metrics.csv", container_samples)
    row = {
        "run_id": run_id,
        "scenario": scenario.scenario_id,
        "scenario_category": scenario.category,
        "mode": "batch",
        "git_commit": git_commit,
        "dataset_path": str(dataset_path),
        "dataset_label": Path(scenario.dataset_dir).name,
        "workers": scenario.workers,
        "status": "ok" if result.returncode == 0 else "error",
        "row_count": metrics.get("row_count", 0),
        "duration_sec": metrics.get("duration_sec", 0.0),
        "throughput_rows_per_sec": metrics.get("throughput_rows_per_sec", 0.0),
        "spark_version": metrics.get("spark_version", ""),
        "avg_cpu_pct": resource_summary["avg_cpu_pct"],
        "max_cpu_pct": resource_summary["max_cpu_pct"],
        "avg_mem_mib": resource_summary["avg_mem_mib"],
        "max_mem_mib": resource_summary["max_mem_mib"],
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "error": (result.stderr or result.stdout).strip() if result.returncode != 0 else "",
    }
    append_csv(raw_dir / "batch_runs.csv", row)


def merge_microbatch_metrics(
    progress_rows: List[Dict[str, object]],
    batch_metric_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    # O Structured Streaming expõe progresso e métricas customizadas em fluxos separados; a análise precisa dos dois.
    batch_by_id = {int(row["batch_id"]): row for row in batch_metric_rows}
    merged: List[Dict[str, object]] = []
    for progress in progress_rows:
        batch_id = int(progress["batch_id"])
        merged.append({**progress, **batch_by_id.get(batch_id, {})})
    return merged


def run_stream_once(
    python_cmd: str,
    scenario: StreamScenario,
    run_id: str,
    stream_duration_sec: int,
    git_commit: str,
    stats_interval_sec: float,
    raw_dir: Path,
    topic_prefix: str,
    record_results: bool = True,
) -> None:
    # Cada repetição stream coordena taxa do producer, cadência de trigger e quantidade de workers do cenário.
    workers = scale_workers(scenario.workers)
    container_names = [SPARK_MASTER_CONTAINER, *workers, KAFKA_CONTAINER]
    stop_event, container_samples, thread = start_container_monitor(
        container_names, stats_interval_sec, run_id, scenario.scenario_id, scenario.category, "stream"
    )
    run_topic = f"{topic_prefix}-{run_id}".replace("_", "-")
    ensure_topic(run_topic)
    started = datetime.now(timezone.utc)

    stream_cmd = [
        "docker",
        "exec",
        SPARK_MASTER_CONTAINER,
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.jars.ivy=/tmp/.ivy",
        "--conf",
        f"spark.executor.instances={scenario.workers}",
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
        str(scenario.trigger_sec),
        "--starting-offsets",
        "earliest",
        "--scenario",
        scenario.scenario_id,
        "--run-id",
        run_id,
        "--workers",
        str(scenario.workers),
    ]
    stream_proc = subprocess.Popen(stream_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(5)
    producer_result = run_command(
        [
            python_cmd,
            "producer/taxi_stream_producer.py",
            "--data-path",
            scenario.data_path,
            "--bootstrap-servers",
            "localhost:29092",
            "--topic",
            run_topic,
            "--rate",
            str(scenario.rate_eps),
            "--duration",
            str(stream_duration_sec),
            "--scenario",
            scenario.scenario_id,
            "--run-id",
            run_id,
        ],
        timeout=stream_duration_sec + 120,
    )
    stream_stdout, stream_stderr = stream_proc.communicate(timeout=stream_duration_sec + 240)
    stop_event.set()
    thread.join(timeout=10)
    ended = datetime.now(timezone.utc)

    stream_metrics = parse_metrics(stream_stdout)
    progress_rows = parse_prefixed_json_lines(stream_stdout, STREAM_PROGRESS_PREFIX)
    batch_metric_rows = parse_prefixed_json_lines(stream_stdout, STREAM_BATCH_METRICS_PREFIX)
    # Latência e backpressure por micro-batch são mantidos separadamente para suportar testes estatísticos e plots.
    merged_microbatches = merge_microbatch_metrics(progress_rows, batch_metric_rows)
    resource_summary = summarize_container_samples(container_samples)
    if record_results:
        append_csv_rows(raw_dir / "container_metrics.csv", container_samples)
        append_csv_rows(
            raw_dir / "stream_microbatches.csv",
            [
                {
                    "git_commit": git_commit,
                    "mode": "stream",
                    "scenario_category": scenario.category,
                    **row,
                }
                for row in merged_microbatches
            ],
        )

    status = "ok" if stream_proc.returncode == 0 and producer_result.returncode == 0 else "error"
    row = {
        "run_id": run_id,
        "scenario": scenario.scenario_id,
        "scenario_category": scenario.category,
        "mode": "stream",
        "git_commit": git_commit,
        "dataset_label": Path(scenario.data_path).name,
        "workers": scenario.workers,
        "configured_rate_eps": scenario.rate_eps,
        "trigger_sec": scenario.trigger_sec,
        "status": status,
        "producer_events_sent": parse_metrics(producer_result.stdout).get("events_sent", 0),
        "producer_achieved_rate_eps": parse_metrics(producer_result.stdout).get("achieved_rate_eps", 0.0),
        "stream_total_input_rows": stream_metrics.get("total_input_rows", 0),
        "stream_micro_batch_count": stream_metrics.get("micro_batch_count", 0),
        "stream_throughput_rows_per_sec": stream_metrics.get("throughput_rows_per_sec", 0.0),
        "stream_avg_input_rows_per_sec": stream_metrics.get("avg_input_rows_per_sec", 0.0),
        "stream_avg_processed_rows_per_sec": stream_metrics.get("avg_processed_rows_per_sec", 0.0),
        "stream_avg_trigger_latency_ms": stream_metrics.get("avg_trigger_latency_ms", 0.0),
        "stream_avg_event_latency_s": average_metric(merged_microbatches, "avg_latency_s"),
        "stream_max_event_latency_s": average_metric(merged_microbatches, "max_latency_s"),
        "stream_avg_backpressure_ms": average_metric(merged_microbatches, "backpressure_ms"),
        "spark_version": stream_metrics.get("spark_version", ""),
        "avg_cpu_pct": resource_summary["avg_cpu_pct"],
        "max_cpu_pct": resource_summary["max_cpu_pct"],
        "avg_mem_mib": resource_summary["avg_mem_mib"],
        "max_mem_mib": resource_summary["max_mem_mib"],
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "error": "\n".join([producer_result.stderr.strip(), stream_stderr.strip()]).strip() if status != "ok" else "",
    }
    if record_results:
        append_csv(raw_dir / "stream_runs.csv", row)


def run_warmup(python_cmd: str, topic_prefix: str) -> None:
    # O warm-up aquece JVM, executores Spark e fluxo Kafka sem contaminar as tabelas finais da análise.
    scenario = StreamScenario("warmup", "warmup", 50, 2, 1)
    run_stream_once(
        python_cmd=python_cmd,
        scenario=scenario,
        run_id="warmup-stream",
        stream_duration_sec=10,
        git_commit=get_git_commit(),
        stats_interval_sec=1.0,
        raw_dir=Path("results/raw"),
        topic_prefix=topic_prefix,
        record_results=False,
    )


def run_post_processing(python_cmd: str) -> None:
    # O pós-processamento faz parte do protocolo: consolidar, plotar e relatar ao final das execuções brutas.
    commands = [
        [python_cmd, "scripts/consolidate_results.py"],
        [python_cmd, "scripts/generate_plots.py"],
        [python_cmd, "scripts/generate_report.py"],
    ]
    for cmd in commands:
        result = run_command(cmd, timeout=180)
        if result.returncode != 0:
            raise RuntimeError(f"Post-processing failed for {' '.join(cmd)}: {result.stderr or result.stdout}")


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parents[1]
    os.chdir(base_dir)
    python_cmd = resolve_python(args.python, base_dir)
    git_commit = get_git_commit()
    raw_dir = base_dir / "results" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # O snapshot do ambiente é necessário para contextualizar as medições na dissertação ou no relatório.
    write_environment_file(base_dir / "results" / "environment.json")
    scale_workers(1)
    ensure_taxi_samples(base_dir)
    ensure_topic(args.topic_prefix)

    if args.warmup:
        run_warmup(python_cmd, args.topic_prefix)

    if not args.skip_batch:
        for scenario in BATCH_SCENARIOS:
            for repetition in range(1, args.batch_repetitions + 1):
                run_id = f"{scenario.scenario_id}_r{repetition}"
                print(f"Running {run_id}")
                run_batch_once(base_dir, scenario, run_id, git_commit, args.stats_interval_sec, raw_dir)

    if not args.skip_stream:
        for scenario in STREAM_SCENARIOS:
            for repetition in range(1, args.stream_repetitions + 1):
                run_id = f"{scenario.scenario_id}_r{repetition}"
                print(f"Running {run_id}")
                run_stream_once(
                    python_cmd,
                    scenario,
                    run_id,
                    args.stream_duration_sec,
                    git_commit,
                    args.stats_interval_sec,
                    raw_dir,
                    args.topic_prefix,
                )

    run_post_processing(python_cmd)
    print(base_dir / "results")


if __name__ == "__main__":
    main()
