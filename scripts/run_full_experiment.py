"""
Runner científico batch vs stream.  Versão compacta.
"""
import argparse
import csv
import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from capture_environment import write_environment_file
from container_monitor import collect_container_metrics

# ── constantes ────────────────────────────────────────────────────────────────
METRICS_PFX        = "METRICS_JSON:"
PROGRESS_PFX       = "STREAM_PROGRESS_JSON:"
BATCH_METRICS_PFX  = "STREAM_BATCH_METRICS_JSON:"
KAFKA_PKG          = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SPARK_MASTER       = "spark-master"
KAFKA_CTR          = "kafka"

# ── cenários ──────────────────────────────────────────────────────────────────
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

BATCH_SCENARIOS: List[BatchScenario] = [
    BatchScenario("BL1","carga","data/samples/200mb",1),
    BatchScenario("BL2","carga","data/samples/1gb",1),
    BatchScenario("BL3","carga","data/samples/3gb",1),
    BatchScenario("BL4","carga","data/samples/10gb",1),
    BatchScenario("BS1","escalabilidade","data/samples/200mb",1),
    BatchScenario("BS2","escalabilidade","data/samples/200mb",2),
    BatchScenario("BS3","escalabilidade","data/samples/200mb",4),
    BatchScenario("BS4","escalabilidade","data/samples/1gb",1),
    BatchScenario("BS5","escalabilidade","data/samples/1gb",2),
    BatchScenario("BS6","escalabilidade","data/samples/1gb",4),
    BatchScenario("BS7","escalabilidade","data/samples/3gb",1),
    BatchScenario("BS8","escalabilidade","data/samples/3gb",2),
    BatchScenario("BS9","escalabilidade","data/samples/3gb",4),
    BatchScenario("BS10","escalabilidade","data/samples/10gb",1),
    BatchScenario("BS11","escalabilidade","data/samples/10gb",2),
    BatchScenario("BS12","escalabilidade","data/samples/10gb",4),
]

STREAM_SCENARIOS: List[StreamScenario] = [
    StreamScenario("SL1","carga",200,1,1),
    StreamScenario("SL2","carga",500,1,1),
    StreamScenario("SL3","carga",1000,1,1),
    StreamScenario("SL4","carga",2000,1,1),
    StreamScenario("SS1","escalabilidade",200,1,1),
    StreamScenario("SS2","escalabilidade",200,1,2),
    StreamScenario("SS3","escalabilidade",200,1,4),
    StreamScenario("SS4","escalabilidade",500,1,1),
    StreamScenario("SS5","escalabilidade",500,1,2),
    StreamScenario("SS6","escalabilidade",500,1,4),
    StreamScenario("SS7","escalabilidade",1000,1,1),
    StreamScenario("SS8","escalabilidade",1000,1,2),
    StreamScenario("SS9","escalabilidade",1000,1,4),
    StreamScenario("SS10","escalabilidade",2000,1,1),
    StreamScenario("SS11","escalabilidade",2000,1,2),
    StreamScenario("SS12","escalabilidade",2000,1,4),
    StreamScenario("ST1","trigger",1000,1,1),
    StreamScenario("ST2","trigger",1000,2,1),
    StreamScenario("ST3","trigger",1000,5,1),
]

# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Runner científico batch vs stream")
    p.add_argument("--batch-repetitions", type=int, default=30)
    p.add_argument("--stream-repetitions", type=int, default=30)
    p.add_argument("--stream-duration-sec", type=int, default=30)
    p.add_argument("--stats-interval-sec", type=float, default=1.0)
    p.add_argument("--warmup", action="store_true")
    p.add_argument("--python", default="")
    p.add_argument("--topic-prefix", default="taxi-topic")
    p.add_argument("--skip-batch", action="store_true")
    p.add_argument("--skip-stream", action="store_true")
    return p.parse_args()

# ── utilitários ───────────────────────────────────────────────────────────────
def _run(cmd: List[str], timeout: Optional[int] = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

def _parse_json_lines(text: str, prefix: str) -> List[Dict]:
    out = []
    for line in text.splitlines():
        if line.startswith(prefix):
            try:
                out.append(json.loads(line[len(prefix):]))
            except json.JSONDecodeError:
                pass
    return out

def _parse_metrics(text: str) -> Dict:
    rows = _parse_json_lines(text, METRICS_PFX)
    return rows[-1] if rows else {}

def _avg(lst: List[float]) -> float:
    return sum(lst) / len(lst) if lst else 0.0

def _resolve_python(python_arg: str, base: Path) -> str:
    if python_arg:
        return python_arg
    venv = os.environ.get("VIRTUAL_ENV", "")
    candidates = [
        *(([Path(venv)/"bin"/"python", Path(venv)/"Scripts"/"python.exe"]) if venv else []),
        base/".venv"/"bin"/"python", base/".venv"/"Scripts"/"python.exe",
        base/"venv"/"bin"/"python",  base/"venv"/"Scripts"/"python.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return str(c)
    return "python"

def _git_commit() -> str:
    r = _run(["git","rev-parse","HEAD"], timeout=15)
    return r.stdout.strip() or "unknown"

# ── CSV helpers ───────────────────────────────────────────────────────────────
def _write_csv(path: Path, rows: List[Dict]) -> None:
    """Escreve/acrescenta uma lista de dicts em uma única abertura de arquivo."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not exists:
            w.writeheader()
        w.writerows(rows)

# ── Docker / infra ────────────────────────────────────────────────────────────
_current_workers: int = -1  # cache para evitar scale redundante

def _scale(n: int) -> List[str]:
    global _current_workers
    if _current_workers != n:
        r = _run(["docker","compose","up","-d","--scale",f"spark-worker={n}"], timeout=240)
        if r.returncode != 0:
            raise RuntimeError(f"compose up falhou: {r.stderr or r.stdout}")
        time.sleep(5)
        _current_workers = n
    r = _run(["docker","ps","--filter","label=com.docker.compose.service=spark-worker","--format","{{.Names}}"], timeout=30)
    workers = [l.strip() for l in r.stdout.splitlines() if l.strip()]
    if len(workers) < n:
        raise RuntimeError(f"Esperava {n} workers, encontrou {len(workers)}")
    return workers

def _kafka_ready() -> None:
    cmd = ["docker","exec",KAFKA_CTR,"kafka-broker-api-versions","--bootstrap-server","kafka:9092"]
    for _ in range(30):
        if _run(cmd, timeout=30).returncode == 0:
            return
        time.sleep(2)
    raise RuntimeError("Kafka broker não respondeu após 60 s")

def _ensure_topic(topic: str) -> None:
    _kafka_ready()
    r = _run([
        "docker","exec",KAFKA_CTR,"kafka-topics","--create","--if-not-exists",
        "--topic",topic,"--bootstrap-server","kafka:9092",
        "--partitions","1","--replication-factor","1",
    ], timeout=60)
    if r.returncode != 0:
        raise RuntimeError(f"Falha ao criar tópico {topic}: {r.stderr}")

def _ensure_samples(base: Path) -> None:
    required = ["200mb","1gb","3gb","10gb"]
    if all((base/"data"/"samples"/d).exists() and
           any((base/"data"/"samples"/d).rglob("*.parquet")) for d in required):
        return
    raw = base/"data"/"raw"/"nyc_taxi"
    if not raw.exists():
        raise RuntimeError(f"Dataset bruto não encontrado: {raw}")
    r = _run([
        "docker","exec",SPARK_MASTER,
        "/opt/spark/bin/spark-submit",
        "/opt/scripts/create_samples.py",
        "--input","/opt/data/raw/nyc_taxi",
        "--output-root","/opt/data/samples",
    ], timeout=2400)
    if r.returncode != 0:
        raise RuntimeError(f"Falha ao criar amostras: {r.stderr or r.stdout}")

def _container_path(p: Path) -> str:
    s = p.as_posix()
    idx = s.find("/data/")
    if idx == -1:
        raise ValueError(f"Caminho fora de data/: {p}")
    return "/opt" + s[idx:]

# ── monitor de containers ─────────────────────────────────────────────────────
def _start_monitor(containers, interval, run_id, scenario, category, mode):
    stop = threading.Event()
    samples: List[Dict] = []
    def _loop():
        samples.extend(collect_container_metrics(containers, interval, stop, run_id, scenario, category, mode))
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return stop, samples, t

def _resource_summary(samples: List[Dict]) -> Dict:
    if not samples:
        return {"avg_cpu_pct":0.,"max_cpu_pct":0.,"avg_mem_mib":0.,"max_mem_mib":0.}
    cpu = [float(s.get("cpu_pct",0)) for s in samples]
    mem = [float(s.get("mem_mib",0)) for s in samples]
    return {"avg_cpu_pct":_avg(cpu),"max_cpu_pct":max(cpu),"avg_mem_mib":_avg(mem),"max_mem_mib":max(mem)}

# ── execução batch ────────────────────────────────────────────────────────────
def run_batch_once(base: Path, sc: BatchScenario, run_id: str, commit: str, interval: float, raw: Path) -> None:
    workers = _scale(sc.workers)
    ctrs = [SPARK_MASTER, *workers, KAFKA_CTR]
    stop, samples, t = _start_monitor(ctrs, interval, run_id, sc.scenario_id, sc.category, "batch")

    started = datetime.now(timezone.utc)
    r = _run([
        "docker","exec",SPARK_MASTER,
        "/opt/spark/bin/spark-submit",
        "--master","spark://spark-master:7077",
        "--conf",f"spark.executor.instances={sc.workers}",
        "/opt/jobs/batch_job.py",
        "--input",_container_path(base/sc.dataset_dir),
        "--scenario",sc.scenario_id,"--run-id",run_id,"--workers",str(sc.workers),
    ], timeout=3600)
    stop.set(); t.join(timeout=10)

    m = _parse_metrics(r.stdout)
    res = _resource_summary(samples)
    _write_csv(raw/"container_metrics.csv", samples)
    _write_csv(raw/"batch_runs.csv", [{
        "run_id":run_id,"scenario":sc.scenario_id,"scenario_category":sc.category,
        "mode":"batch","git_commit":commit,
        "dataset_label":Path(sc.dataset_dir).name,"workers":sc.workers,
        "status":"ok" if r.returncode==0 else "error",
        "row_count":m.get("row_count",0),
        "duration_sec":m.get("duration_sec",0.),
        "throughput_rows_per_sec":m.get("throughput_rows_per_sec",0.),
        "spark_version":m.get("spark_version",""),
        **res,
        "started_at_utc":started.isoformat(),
        "ended_at_utc":datetime.now(timezone.utc).isoformat(),
        "error":(r.stderr or r.stdout).strip() if r.returncode!=0 else "",
    }])

# ── execução stream ───────────────────────────────────────────────────────────
def run_stream_once(
    py: str, sc: StreamScenario, run_id: str, duration: int,
    commit: str, interval: float, raw: Path, topic_pfx: str, record: bool = True,
) -> None:
    workers = _scale(sc.workers)
    ctrs = [SPARK_MASTER, *workers, KAFKA_CTR]
    stop, samples, t = _start_monitor(ctrs, interval, run_id, sc.scenario_id, sc.category, "stream")
    topic = f"{topic_pfx}-{run_id}".replace("_","-")
    _ensure_topic(topic)

    started = datetime.now(timezone.utc)
    stream_proc = subprocess.Popen([
        "docker","exec",SPARK_MASTER,
        "/opt/spark/bin/spark-submit",
        "--master","spark://spark-master:7077",
        "--conf","spark.jars.ivy=/tmp/.ivy",
        "--conf",f"spark.executor.instances={sc.workers}",
        # Tenta usar JARs pré-baixados; se ausentes, baixa via --packages
        "--jars","/opt/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
                 "/opt/jars/kafka-clients-3.4.1.jar,"
                 "/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
                 "/opt/jars/commons-pool2-2.11.1.jar",
        "--packages",KAFKA_PKG,
        "/opt/jobs/stream_job.py",
        "--bootstrap-servers","kafka:9092",
        "--topic",topic,
        "--duration-sec",str(duration),
        "--trigger-sec",str(sc.trigger_sec),
        "--starting-offsets","earliest",
        "--rate-eps",str(sc.rate_eps),
        "--scenario",sc.scenario_id,"--run-id",run_id,"--workers",str(sc.workers),
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    time.sleep(5)
    prod = _run([
        py,"producer/taxi_stream_producer.py",
        "--data-path",sc.data_path,
        "--bootstrap-servers","localhost:29092",
        "--topic",topic,
        "--rate",str(sc.rate_eps),
        "--duration",str(duration),
        "--scenario",sc.scenario_id,"--run-id",run_id,
    ], timeout=duration+120)

    s_out, s_err = stream_proc.communicate(timeout=duration+240)
    stop.set(); t.join(timeout=10)

    sm = _parse_metrics(s_out)
    prog_rows = _parse_json_lines(s_out, PROGRESS_PFX)
    batch_rows = _parse_json_lines(s_out, BATCH_METRICS_PFX)
    by_id = {int(b["batch_id"]): b for b in batch_rows}
    merged = [{**p, **by_id.get(int(p["batch_id"]),{})} for p in prog_rows]

    res = _resource_summary(samples)
    pm = _parse_metrics(prod.stdout)
    status = "ok" if stream_proc.returncode==0 and prod.returncode==0 else "error"

    if record:
        _write_csv(raw/"container_metrics.csv", samples)
        _write_csv(raw/"stream_microbatches.csv", [
            {"git_commit":commit,"mode":"stream","scenario_category":sc.category,**row}
            for row in merged
        ])
        _write_csv(raw/"stream_runs.csv", [{
            "run_id":run_id,"scenario":sc.scenario_id,"scenario_category":sc.category,
            "mode":"stream","git_commit":commit,
            "dataset_label":Path(sc.data_path).name,"workers":sc.workers,
            "configured_rate_eps":sc.rate_eps,"trigger_sec":sc.trigger_sec,
            "status":status,
            "producer_events_sent":pm.get("events_sent",0),
            "producer_achieved_rate_eps":pm.get("achieved_rate_eps",0.),
            "stream_total_input_rows":sm.get("total_input_rows",0),
            "stream_micro_batch_count":sm.get("micro_batch_count",0),
            "stream_throughput_rows_per_sec":sm.get("throughput_rows_per_sec",0.),
            "stream_avg_input_rows_per_sec":sm.get("avg_input_rows_per_sec",0.),
            "stream_avg_processed_rows_per_sec":sm.get("avg_processed_rows_per_sec",0.),
            "stream_avg_trigger_latency_ms":sm.get("avg_trigger_latency_ms",0.),
            "stream_avg_event_latency_s":_avg([float(r.get("avg_latency_s",0) or 0) for r in merged]),
            "stream_max_event_latency_s":_avg([float(r.get("max_latency_s",0) or 0) for r in merged]),
            "stream_avg_backpressure_ms":_avg([float(r.get("backpressure_ms",0) or 0) for r in merged]),
            "spark_version":sm.get("spark_version",""),
            **res,
            "started_at_utc":started.isoformat(),
            "ended_at_utc":datetime.now(timezone.utc).isoformat(),
            "error":"\n".join(filter(None,[prod.stderr.strip(),s_err.strip()])) if status!="ok" else "",
        }])

# ── pós-processamento ─────────────────────────────────────────────────────────
def _post(py: str) -> None:
    for script in ["scripts/consolidate_results.py","scripts/generate_plots.py","scripts/generate_report.py"]:
        r = _run([py, script], timeout=180)
        if r.returncode != 0:
            raise RuntimeError(f"{script} falhou: {r.stderr or r.stdout}")

# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()
    base = Path(__file__).resolve().parents[1]
    os.chdir(base)
    py = _resolve_python(args.python, base)
    commit = _git_commit()
    raw = base/"results"/"raw"
    raw.mkdir(parents=True, exist_ok=True)

    write_environment_file(base/"results"/"environment.json")
    _scale(1)
    _ensure_samples(base)
    _ensure_topic(args.topic_prefix)

    if args.warmup:
        run_stream_once(py, StreamScenario("warmup","warmup",50,2,1),
                        "warmup-stream", 10, commit, 1.0, raw, args.topic_prefix, record=False)

    if not args.skip_batch:
        for sc in BATCH_SCENARIOS:
            for rep in range(1, args.batch_repetitions+1):
                rid = f"{sc.scenario_id}_r{rep}"
                ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{ts}][batch] {rid} | dataset={Path(sc.dataset_dir).name} workers={sc.workers}")
                run_batch_once(base, sc, rid, commit, args.stats_interval_sec, raw)

    if not args.skip_stream:
        for sc in STREAM_SCENARIOS:
            for rep in range(1, args.stream_repetitions+1):
                rid = f"{sc.scenario_id}_r{rep}"
                ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[{ts}][stream] {rid} | rate={sc.rate_eps}eps trigger={sc.trigger_sec}s workers={sc.workers}")
                run_stream_once(py, sc, rid, args.stream_duration_sec,
                                commit, args.stats_interval_sec, raw, args.topic_prefix)

    _post(py)
    print(base/"results")


if __name__ == "__main__":
    main()
