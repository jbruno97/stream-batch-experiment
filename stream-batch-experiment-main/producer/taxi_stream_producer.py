"""
Kafka producer para o dataset NYC Taxi.
Lê o parquet em chunks para não explodir a RAM.
"""
import argparse
import itertools
import json
import time
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer

METRICS_PREFIX = "METRICS_JSON:"
_CHUNK = 5_000  # linhas por batch de leitura


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--data-path", default="data/samples/200mb")
    p.add_argument("--rate", type=float, default=200.0)
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--topic", default="taxi-topic")
    p.add_argument("--bootstrap", "--bootstrap-servers", dest="bootstrap", default="localhost:29092")
    p.add_argument("--scenario", default="S1")
    p.add_argument("--run-id", default="run-0")
    return p.parse_args()


def _iter_records(data_path: str):
    """Gera dicts linha a linha sem carregar tudo em memória."""
    path = Path(data_path)
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    for f in itertools.cycle(files):
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(batch_size=_CHUNK):
            yield from batch.to_pydict_list() if hasattr(batch, "to_pydict_list") else (
                dict(zip(batch.schema.names, row))
                for row in zip(*[col.to_pylist() for col in batch.columns])
            )


def main() -> None:
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        linger_ms=5,           # micro-batching interno para reduzir syscalls
        batch_size=32_768,
        compression_type="lz4",
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    deadline = time.time() + args.duration
    sent = 0
    start = time.time()

    for row in _iter_records(args.data_path):
        if time.time() >= deadline:
            break
        row["event_time_ms"] = int(time.time() * 1000)
        producer.send(args.topic, row)
        sent += 1
        if interval > 0:
            time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start
    metrics = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "topic": args.topic,
        "data_path": args.data_path,
        "duration_sec": float(elapsed),
        "events_sent": int(sent),
        "achieved_rate_eps": sent / elapsed if elapsed > 0 else 0.0,
    }
    print(f"Sent {sent} messages.")
    print(METRICS_PREFIX + json.dumps(metrics, sort_keys=True))


if __name__ == "__main__":
    main()
