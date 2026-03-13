import argparse
import itertools
import json
import time

import pandas as pd
from kafka import KafkaProducer


METRICS_PREFIX = "METRICS_JSON:"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Taxi Kafka producer backed by parquet samples")
    parser.add_argument("--data-path", default="data/samples/200mb")
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--topic", default="taxi-topic")
    parser.add_argument("--bootstrap", "--bootstrap-servers", dest="bootstrap", default="localhost:29092")
    parser.add_argument("--scenario", default="S1")
    parser.add_argument("--run-id", default="run-0")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = pd.read_parquet(args.data_path)
    if df.empty:
        raise ValueError(f"Dataset vazio: {args.data_path}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    interval = (1.0 / args.rate) if args.rate > 0 else 0.0
    records = df.to_dict(orient="records")
    start_time = time.time()
    deadline = start_time + args.duration
    sent = 0

    for row in itertools.cycle(records):
        if time.time() >= deadline:
            break

        message = dict(row)
        message["event_time_ms"] = int(time.time() * 1000)
        producer.send(args.topic, message)
        sent += 1

        if interval > 0:
            time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start_time
    achieved_rate = (sent / elapsed) if elapsed > 0 else 0.0
    metrics = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "topic": args.topic,
        "data_path": args.data_path,
        "duration_sec": float(elapsed),
        "events_sent": int(sent),
        "achieved_rate_eps": float(achieved_rate),
    }

    print(f"Sent {sent} messages.")
    print(METRICS_PREFIX + json.dumps(metrics, sort_keys=True))


if __name__ == "__main__":
    main()
