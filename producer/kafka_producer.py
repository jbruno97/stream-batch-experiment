import argparse
import json
import random
import time

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka load generator for stream experiment")
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--topic", default="input-topic")
    parser.add_argument("--rate", type=float, default=200.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--scenario", default="stream")
    parser.add_argument("--run-id", default="run-0")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    categories = ["A", "B", "C"]
    start_time = time.time()
    end_time = start_time + args.duration
    sent_count = 0

    # Rate control via target timestamp per emitted event.
    while True:
        now = time.time()
        if now >= end_time:
            break

        message = {
            "category": random.choice(categories),
            "value": random.randint(1, 100),
            "timestamp": int(now * 1000),
        }
        producer.send(args.topic, message)
        sent_count += 1

        if args.rate > 0:
            target_time = start_time + (sent_count / args.rate)
            sleep_for = target_time - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

    producer.flush()
    duration_sec = time.time() - start_time
    achieved_eps = (sent_count / duration_sec) if duration_sec > 0 else 0.0

    metrics = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "topic": args.topic,
        "configured_rate_eps": float(args.rate),
        "duration_sec": float(duration_sec),
        "events_sent": int(sent_count),
        "achieved_rate_eps": float(achieved_eps),
        "timestamp_unix": int(time.time()),
    }

    print(f"messages sent: {sent_count}")
    print("METRICS_JSON:" + json.dumps(metrics, sort_keys=True))


if __name__ == "__main__":
    main()
