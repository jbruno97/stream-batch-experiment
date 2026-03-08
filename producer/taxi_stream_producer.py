import json
import os
import time
import pandas as pd
from kafka import KafkaProducer


def main() -> None:
    # Parametros do cenario de streaming.
    data_path = os.getenv("DATA_PATH", "data/samples/200mb")
    rate = int(os.getenv("RATE", "200"))
    topic = os.getenv("KAFKA_TOPIC", "taxi-topic")
    broker = os.getenv("KAFKA_BROKER", "localhost:29092")

    # Le os dados de origem e prepara o produtor Kafka.
    df = pd.read_parquet(data_path)

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    # Controla taxa de envio (eventos por segundo).
    interval = (1.0 / rate) if rate > 0 else 0.0
    sent = 0

    for _, row in df.iterrows():
        message = row.to_dict()
        # Timestamp de envio para medir latencia no consumidor.
        message["event_time_ms"] = int(time.time() * 1000)
        producer.send(topic, message)
        sent += 1
        if interval > 0:
            time.sleep(interval)

    # Garante envio de todas as mensagens pendentes.
    producer.flush()
    print(f"Sent {sent} messages.")


if __name__ == "__main__":
    main()
