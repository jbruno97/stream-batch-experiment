import json
import platform
import subprocess
from pathlib import Path
from typing import Dict

import psutil


def _run_text(cmd: list[str]) -> str:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15, check=False)
    except OSError:
        return "unknown"
    output = (result.stdout or result.stderr).strip()
    return output or "unknown"


def collect_environment_metadata() -> Dict[str, object]:
    return {
        "cpu": platform.processor() or platform.uname().processor or "unknown",
        "machine": platform.machine(),
        "cores_logical": psutil.cpu_count(logical=True),
        "cores_physical": psutil.cpu_count(logical=False),
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "storage_total_gb": round(psutil.disk_usage("/").total / (1024**3), 2),
        "os": platform.platform(),
        "python_version": platform.python_version(),
        "docker_version": _run_text(["docker", "--version"]),
        "spark_image": "apache/spark:3.5.1",
        "spark_connector": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "kafka_image": "confluentinc/cp-kafka:7.5.0",
        "zookeeper_image": "confluentinc/cp-zookeeper:7.5.0",
    }


def write_environment_file(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    environment = collect_environment_metadata()
    output_path.write_text(json.dumps(environment, indent=2), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    path = write_environment_file(Path("results/environment.json"))
    print(path)
