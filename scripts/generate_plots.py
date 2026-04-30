import argparse
import csv
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import seaborn as sns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate scientific plots for the experiment")
    parser.add_argument("--raw-dir", default="results/raw")
    parser.add_argument("--summary-dir", default="results/summary")
    parser.add_argument("--plots-dir", default="results/plots")
    return parser.parse_args()


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def to_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def setup_style() -> None:
    sns.set_theme(style="whitegrid", palette="deep")
    plt.rcParams.update(
        {
            "figure.figsize": (10, 6),
            "axes.titlesize": 14,
            "axes.labelsize": 12,
            "legend.fontsize": 10,
        }
    )


def save_plot(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()


def throughput_batch_vs_stream(batch_summary: List[Dict[str, str]], stream_summary: List[Dict[str, str]], output_path: Path) -> None:
    # Este gráfico é a comparação principal entre paradigmas para a discussão final.
    rows = []
    for row in batch_summary:
        rows.append({"scenario": row["scenario"], "mode": "Batch", "throughput": to_float(row.get("throughput_rows_per_sec_mean"))})
    for row in stream_summary:
        rows.append({"scenario": row["scenario"], "mode": "Stream", "throughput": to_float(row.get("stream_throughput_rows_per_sec_mean"))})
    if not rows:
        return
    plt.figure()
    sns.barplot(data=rows, x="scenario", y="throughput", hue="mode")
    plt.title("Throughput Batch vs Stream")
    plt.xlabel("Scenario")
    plt.ylabel("Rows per second")
    save_plot(output_path)


def latency_boxplot(stream_microbatch_rows: List[Dict[str, str]], output_path: Path) -> None:
    # O boxplot expõe dispersão e outliers, essenciais para interpretar cientificamente a latência.
    rows = [
        {"scenario": row["scenario"], "latency_s": to_float(row.get("avg_latency_s"))}
        for row in stream_microbatch_rows
        if row.get("avg_latency_s")
    ]
    if not rows:
        return
    plt.figure()
    sns.boxplot(data=rows, x="scenario", y="latency_s")
    plt.title("Streaming Latency Distribution")
    plt.xlabel("Scenario")
    plt.ylabel("Average latency per micro-batch (s)")
    save_plot(output_path)


def cpu_usage_comparison(container_summary: List[Dict[str, str]], output_path: Path) -> None:
    # A comparação de CPU por container ajuda a separar saturação computacional de gargalos da aplicação.
    rows = [
        {
            "scenario": row["scenario"],
            "container": row["container"],
            "cpu_pct_mean": to_float(row.get("cpu_pct_mean")),
        }
        for row in container_summary
    ]
    if not rows:
        return
    plt.figure()
    sns.barplot(data=rows, x="scenario", y="cpu_pct_mean", hue="container")
    plt.title("Average CPU Usage by Container")
    plt.xlabel("Scenario")
    plt.ylabel("CPU (%)")
    save_plot(output_path)


def scalability_workers(summary_rows: List[Dict[str, str]], metric_key: str, title: str, ylabel: str, output_path: Path) -> None:
    # Os plots de escalabilidade por worker apoiam a análise da escala horizontal em cada paradigma.
    rows = [
        {"scenario": row["scenario"], "workers": int(float(row.get("workers", 0) or 0)), "metric": to_float(row.get(metric_key))}
        for row in summary_rows
        if row.get("workers")
    ]
    if not rows:
        return
    plt.figure()
    sns.lineplot(data=rows, x="workers", y="metric", hue="scenario", marker="o")
    plt.title(title)
    plt.xlabel("Spark workers")
    plt.ylabel(ylabel)
    save_plot(output_path)


def combined_scalability_plot(batch_summary: List[Dict[str, str]], stream_summary: List[Dict[str, str]], output_path: Path) -> None:
    # A visão combinada facilita comparar como batch e stream reagem ao aumento de workers.
    rows = []
    for row in batch_summary:
        if row.get("workers"):
            rows.append(
                {
                    "scenario": row["scenario"],
                    "mode": "Batch",
                    "workers": int(float(row["workers"])),
                    "throughput": to_float(row.get("throughput_rows_per_sec_mean")),
                }
            )
    for row in stream_summary:
        if row.get("workers"):
            rows.append(
                {
                    "scenario": row["scenario"],
                    "mode": "Stream",
                    "workers": int(float(row["workers"])),
                    "throughput": to_float(row.get("stream_throughput_rows_per_sec_mean")),
                }
            )
    if not rows:
        return
    plt.figure()
    sns.lineplot(data=rows, x="workers", y="throughput", hue="mode", style="scenario", marker="o")
    plt.title("Scalability by Spark Workers")
    plt.xlabel("Spark workers")
    plt.ylabel("Throughput (rows/s)")
    save_plot(output_path)


def main() -> None:
    args = parse_args()
    setup_style()
    raw_dir = Path(args.raw_dir)
    summary_dir = Path(args.summary_dir)
    plots_dir = Path(args.plots_dir)

    batch_summary = read_csv_rows(summary_dir / "batch_summary.csv")
    stream_summary = read_csv_rows(summary_dir / "stream_summary.csv")
    container_summary = read_csv_rows(summary_dir / "container_summary.csv")
    stream_microbatches = read_csv_rows(raw_dir / "stream_microbatches.csv")

    throughput_batch_vs_stream(batch_summary, stream_summary, plots_dir / "throughput_batch_vs_stream.png")
    latency_boxplot(stream_microbatches, plots_dir / "latency_boxplot.png")
    cpu_usage_comparison(container_summary, plots_dir / "cpu_usage_comparison.png")
    scalability_workers(
        batch_summary,
        "throughput_rows_per_sec_mean",
        "Batch Scalability by Workers",
        "Throughput (rows/s)",
        plots_dir / "scalability_workers_batch.png",
    )
    scalability_workers(
        stream_summary,
        "stream_throughput_rows_per_sec_mean",
        "Stream Scalability by Workers",
        "Throughput (rows/s)",
        plots_dir / "scalability_workers_stream.png",
    )
    combined_scalability_plot(batch_summary, stream_summary, plots_dir / "scalability_workers.png")
    print(plots_dir)


if __name__ == "__main__":
    main()
