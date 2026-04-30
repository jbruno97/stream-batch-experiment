import argparse
import csv
from pathlib import Path
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a markdown report for the experiment")
    parser.add_argument("--summary-dir", default="results/summary")
    parser.add_argument("--output", default="results/report.md")
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


def best_scenario(rows: List[Dict[str, str]], metric_key: str) -> Dict[str, str]:
    if not rows:
        return {}
    return max(rows, key=lambda row: to_float(row.get(metric_key)))


def render_section(rows: List[Dict[str, str]], title: str, throughput_key: str, latency_key: str = "") -> List[str]:
    # O relatório é intencionalmente conciso; os CSVs e plots carregam o detalhe estatístico completo.
    lines = [f"## {title}", ""]
    if not rows:
        lines.append("No results available.")
        lines.append("")
        return lines
    for row in rows:
        lines.append(
            f"- {row['scenario']}: throughput medio {to_float(row.get(throughput_key)):.2f} rows/s"
            + (f", latencia media {to_float(row.get(latency_key)):.4f}" if latency_key else "")
        )
    lines.append("")
    return lines


def main() -> None:
    args = parse_args()
    summary_dir = Path(args.summary_dir)
    output_path = Path(args.output)
    batch_rows = read_csv_rows(summary_dir / "batch_summary.csv")
    stream_rows = read_csv_rows(summary_dir / "stream_summary.csv")
    microbatch_rows = read_csv_rows(summary_dir / "stream_microbatch_summary.csv")

    best_batch = best_scenario(batch_rows, "throughput_rows_per_sec_mean")
    best_stream = best_scenario(stream_rows, "stream_throughput_rows_per_sec_mean")
    best_latency = min(microbatch_rows, key=lambda row: to_float(row.get("avg_latency_s_mean", 0.0))) if microbatch_rows else {}

    # Este relatório markdown é o artefato final legível por humanos gerado a partir dos sumários consolidados.
    lines = ["# Experimental Results", ""]
    lines.extend(render_section(batch_rows, "Batch Processing", "throughput_rows_per_sec_mean"))
    lines.extend(render_section(stream_rows, "Streaming Processing", "stream_throughput_rows_per_sec_mean", "stream_avg_trigger_latency_ms_mean"))
    lines.append("## Highlights")
    lines.append("")
    if best_batch:
        lines.append(
            f"- Melhor throughput batch: {best_batch['scenario']} com {to_float(best_batch.get('throughput_rows_per_sec_mean')):.2f} rows/s."
        )
    if best_stream:
        lines.append(
            f"- Melhor throughput stream: {best_stream['scenario']} com {to_float(best_stream.get('stream_throughput_rows_per_sec_mean')):.2f} rows/s."
        )
    if best_latency:
        lines.append(
            f"- Menor latencia media de micro-batch: {best_latency['scenario']} com {to_float(best_latency.get('avg_latency_s_mean')):.4f} s."
        )
    lines.append("")
    lines.append("## Research Questions")
    lines.append("")
    lines.append("- RQ1: Qual paradigma apresenta maior throughput em sistemas intensivos de dados?")
    lines.append("- RQ2: Como o volume de dados afeta o desempenho batch vs stream?")
    lines.append("- RQ3: Qual o impacto da taxa de eventos na latencia do streaming?")
    lines.append("- RQ4: Como a escalabilidade horizontal afeta os dois paradigmas?")
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
