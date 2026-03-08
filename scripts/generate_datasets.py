import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate medium dataset from small dataset")
    parser.add_argument("--source", default="data/small/input.csv")
    parser.add_argument("--target", default="data/medium/input.csv")
    parser.add_argument("--target-rows", type=int, default=100000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_path = Path(args.source)
    target_path = Path(args.target)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    with source_path.open("r", newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        rows = list(reader)
        fieldnames = reader.fieldnames or ["category", "value", "timestamp"]

    if not rows:
        raise ValueError(f"Source dataset is empty: {source_path}")

    with target_path.open("w", newline="", encoding="utf-8") as target_file:
        writer = csv.DictWriter(target_file, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(args.target_rows):
            row = dict(rows[i % len(rows)])
            writer.writerow(row)

    print(f"Generated dataset: {target_path} with {args.target_rows} rows")


if __name__ == "__main__":
    main()
