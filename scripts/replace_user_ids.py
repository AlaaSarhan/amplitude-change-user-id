#!/usr/bin/env python3
"""Replace user_id values in converted JSON files using a CSV mapping.

Run with: poetry run python scripts/replace_user_ids.py --csv mapping.csv
Outputs replaced files to ./replaced (use --output to override).
"""

import argparse
import csv
import json
from pathlib import Path


def load_mapping(csv_path: Path) -> dict[str, str]:
    mapping = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[row["old_user_id"]] = row["new_user_id"]
    return mapping


def replace_in_file(json_path: Path, output_path: Path, mapping: dict[str, str]) -> tuple[int, int]:
    lines_out = []
    replaced = 0
    skipped = 0

    with open(json_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                old_id = event.get("user_id")
                if old_id and old_id in mapping:
                    event["user_id"] = mapping[old_id]
                    replaced += 1
                lines_out.append(json.dumps(event))
            except json.JSONDecodeError:
                skipped += 1

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines_out) + "\n" if lines_out else "")

    return replaced, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Replace user_id values in converted JSON files"
    )
    parser.add_argument("--csv", required=True, help="CSV file with old_user_id and new_user_id columns")
    parser.add_argument(
        "--converted",
        default="./converted",
        help="Directory containing converted JSON files (default: ./converted)",
    )
    parser.add_argument(
        "--output",
        default="./replaced",
        help="Directory to write output files (default: ./replaced)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        return

    converted_dir = Path(args.converted)
    if not converted_dir.exists():
        print(f"Error: Converted directory not found: {converted_dir}")
        return

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    mapping = load_mapping(csv_path)
    print(f"Loaded {len(mapping)} user ID mapping(s) from {csv_path}\n")

    json_files = sorted(converted_dir.glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {converted_dir}")
        return

    total_replaced = 0
    for json_file in json_files:
        replaced, skipped = replace_in_file(json_file, output_dir / json_file.name, mapping)
        total_replaced += replaced
        status = f"replaced {replaced}" if replaced else "no matches"
        if skipped:
            status += f", skipped {skipped} invalid lines"
        print(f"{json_file.name}: {status}")

    print(f"\nDone. Total replacements: {total_replaced}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
