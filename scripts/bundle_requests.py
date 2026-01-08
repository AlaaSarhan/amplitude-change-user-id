#!/usr/bin/env python3
"""Bundle converted events into curl request commands stored in shell files.

Run with: poetry run python scripts/bundle_requests.py --input ./converted --output ./requests --api-key YOUR_API_KEY
"""

import argparse
import json
import os
from pathlib import Path


# API limits
MAX_EVENTS_PER_BATCH = 2000
MAX_PAYLOAD_BYTES = 20 * 1024 * 1024  # 20MB
# Leave some headroom for the JSON wrapper and API key
SAFE_PAYLOAD_BYTES = 19 * 1024 * 1024  # 19MB to be safe


def read_all_events(input_dir: Path) -> list[dict]:
    """Read all events from converted JSON files."""
    all_events = []

    json_files = list(input_dir.glob("*.json"))
    if not json_files:
        return all_events

    for json_file in sorted(json_files):
        with open(json_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    all_events.append(event)
                except json.JSONDecodeError:
                    continue

    return all_events


def create_payload(api_key: str, events: list[dict]) -> dict:
    """Create the full API payload."""
    return {
        "api_key": api_key,
        "events": events
    }


def estimate_payload_size(api_key: str, events: list[dict]) -> int:
    """Estimate the JSON payload size in bytes."""
    payload = create_payload(api_key, events)
    return len(json.dumps(payload).encode('utf-8'))


def batch_events(events: list[dict], api_key: str) -> list[list[dict]]:
    """Split events into batches respecting size and count limits."""
    batches = []
    current_batch = []

    for event in events:
        # Try adding to current batch
        test_batch = current_batch + [event]

        # Check count limit
        if len(test_batch) > MAX_EVENTS_PER_BATCH:
            # Current batch is full, start new one
            if current_batch:
                batches.append(current_batch)
            current_batch = [event]
            continue

        # Check size limit
        if estimate_payload_size(api_key, test_batch) > SAFE_PAYLOAD_BYTES:
            # Would exceed size limit, start new batch
            if current_batch:
                batches.append(current_batch)
            current_batch = [event]
            continue

        # Event fits in current batch
        current_batch = test_batch

    # Don't forget the last batch
    if current_batch:
        batches.append(current_batch)

    return batches


def generate_curl_script(
    batch_num: int,
    api_key: str,
    events: list[dict],
    output_dir: Path,
    eu: bool = False
) -> Path:
    """Generate a shell script with curl command for a batch."""
    endpoint = "https://api.eu.amplitude.com/batch" if eu else "https://api2.amplitude.com/batch"

    payload = create_payload(api_key, events)
    payload_json = json.dumps(payload, separators=(',', ':'))  # Compact JSON

    # Create the shell script
    script_path = output_dir / f"batch_{batch_num:04d}.sh"

    # Escape single quotes in the JSON for shell
    escaped_json = payload_json.replace("'", "'\\''")

    script_content = f"""#!/bin/bash
# Batch {batch_num}: {len(events)} events
# Payload size: {len(payload_json)} bytes

curl -X POST '{endpoint}' \\
  -H 'Content-Type: application/json' \\
  -d '{escaped_json}'

echo ""
echo "Batch {batch_num} complete"
"""

    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(script_content)

    # Make executable
    os.chmod(script_path, 0o755)

    return script_path


def generate_run_all_script(output_dir: Path, num_batches: int, delay_seconds: int = 1):
    """Generate a master script that runs all batches with delays."""
    script_path = output_dir / "run_all.sh"

    script_content = f"""#!/bin/bash
# Run all {num_batches} batch scripts with {delay_seconds} second delay between each
# This helps respect rate limits

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Starting upload of {num_batches} batches..."
echo ""

for i in $(seq -f "%04g" 1 {num_batches}); do
    script="$SCRIPT_DIR/batch_$i.sh"
    if [ -f "$script" ]; then
        echo "Running batch $i of {num_batches}..."
        bash "$script"
        echo ""

        # Delay between batches (except after the last one)
        if [ "$i" -lt "{num_batches:04d}" ]; then
            echo "Waiting {delay_seconds} second(s) before next batch..."
            sleep {delay_seconds}
        fi
    fi
done

echo ""
echo "All batches complete!"
"""

    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(script_content)

    os.chmod(script_path, 0o755)
    return script_path


def main():
    parser = argparse.ArgumentParser(
        description="Bundle converted events into curl request shell scripts"
    )
    parser.add_argument(
        "--input",
        default="./converted",
        help="Input directory containing converted JSON files (default: ./converted)"
    )
    parser.add_argument(
        "--output",
        default="./requests",
        help="Output directory for shell scripts (default: ./requests)"
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="Amplitude API key for the target project"
    )
    parser.add_argument(
        "--eu",
        action="store_true",
        help="Use EU data residency endpoint"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=1,
        help="Seconds to delay between batches in run_all.sh (default: 1)"
    )

    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}")
        return

    # Read all events
    print(f"Reading events from {input_dir}...")
    all_events = read_all_events(input_dir)
    print(f"Found {len(all_events)} total events")

    if not all_events:
        print("No events to process")
        return

    # Batch events
    print(f"\nBatching events (max {MAX_EVENTS_PER_BATCH} events, max ~{SAFE_PAYLOAD_BYTES // (1024*1024)}MB per batch)...")
    batches = batch_events(all_events, args.api_key)
    print(f"Created {len(batches)} batch(es)")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate shell scripts
    print(f"\nGenerating shell scripts in {output_dir}...")
    for i, batch in enumerate(batches, 1):
        script_path = generate_curl_script(i, args.api_key, batch, output_dir, args.eu)
        payload_size = estimate_payload_size(args.api_key, batch)
        print(f"  {script_path.name}: {len(batch)} events, {payload_size:,} bytes")

    # Generate run_all.sh
    run_all_path = generate_run_all_script(output_dir, len(batches), args.delay)
    print(f"\nGenerated master script: {run_all_path}")

    print(f"\n{'='*60}")
    print(f"Bundle complete!")
    print(f"  Total events: {len(all_events)}")
    print(f"  Total batches: {len(batches)}")
    print(f"  Output directory: {output_dir}")
    print(f"\nTo upload all events, run:")
    print(f"  bash {output_dir}/run_all.sh")
    print(f"\nOr run individual batches:")
    print(f"  bash {output_dir}/batch_0001.sh")


if __name__ == "__main__":
    main()
