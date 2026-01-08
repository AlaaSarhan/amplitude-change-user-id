#!/usr/bin/env python3
"""Convert exported Amplitude events to the Batch Upload API format.

Run with: poetry run python scripts/convert_events.py --input ./exports --output ./converted
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path


# Mapping from Export API field names to Upload API field names
# Some fields have different names between export and upload
FIELD_MAPPING = {
    # Core identifiers (direct mapping)
    "user_id": "user_id",
    "device_id": "device_id",
    "event_type": "event_type",

    # Properties (direct mapping)
    "event_properties": "event_properties",
    "user_properties": "user_properties",
    "group_properties": "group_properties",
    "groups": "groups",

    # Session and event IDs
    "session_id": "session_id",
    "event_id": "event_id",
    "insert_id": "insert_id",

    # Device information
    "platform": "platform",
    "os_name": "os_name",
    "os_version": "os_version",
    "device_brand": "device_brand",
    "device_manufacturer": "device_manufacturer",
    "device_model": "device_model",
    "device_type": "device_type",
    "carrier": "carrier",

    # Location
    "country": "country",
    "region": "region",
    "city": "city",
    "dma": "dma",
    "location_lat": "location_lat",
    "location_lng": "location_lng",

    # Other
    "language": "language",
    "ip_address": "ip_address",
    "library": "library",
    "app_version": "app_version",

    # Revenue fields
    "price": "price",
    "quantity": "quantity",
    "revenue": "revenue",
    "productId": "productId",
    "revenueType": "revenueType",

    # Mobile identifiers
    "idfa": "idfa",
    "idfv": "idfv",
    "adid": "adid",
    "android_id": "android_id",
}


def parse_timestamp(event: dict) -> int | None:
    """Parse event timestamp and convert to milliseconds.

    Export API provides event_time as a string like "2024-01-15 10:30:45.123456"
    Upload API expects time in milliseconds since epoch.
    """
    # Try different timestamp fields in order of preference
    for field in ["event_time", "client_event_time", "server_received_time"]:
        if field in event and event[field]:
            try:
                # Handle format: "2024-01-15 10:30:45.123456"
                ts_str = event[field]
                if "." in ts_str:
                    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                return int(dt.timestamp() * 1000)
            except (ValueError, TypeError):
                continue

    # If we have a numeric timestamp already
    if "time" in event and event["time"]:
        return int(event["time"])

    return None


def convert_event(export_event: dict) -> dict | None:
    """Convert a single event from export format to upload format."""
    upload_event = {}

    # Map standard fields
    for export_field, upload_field in FIELD_MAPPING.items():
        if export_field in export_event and export_event[export_field] is not None:
            value = export_event[export_field]
            # Skip empty strings and empty dicts
            if value == "" or value == {}:
                continue
            upload_event[upload_field] = value

    # Handle timestamp conversion
    timestamp = parse_timestamp(export_event)
    if timestamp:
        upload_event["time"] = timestamp

    # Validate required fields
    has_user_id = upload_event.get("user_id")
    has_device_id = upload_event.get("device_id")
    has_event_type = upload_event.get("event_type")

    if not (has_user_id or has_device_id):
        return None  # Skip events without user identification

    if not has_event_type:
        return None  # Skip events without event type

    return upload_event


def process_json_file(input_path: Path) -> list[dict]:
    """Process a single JSON file (one event per line) and return converted events."""
    converted = []
    skipped = 0

    with open(input_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                export_event = json.loads(line)
                upload_event = convert_event(export_event)

                if upload_event:
                    converted.append(upload_event)
                else:
                    skipped += 1

            except json.JSONDecodeError as e:
                print(f"  Warning: Invalid JSON at line {line_num}: {e}")
                skipped += 1

    if skipped > 0:
        print(f"  Skipped {skipped} invalid/incomplete events")

    return converted


def main():
    parser = argparse.ArgumentParser(
        description="Convert exported Amplitude events to Upload API format"
    )
    parser.add_argument(
        "--input",
        default="./exports",
        help="Input directory containing exported JSON files (default: ./exports)"
    )
    parser.add_argument(
        "--output",
        default="./converted",
        help="Output directory for converted JSON files (default: ./converted)"
    )

    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}")
        return

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all JSON files in input directory
    json_files = list(input_dir.glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {input_dir}")
        return

    print(f"Found {len(json_files)} JSON file(s) to convert\n")

    total_events = 0

    for json_file in sorted(json_files):
        print(f"Processing: {json_file.name}")

        converted_events = process_json_file(json_file)
        total_events += len(converted_events)

        # Write converted events to output file (one event per line for consistency)
        output_file = output_dir / f"converted_{json_file.name}"
        with open(output_file, 'w', encoding='utf-8') as f:
            for event in converted_events:
                f.write(json.dumps(event) + "\n")

        print(f"  -> Converted {len(converted_events)} events to {output_file}")

    print(f"\nConversion complete! Total events converted: {total_events}")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
