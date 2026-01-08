#!/usr/bin/env python3
"""Export data from Amplitude Export API and unzip into JSON files.

Run with: poetry run python scripts/export_data.py --api-key KEY --secret-key SECRET --start 20240101T00 --end 20240101T23 --output ./exports
"""

import argparse
import base64
import gzip
import io
import shutil
import sys
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError


def get_auth_header(api_key: str, secret_key: str) -> str:
    """Generate Basic Auth header from API key and secret."""
    credentials = f"{api_key}:{secret_key}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def export_data(api_key: str, secret_key: str, start: str, end: str, eu: bool = False) -> bytes:
    """Call the Amplitude Export API and return the response data."""
    base_url = "https://analytics.eu.amplitude.com" if eu else "https://amplitude.com"
    url = f"{base_url}/api/2/export?start={start}&end={end}"

    request = Request(url)
    request.add_header("Authorization", get_auth_header(api_key, secret_key))

    print(f"Auth header: {get_auth_header(api_key, secret_key)}")
    print(f"Exporting data from {start} to {end}...")
    print(f"URL: {url}")

    try:
        with urlopen(request) as response:
            return response.read()
    except HTTPError as e:
        if e.code == 400:
            print("Error 400: Export file exceeds 4GB limit. Try a smaller date range.")
        elif e.code == 404:
            print("Error 404: No data found for the specified date range.")
        elif e.code == 504:
            print("Error 504: Request timeout. Try a smaller date range.")
        else:
            print(f"HTTP Error {e.code}: {e.reason}")
        sys.exit(1)


def extract_to_folder(data: bytes, output_dir: Path) -> int:
    """Extract compressed data (zip containing gzipped JSON) to output folder.

    Returns the number of JSON files extracted.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    json_count = 0

    # The export API returns a zip file
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for zip_info in zf.namelist():
            print(f"Processing: {zip_info}")

            file_data = zf.read(zip_info)

            # Each file in the zip is gzipped JSON
            if zip_info.endswith('.json.gz'):
                # Decompress gzip
                decompressed = gzip.decompress(file_data)
                output_name = Path(zip_info).stem  # Remove .gz extension
                output_path = output_dir / output_name

                with open(output_path, 'wb') as f:
                    f.write(decompressed)
                json_count += 1
                print(f"  -> Extracted: {output_path}")

            elif zip_info.endswith('.json'):
                # Already JSON, just copy
                output_path = output_dir / Path(zip_info).name
                with open(output_path, 'wb') as f:
                    f.write(file_data)
                json_count += 1
                print(f"  -> Extracted: {output_path}")

            elif zip_info.endswith('.gz'):
                # Generic gzip file
                decompressed = gzip.decompress(file_data)
                output_name = Path(zip_info).stem
                output_path = output_dir / output_name

                with open(output_path, 'wb') as f:
                    f.write(decompressed)
                json_count += 1
                print(f"  -> Extracted: {output_path}")

    return json_count


def main():
    parser = argparse.ArgumentParser(
        description="Export data from Amplitude and extract JSON files"
    )
    parser.add_argument("--api-key", required=True, help="Amplitude API key")
    parser.add_argument("--secret-key", required=True, help="Amplitude Secret key")
    parser.add_argument(
        "--start",
        required=True,
        help="Start date/hour in format YYYYMMDDTHH (e.g., 20240101T00)"
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date/hour in format YYYYMMDDTHH (e.g., 20240101T23)"
    )
    parser.add_argument(
        "--output",
        default="./exports",
        help="Output directory for JSON files (default: ./exports)"
    )
    parser.add_argument(
        "--eu",
        action="store_true",
        help="Use EU data residency endpoint"
    )

    args = parser.parse_args()

    output_dir = Path(args.output)

    # Clean output directory if it exists
    if output_dir.exists():
        print(f"Cleaning existing output directory: {output_dir}")
        shutil.rmtree(output_dir)

    # Export data
    data = export_data(args.api_key, args.secret_key, args.start, args.end, args.eu)
    print(f"Downloaded {len(data)} bytes")

    # Extract to folder
    json_count = extract_to_folder(data, output_dir)
    print(f"\nExport complete! Extracted {json_count} JSON file(s) to {output_dir}")


if __name__ == "__main__":
    main()
