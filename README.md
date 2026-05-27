# amplitude-change-user-id
A tool to change your Amplitude User IDs

All code has been generated using Claude Code. Has not been tested for production by the author yet.

## Usage

Run the following scripts in order:
1. Exports events from Amplitude
    
    `poetry run python scripts/export_data.py --eu --api-key KEY --secret-key SECRET --start 20240101T00 --end 20240101T23 --output ./exports`
2. Convert exported event files to backfill (import)
    
    `poetry run python scripts/convert_events.py --input ./exports --output ./converted`
3. Replace user ids
    
    `poetry run python scripts/replace_user_ids.py --csv mapping.csv`
    
    Input CSV needs to have two columns:
    - `old_user_id`
    - `new_user_id`
4. Bundle import events into CURL batch requests
    
    `poetry run python scripts/bundle_requests.py --input ./converted --output ./requests --api-key YOUR_API_KEY`
5. Execute the bundled request script files
    
    `./requests/run_all.sh`
