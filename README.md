# OneSignal Tags Manager

A Python script to manage OneSignal user tags at scale. Handles large user bases efficiently with parallel processing, rate limiting, and resume capability.

## Features

- **Reset tags** - Clear all tags or preserve specific ones
- **Sync user_mode** - Set `user_mode` tag from a local CSV file
- **Large scale** - Handles 100k+ users efficiently
- **Rate limited** - Stays within OneSignal API limits (50% of max)
- **Resumable** - Saves progress, can resume if interrupted
- **Parallel** - Concurrent API calls with bounded parallelism

## Requirements

```bash
pip install requests tqdm
```

- `requests` - Required for API calls
- `tqdm` - Optional, for progress bar

## Quick Start

1. Edit `config.json` with your OneSignal credentials:

```json
{
  "app_id": "your-app-id",
  "api_key": "your-rest-api-key",
  "preserve_tags": [],
  "sync_user_mode": false
}
```

2. Run:

```bash
python onesignal_tags_manager.py
```

## Configuration

### Required Fields

| Field | Description |
|-------|-------------|
| `app_id` | Your OneSignal App ID |
| `api_key` | Your OneSignal REST API Key (Settings → Keys & IDs) |

### Tag Reset Options

| Field | Type | Description |
|-------|------|-------------|
| `preserve_tags` | list | Tags to keep (not reset). Empty `[]` = reset ALL tags |

### User Mode Sync Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sync_user_mode` | bool | `false` | Enable syncing `user_mode` from local CSV |
| `local_csv_path` | string | `"users.csv"` | Path to local CSV file |
| `local_csv_user_id_column` | string | `"user_id"` | Column name for external_id |
| `local_csv_user_type_column` | string | `"user_type"` | Column name for user_mode value |
| `default_user_mode` | string | `"free"` | Value if user not in local CSV |
| `no_external_id_user_mode` | string | `"unknown"` | Value if user has no external_id |

### Performance Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_requests_per_second` | number | `500` | Rate limit (max 500 = 50% of OneSignal limit) |
| `max_workers` | number | `50` | Parallel workers |
| `enable_resume` | bool | `true` | Save progress for resume |
| `csv_url` | string | `null` | Skip export, use existing CSV URL |

## Usage Modes

### Mode 1: Full Reset (Clear ALL Tags)

Reset every tag for every user to empty string.

```json
{
  "app_id": "...",
  "api_key": "...",
  "preserve_tags": [],
  "sync_user_mode": false
}
```

### Mode 2: Reset All Except Specific Tags

Reset all tags except the ones you want to keep.

```json
{
  "app_id": "...",
  "api_key": "...",
  "preserve_tags": ["user_mode", "subscription_type"],
  "sync_user_mode": false
}
```

### Mode 3: Reset + Sync user_mode from CSV

Reset tags AND set `user_mode` based on a local CSV lookup.

```json
{
  "app_id": "...",
  "api_key": "...",
  "preserve_tags": ["user_mode"],
  "sync_user_mode": true,
  "local_csv_path": "users.csv",
  "local_csv_user_id_column": "user_id",
  "local_csv_user_type_column": "user_type",
  "default_user_mode": "free",
  "no_external_id_user_mode": "unknown"
}
```

**Logic:**

| Condition | user_mode value |
|-----------|-----------------|
| User has no `external_user_id` | `"unknown"` |
| User's `external_user_id` found in local CSV | Value from `user_type` column |
| User's `external_user_id` NOT in local CSV | `"free"` |

## Local CSV Format

When using `sync_user_mode`, your CSV should look like:

```csv
user_id,user_type
abc123,premium
xyz789,trial
def456,pro
user001,free
```

- `user_id` column → matches OneSignal's `external_user_id`
- `user_type` column → value to set as `user_mode` tag

## Command Line

```bash
# Run with default config.json
python onesignal_tags_manager.py

# Use a different config file
python onesignal_tags_manager.py --config production_config.json

# Clear progress and start fresh
python onesignal_tags_manager.py --clear-progress
```

## Resume Capability

The script saves progress to `onesignal_manager_progress.json`. If interrupted:

- **Ctrl+C** - Progress is saved, can resume
- **Error/Crash** - Progress is saved, can resume
- **Re-run** - Automatically skips already-processed users

To start fresh:

```bash
python onesignal_tags_manager.py --clear-progress
```

## Output Example

```
======================================================================
OneSignal Tags Manager v4
Mode: PRESERVE: ['user_mode']
Sync user_mode: YES
Rate: 500/sec, Workers: 50
======================================================================
Loaded 15,000 users from local CSV
Requesting CSV export...
Download attempt 1/30
Downloading: 100.0%
Downloaded 12.5 MB
Decompressing...
CSV ready: 45.2 MB
Processing users...
Progress: 10,000 done, 87 active | matched=5,000 default=4,500 no_ext=500 | 99.5/sec
Progress: 20,000 done, 92 active | matched=10,200 default=8,800 no_ext=1,000 | 101.2/sec
...
======================================================================
RESULTS
======================================================================
  Total users:           63,602
  With tags:             47,545
  Updated:               47,545
    ├─ Matched local:    12,345
    ├─ Default (free):   30,200
    └─ No external_id:    5,000
  Skipped (resumed):     0
  Not found (404):       0
  Client errors:         0
  Errors:                0
  Retries:               12
  Time:                  478.3s
  Throughput:            99.4/sec
======================================================================
```

## Troubleshooting

### "CSV not ready (404)"

The export takes time to generate. The script automatically retries up to 30 times with 10s delays. If it still fails, try increasing the initial wait:

```json
{
  "csv_download_initial_wait": 60
}
```

### Rate Limited (429 errors)

Reduce rate and workers:

```json
{
  "max_requests_per_second": 200,
  "max_workers": 20
}
```

### Connection Pool Warnings

These are suppressed by default. If you see issues, reduce workers:

```json
{
  "max_workers": 30
}
```

### Out of Memory

The script streams the OneSignal CSV, but loads the local CSV into memory. If your local CSV is huge (millions of rows), consider splitting it or filtering it first.

## Files

| File | Purpose |
|------|---------|
| `onesignal_tags_manager.py` | Main script |
| `config.json` | Configuration |
| `onesignal_manager_progress.json` | Resume progress (auto-created) |
| `users.csv` | Your local user data (if syncing user_mode) |

## Finding Your OneSignal Credentials

1. Go to [OneSignal Dashboard](https://onesignal.com)
2. Select your app
3. Go to **Settings** → **Keys & IDs**
4. Copy:
   - **OneSignal App ID** → `app_id`
   - **REST API Key** → `api_key`

## License

MIT - Use freely.
