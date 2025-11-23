#!/usr/bin/env python3
"""
Reset OneSignal Tags - Production Version

Rate Limits (from OneSignal docs):
- 1000 requests/sec/app (we'll use 50% = 500 req/sec max)
- 1 request/sec/user or subscription
- Up to 100 property modifications per request

Features:
- Retry logic for 404 (CSV not ready)
- Precise rate limiting at 50% of limit
- True parallel execution with ThreadPoolExecutor
- Resume capability
- Progress tracking
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import gzip
import csv
import json
import time
import os
import logging
import threading
from dataclasses import dataclass
from typing import Iterator, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import tempfile

# Suppress urllib3 connection pool warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Optional: progress bar
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Thread-safe rate limiter using sliding window algorithm.
    Ensures we don't exceed max_requests_per_second.
    """
    
    def __init__(self, max_requests_per_second: float):
        self.max_rps = max_requests_per_second
        self.min_interval = 1.0 / max_requests_per_second
        self.lock = threading.Lock()
        self.request_times: deque = deque()
        self.window_size = 1.0  # 1 second window
    
    def acquire(self):
        """
        Block until we can make a request within rate limits.
        Thread-safe.
        """
        with self.lock:
            now = time.monotonic()
            
            # Remove old requests outside the window
            while self.request_times and (now - self.request_times[0]) > self.window_size:
                self.request_times.popleft()
            
            # If we're at the limit, wait
            if len(self.request_times) >= self.max_rps:
                # Wait until the oldest request exits the window
                sleep_time = self.window_size - (now - self.request_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.monotonic()
                    # Clean up again after sleeping
                    while self.request_times and (now - self.request_times[0]) > self.window_size:
                        self.request_times.popleft()
            
            # Record this request
            self.request_times.append(now)
    
    def get_current_rate(self) -> float:
        """Get current requests per second (for monitoring)."""
        with self.lock:
            now = time.monotonic()
            # Count requests in the last second
            count = sum(1 for t in self.request_times if (now - t) <= 1.0)
            return count


@dataclass
class Config:
    """Configuration for the OneSignal tag reset script."""
    app_id: str = "828cd580-3be3-4041-9c87-4576960ac1d2"
    alias_label: str = "onesignal_id"
    api_key: str = ""
    
    # Rate limiting - OneSignal allows 1000 req/sec, we use 50%
    max_requests_per_second: float = 500.0  # 50% of 1000
    
    # Concurrency - number of parallel workers
    max_workers: int = 50  # Threads making parallel requests
    
    # CSV download retry settings
    csv_download_max_retries: int = 30  # Max attempts to download CSV
    csv_download_retry_delay: float = 10.0  # Seconds between retries
    csv_download_initial_wait: float = 20.0  # Initial wait before first attempt
    
    # API call retry settings
    api_max_retries: int = 3
    api_retry_delay: float = 1.0
    
    # Resume support
    progress_file: str = "onesignal_reset_progress.json"
    enable_resume: bool = True
    
    # Progress save frequency
    save_progress_every: int = 100  # Save after every N users
    
    # Temp directory for large files
    temp_dir: Optional[str] = None


class ProgressTracker:
    """Thread-safe progress tracker with persistence."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.processed_ids: Set[str] = set()
        self.lock = threading.Lock()
        self.unsaved_count = 0
        self.load()
    
    def load(self):
        """Load progress from file."""
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    data = json.load(f)
                    self.processed_ids = set(data.get("processed_ids", []))
                logger.info(f"Resumed: {len(self.processed_ids)} users already processed")
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")
    
    def save(self, force: bool = False):
        """Save progress to file (thread-safe)."""
        with self.lock:
            if force or self.unsaved_count >= 100:
                with open(self.filepath, 'w') as f:
                    json.dump({"processed_ids": list(self.processed_ids)}, f)
                self.unsaved_count = 0
    
    def mark_processed(self, user_id: str):
        """Mark a user as processed (thread-safe)."""
        with self.lock:
            self.processed_ids.add(user_id)
            self.unsaved_count += 1
    
    def is_processed(self, user_id: str) -> bool:
        """Check if user was already processed."""
        return user_id in self.processed_ids
    
    def clear(self):
        """Clear all progress."""
        with self.lock:
            self.processed_ids.clear()
            if os.path.exists(self.filepath):
                os.remove(self.filepath)


class OneSignalTagResetter:
    """
    Production-ready OneSignal tag resetter with:
    - Proper rate limiting (50% of 1000 req/sec)
    - Parallel execution
    - Retry logic for 404
    - Resume capability
    """
    
    BASE_URL = "https://api.onesignal.com"
    
    def __init__(self, config: Config):
        self.config = config
        
        # Configure session with connection pool sized for our workers
        self.session = requests.Session()
        
        # Set up adapter with connection pool matching worker count
        # pool_connections = number of connection pools to cache
        # pool_maxsize = max connections per pool
        # pool_block = True means wait for connection instead of creating new one
        adapter = HTTPAdapter(
            pool_connections=100,
            pool_maxsize=config.max_workers + 10,  # Slightly more than workers
            max_retries=0,  # We handle retries ourselves
            pool_block=True  # Wait for available connection instead of discarding
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.session.headers.update({
            "Authorization": f"Key {config.api_key}",
            "Content-Type": "application/json"
        })
        
        # Rate limiter at 50% of max (500 req/sec)
        self.rate_limiter = RateLimiter(config.max_requests_per_second)
        
        # Progress tracker
        self.progress = ProgressTracker(config.progress_file) if config.enable_resume else None
        
        # Stats (thread-safe)
        self.stats_lock = threading.Lock()
        self.stats = {
            "total_users": 0,
            "users_with_tags": 0,
            "users_updated": 0,
            "users_skipped": 0,
            "users_not_found": 0,  # 404 - user doesn't exist
            "client_errors": 0,    # Other 4xx errors
            "errors": 0,           # Actual failures
            "retries": 0
        }
    
    def export_subscribers(self) -> str:
        """Request CSV export from OneSignal."""
        url = f"{self.BASE_URL}/players/csv_export"
        params = {"app_id": self.config.app_id}
        payload = {
            "extra_fields": ["external_user_id", "player_id", "onesignal_id"],
            "segment_name": "Subscribed Users"
        }
        
        logger.info("Requesting CSV export from OneSignal...")
        response = self.session.post(url, params=params, json=payload)
        response.raise_for_status()
        
        result = response.json()
        csv_url = result.get("csv_file_url")
        
        if not csv_url:
            raise ValueError(f"No CSV URL in response: {result}")
        
        logger.info(f"CSV export URL: {csv_url}")
        return csv_url
    
    def download_csv_with_retry(self, csv_url: str) -> str:
        """
        Download CSV with retry logic for 404 (file not ready).
        Returns path to decompressed CSV file.
        """
        temp_dir = self.config.temp_dir or tempfile.gettempdir()
        gzip_path = os.path.join(temp_dir, "onesignal_export.csv.gz")
        csv_path = os.path.join(temp_dir, "onesignal_export.csv")
        
        # Initial wait
        logger.info(f"Waiting {self.config.csv_download_initial_wait}s for export to be ready...")
        time.sleep(self.config.csv_download_initial_wait)
        
        # Retry loop for download
        for attempt in range(1, self.config.csv_download_max_retries + 1):
            try:
                logger.info(f"Download attempt {attempt}/{self.config.csv_download_max_retries}...")
                
                response = requests.get(csv_url, stream=True, timeout=60)
                
                if response.status_code == 404:
                    logger.warning(f"CSV not ready (404). Retrying in {self.config.csv_download_retry_delay}s...")
                    time.sleep(self.config.csv_download_retry_delay)
                    continue
                
                response.raise_for_status()
                
                # Download successful - stream to file
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(gzip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size:
                                pct = (downloaded / total_size) * 100
                                print(f"\rDownloading: {pct:.1f}% ({downloaded / 1024 / 1024:.1f} MB)", end="", flush=True)
                
                print()  # New line
                logger.info(f"Downloaded {downloaded / 1024 / 1024:.2f} MB")
                break
                
            except requests.exceptions.RequestException as e:
                if attempt < self.config.csv_download_max_retries:
                    logger.warning(f"Download error: {e}. Retrying in {self.config.csv_download_retry_delay}s...")
                    time.sleep(self.config.csv_download_retry_delay)
                else:
                    raise RuntimeError(f"Failed to download CSV after {attempt} attempts: {e}")
        else:
            raise RuntimeError(f"CSV file not ready after {self.config.csv_download_max_retries} attempts")
        
        # Decompress
        logger.info("Decompressing gzip file...")
        with gzip.open(gzip_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                while True:
                    chunk = f_in.read(65536)
                    if not chunk:
                        break
                    f_out.write(chunk)
        
        os.remove(gzip_path)
        csv_size = os.path.getsize(csv_path)
        logger.info(f"Decompressed CSV: {csv_size / 1024 / 1024:.2f} MB")
        
        return csv_path
    
    def stream_users_from_csv(self, csv_path: str) -> Iterator[dict]:
        """Stream users who have non-empty tags."""
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                self.stats["total_users"] += 1
                
                onesignal_id = row.get("onesignal_id")
                if not onesignal_id:
                    continue
                
                # Skip if already processed
                if self.progress and self.progress.is_processed(onesignal_id):
                    self.stats["users_skipped"] += 1
                    continue
                
                # Parse tags
                tags_str = row.get("tags", "{}")
                if not tags_str or tags_str.strip() in ["", "{}", "null"]:
                    continue
                
                try:
                    tags = json.loads(tags_str)
                    if tags and isinstance(tags, dict) and len(tags) > 0:
                        self.stats["users_with_tags"] += 1
                        yield {
                            "onesignal_id": onesignal_id,
                            "tags": tags
                        }
                except json.JSONDecodeError:
                    continue
    
    def update_user_tags(self, user: dict) -> tuple[str, bool, str]:
        """
        Update a user's tags via API.
        Uses rate limiter to control throughput.
        Returns (user_id, success, status).
        
        Status can be:
        - "updated": Successfully updated
        - "not_found": User doesn't exist (404) - skip gracefully
        - "error": Other error occurred
        """
        onesignal_id = user["onesignal_id"]
        url = (
            f"{self.BASE_URL}/apps/{self.config.app_id}"
            f"/users/by/{self.config.alias_label}/{onesignal_id}"
        )
        
        # Create payload - set all tags to empty string
        empty_tags = {key: "" for key in user["tags"].keys()}
        payload = {"properties": {"tags": empty_tags}}
        
        for attempt in range(self.config.api_max_retries):
            try:
                # Acquire rate limit token (blocks if needed)
                self.rate_limiter.acquire()
                
                response = self.session.patch(url, json=payload, timeout=30)
                
                # Handle 404 - User not found (just skip, don't retry)
                if response.status_code == 404:
                    logger.debug(f"User not found (404): {onesignal_id} - skipping")
                    return (onesignal_id, True, "not_found")  # Return True so we mark as processed
                
                # Handle 429 - Rate limited (retry with backoff)
                if response.status_code == 429:
                    with self.stats_lock:
                        self.stats["retries"] += 1
                    retry_after = int(response.headers.get("Retry-After", 1))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                # Handle other 4xx errors (don't retry, just skip)
                if 400 <= response.status_code < 500:
                    logger.warning(f"Client error {response.status_code} for {onesignal_id}: {response.text[:100]}")
                    return (onesignal_id, True, "client_error")  # Mark as processed, move on
                
                # Handle 5xx errors (retry)
                if response.status_code >= 500:
                    raise requests.RequestException(f"Server error: {response.status_code}")
                
                response.raise_for_status()
                return (onesignal_id, True, "updated")
                
            except requests.RequestException as e:
                with self.stats_lock:
                    self.stats["retries"] += 1
                if attempt < self.config.api_max_retries - 1:
                    time.sleep(self.config.api_retry_delay)
                else:
                    logger.error(f"Failed {onesignal_id} after {self.config.api_max_retries} attempts: {e}")
                    return (onesignal_id, False, "error")
        
        return (onesignal_id, False, "error")
    
    def process_users_parallel(self, users: Iterator[dict]):
        """
        Process users with true parallel execution.
        Rate limiter ensures we stay at 50% of limit.
        """
        submitted_count = 0
        start_time = time.time()
        last_log_time = start_time
        
        def process_and_track(user):
            """Process user and update stats."""
            user_id, success, status = self.update_user_tags(user)
            
            with self.stats_lock:
                if status == "updated":
                    self.stats["users_updated"] += 1
                elif status == "not_found":
                    self.stats["users_not_found"] += 1
                elif status == "client_error":
                    self.stats["client_errors"] += 1
                else:  # error
                    self.stats["errors"] += 1
                
                # Mark as processed regardless (so we don't retry on resume)
                if success and self.progress:
                    self.progress.mark_processed(user_id)
            
            return success
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {}
            buffer_size = self.config.max_workers * 2
            
            # Wrap iterator with tqdm if available
            user_iter = tqdm(users, desc="Processing", unit="user") if HAS_TQDM else users
            
            for user in user_iter:
                # Submit work
                future = executor.submit(process_and_track, user)
                futures[future] = user["onesignal_id"]
                submitted_count += 1
                
                # Clean up completed futures periodically
                if len(futures) >= buffer_size:
                    done = [f for f in futures if f.done()]
                    for f in done:
                        del futures[f]
                
                # Log progress every 10 seconds (time-based, not count-based)
                current_time = time.time()
                if current_time - last_log_time >= 10:
                    with self.stats_lock:
                        completed = self.stats["users_updated"] + self.stats["users_not_found"] + \
                                   self.stats["client_errors"] + self.stats["errors"]
                        elapsed = current_time - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        in_flight = submitted_count - completed
                        logger.info(
                            f"Progress: {completed:,} done, {in_flight} in-flight, "
                            f"{self.stats['errors']} errors, {rate:.1f}/sec"
                        )
                    
                    # Save progress
                    if self.progress:
                        self.progress.save(force=True)
                    
                    last_log_time = current_time
            
            # Wait for remaining futures
            if futures:
                logger.info(f"Waiting for {len(futures)} remaining requests...")
            for future in as_completed(futures):
                pass
        
        # Final save
        if self.progress:
            self.progress.save(force=True)
    
    def run(self, csv_url: Optional[str] = None):
        """Execute the full workflow."""
        logger.info("=" * 70)
        logger.info("OneSignal Tag Reset - Production Version")
        logger.info(f"Rate limit: {self.config.max_requests_per_second} req/sec (50% of max)")
        logger.info(f"Workers: {self.config.max_workers}")
        logger.info("=" * 70)
        
        csv_path = None
        start_time = time.time()
        
        try:
            # Step 1: Get CSV URL
            if not csv_url:
                csv_url = self.export_subscribers()
            
            # Step 2: Download with retry for 404
            csv_path = self.download_csv_with_retry(csv_url)
            
            # Step 3: Process users in parallel
            logger.info("Processing users...")
            users = self.stream_users_from_csv(csv_path)
            self.process_users_parallel(users)
            
            # Final stats
            elapsed = time.time() - start_time
            self._print_stats(elapsed)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️  Interrupted! Progress saved.")
            if self.progress:
                self.progress.save(force=True)
            elapsed = time.time() - start_time
            self._print_stats(elapsed)
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            if self.progress:
                self.progress.save(force=True)
            raise
            
        finally:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
                logger.info("Cleaned up temp files")
    
    def _print_stats(self, elapsed: float):
        """Print final statistics."""
        rate = self.stats["users_updated"] / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 70)
        logger.info("RESULTS")
        logger.info("=" * 70)
        logger.info(f"  Total users in CSV:    {self.stats['total_users']:,}")
        logger.info(f"  Users with tags:       {self.stats['users_with_tags']:,}")
        logger.info(f"  Successfully updated:  {self.stats['users_updated']:,}")
        logger.info(f"  Skipped (resumed):     {self.stats['users_skipped']:,}")
        logger.info(f"  Not found (404):       {self.stats['users_not_found']:,}")
        logger.info(f"  Client errors (4xx):   {self.stats['client_errors']:,}")
        logger.info(f"  Failed (errors):       {self.stats['errors']:,}")
        logger.info(f"  API retries:           {self.stats['retries']:,}")
        logger.info(f"  Time elapsed:          {elapsed:.1f} seconds")
        logger.info(f"  Average throughput:    {rate:.1f} users/sec")
        logger.info("=" * 70)


def load_config_from_file(config_path: str = "config.json") -> dict:
    """Load configuration from JSON file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, config_path)
    
    if not os.path.exists(config_file):
        # Create sample config file
        sample_config = {
            "app_id": "YOUR_APP_ID_HERE",
            "alias_label": "onesignal_id",
            "api_key": "YOUR_ONESIGNAL_REST_API_KEY_HERE",
            
            "# Optional settings below": "",
            "max_requests_per_second": 500,
            "max_workers": 50,
            "enable_resume": True,
            "csv_url": None
        }
        with open(config_file, 'w') as f:
            json.dump(sample_config, f, indent=2)
        
        logger.error(f"Config file not found. Created sample config at: {config_file}")
        logger.error("Please edit config.json with your OneSignal credentials and run again.")
        return None
    
    with open(config_file, 'r') as f:
        return json.load(f)


def main():
    """Main entry point - loads config from config.json."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Reset OneSignal user tags")
    parser.add_argument("--clear-progress", action="store_true", help="Clear progress and start fresh")
    parser.add_argument("--config", default="config.json", help="Path to config file (default: config.json)")
    args = parser.parse_args()
    
    # Load config from JSON
    config_data = load_config_from_file(args.config)
    if config_data is None:
        return
    
    # Validate required fields
    if config_data.get("api_key", "").startswith("YOUR_"):
        logger.error("Please update 'api_key' in config.json with your actual OneSignal REST API Key")
        return
    
    if config_data.get("app_id", "").startswith("YOUR_"):
        logger.error("Please update 'app_id' in config.json with your actual OneSignal App ID")
        return
    
    # Build config object
    rate_limit = config_data.get("max_requests_per_second", 500)
    if rate_limit > 500:
        logger.warning(f"Rate limit {rate_limit} exceeds 50% of max. Using 500.")
        rate_limit = 500
    
    config = Config(
        app_id=config_data["app_id"],
        alias_label=config_data.get("alias_label", "onesignal_id"),
        api_key=config_data["api_key"],
        max_workers=config_data.get("max_workers", 50),
        max_requests_per_second=rate_limit,
        enable_resume=config_data.get("enable_resume", True)
    )
    
    logger.info(f"Loaded config: app_id={config.app_id}, alias_label={config.alias_label}")
    
    resetter = OneSignalTagResetter(config)
    
    if args.clear_progress and resetter.progress:
        resetter.progress.clear()
        logger.info("Progress cleared")
    
    # Check for pre-existing CSV URL in config
    csv_url = config_data.get("csv_url")
    resetter.run(csv_url=csv_url)


if __name__ == "__main__":
    main()
