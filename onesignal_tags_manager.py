#!/usr/bin/env python3
"""
OneSignal Tags Manager v4

Flexible tag management:
- Reset all tags (if preserve_tags is empty)
- Reset all tags EXCEPT specified ones
- Sync user_mode from local CSV

Usage:
  python onesignal_tags_manager.py
  python onesignal_tags_manager.py --config my_config.json
  python onesignal_tags_manager.py --clear-progress

Rate Limits (OneSignal):
- 1000 requests/sec/app (we use 50% = 500 req/sec max)
"""

import requests
from requests.adapters import HTTPAdapter
import urllib3
import gzip
import csv
import json
import time
import os
import logging
import threading
from dataclasses import dataclass, field
from typing import Iterator, Optional, Set, Dict, List
from concurrent.futures import ThreadPoolExecutor
import tempfile

# Suppress urllib3 warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe sliding window rate limiter."""
    
    def __init__(self, max_rps: float):
        self.max_rps = max_rps
        self.lock = threading.Lock()
        self.timestamps: List[float] = []
    
    def acquire(self):
        with self.lock:
            now = time.monotonic()
            # Remove timestamps older than 1 second
            self.timestamps = [t for t in self.timestamps if now - t < 1.0]
            
            if len(self.timestamps) >= self.max_rps:
                sleep_time = 1.0 - (now - self.timestamps[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.monotonic()
                    self.timestamps = [t for t in self.timestamps if now - t < 1.0]
            
            self.timestamps.append(now)


@dataclass
class Config:
    # OneSignal
    app_id: str = ""
    api_key: str = ""
    
    # Tags to preserve (not reset). Empty list = reset ALL tags
    preserve_tags: List[str] = field(default_factory=list)
    
    # User mode sync from local CSV
    sync_user_mode: bool = False
    local_csv_path: str = ""
    local_csv_user_id_column: str = "user_id"  # maps to external_user_id
    local_csv_user_type_column: str = "user_type"  # value to set as user_mode
    default_user_mode: str = "free"  # if not in local CSV
    no_external_id_user_mode: str = "unknown"  # if user has no external_id
    
    # Rate limiting (50% of 1000)
    max_requests_per_second: float = 500.0
    max_workers: int = 50
    
    # Download settings
    csv_download_max_retries: int = 30
    csv_download_retry_delay: float = 10.0
    csv_download_initial_wait: float = 20.0
    
    # API retry
    api_max_retries: int = 3
    api_retry_delay: float = 1.0
    
    # Resume
    progress_file: str = "onesignal_manager_progress.json"
    enable_resume: bool = True
    
    # Optional: skip export, use existing URL
    csv_url: Optional[str] = None
    
    temp_dir: Optional[str] = None


class ProgressTracker:
    """Thread-safe progress with persistence."""
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.processed: Set[str] = set()
        self.lock = threading.Lock()
        self._load()
    
    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    self.processed = set(json.load(f).get("processed", []))
                logger.info(f"Resumed: {len(self.processed):,} already processed")
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")
    
    def save(self):
        with self.lock:
            with open(self.filepath, 'w') as f:
                json.dump({"processed": list(self.processed)}, f)
    
    def mark(self, user_id: str):
        with self.lock:
            self.processed.add(user_id)
    
    def is_done(self, user_id: str) -> bool:
        return user_id in self.processed
    
    def clear(self):
        with self.lock:
            self.processed.clear()
            if os.path.exists(self.filepath):
                os.remove(self.filepath)


class OneSignalTagsManager:
    """Manage OneSignal user tags - reset and sync."""
    
    BASE_URL = "https://api.onesignal.com"
    
    def __init__(self, config: Config):
        self.config = config
        
        # HTTP session with connection pool
        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=100,
            pool_maxsize=config.max_workers + 10,
            max_retries=0,
            pool_block=True
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "Authorization": f"Key {config.api_key}",
            "Content-Type": "application/json"
        })
        
        self.rate_limiter = RateLimiter(config.max_requests_per_second)
        self.progress = ProgressTracker(config.progress_file) if config.enable_resume else None
        
        # Local user mapping for user_mode sync
        self.local_users: Dict[str, str] = {}
        
        # Thread-safe stats
        self.stats_lock = threading.Lock()
        self.stats = {
            "total": 0,
            "with_tags": 0,
            "updated": 0,
            "skipped": 0,
            "not_found": 0,
            "no_external_id": 0,
            "matched_local": 0,
            "default_mode": 0,
            "client_errors": 0,
            "errors": 0,
            "retries": 0
        }
    
    def load_local_csv(self) -> bool:
        """Load local CSV for user_mode mapping."""
        if not self.config.sync_user_mode:
            return True
        
        path = self.config.local_csv_path
        if not path:
            logger.warning("sync_user_mode enabled but no local_csv_path specified")
            return True
        
        if not os.path.exists(path):
            logger.error(f"Local CSV not found: {path}")
            return False
        
        logger.info(f"Loading local CSV: {path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                id_col = self.config.local_csv_user_id_column
                type_col = self.config.local_csv_user_type_column
                
                if id_col not in reader.fieldnames:
                    logger.error(f"Column '{id_col}' not in CSV. Found: {reader.fieldnames}")
                    return False
                if type_col not in reader.fieldnames:
                    logger.error(f"Column '{type_col}' not in CSV. Found: {reader.fieldnames}")
                    return False
                
                for row in reader:
                    uid = row.get(id_col, "").strip()
                    utype = row.get(type_col, "").strip()
                    if uid:
                        self.local_users[uid] = utype or self.config.default_user_mode
            
            logger.info(f"Loaded {len(self.local_users):,} users from local CSV")
            return True
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return False
    
    def export_subscribers(self) -> str:
        """Request CSV export from OneSignal."""
        url = f"{self.BASE_URL}/players/csv_export"
        params = {"app_id": self.config.app_id}
        payload = {
            "extra_fields": ["external_user_id", "player_id", "onesignal_id"],
            "last_active_since": "1514764800"
        }
        
        logger.info("Requesting CSV export...")
        resp = self.session.post(url, params=params, json=payload)
        resp.raise_for_status()
        
        csv_url = resp.json().get("csv_file_url")
        if not csv_url:
            raise ValueError(f"No csv_file_url in response: {resp.json()}")
        
        logger.info(f"Export URL: {csv_url}")
        return csv_url
    
    def download_csv(self, csv_url: str) -> str:
        """Download and decompress CSV with retry for 404."""
        temp_dir = self.config.temp_dir or tempfile.gettempdir()
        gz_path = os.path.join(temp_dir, "onesignal_export.csv.gz")
        csv_path = os.path.join(temp_dir, "onesignal_export.csv")
        
        logger.info(f"Waiting {self.config.csv_download_initial_wait}s for export...")
        time.sleep(self.config.csv_download_initial_wait)
        
        for attempt in range(1, self.config.csv_download_max_retries + 1):
            try:
                logger.info(f"Download attempt {attempt}/{self.config.csv_download_max_retries}")
                resp = requests.get(csv_url, stream=True, timeout=60)
                
                if resp.status_code == 404:
                    logger.warning(f"Not ready (404). Retry in {self.config.csv_download_retry_delay}s")
                    time.sleep(self.config.csv_download_retry_delay)
                    continue
                
                resp.raise_for_status()
                
                total = int(resp.headers.get('content-length', 0))
                downloaded = 0
                
                with open(gz_path, 'wb') as f:
                    for chunk in resp.iter_content(65536):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                print(f"\rDownloading: {downloaded/total*100:.1f}%", end="", flush=True)
                print()
                logger.info(f"Downloaded {downloaded/1024/1024:.1f} MB")
                break
            except requests.RequestException as e:
                if attempt < self.config.csv_download_max_retries:
                    logger.warning(f"Error: {e}. Retry in {self.config.csv_download_retry_delay}s")
                    time.sleep(self.config.csv_download_retry_delay)
                else:
                    raise RuntimeError(f"Download failed after {attempt} attempts")
        else:
            raise RuntimeError("CSV not ready after max retries")
        
        # Decompress
        logger.info("Decompressing...")
        with gzip.open(gz_path, 'rb') as f_in, open(csv_path, 'wb') as f_out:
            while chunk := f_in.read(65536):
                f_out.write(chunk)
        
        os.remove(gz_path)
        logger.info(f"CSV ready: {os.path.getsize(csv_path)/1024/1024:.1f} MB")
        return csv_path
    
    def stream_users(self, csv_path: str) -> Iterator[dict]:
        """Stream users from OneSignal CSV."""
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                self.stats["total"] += 1
                
                onesignal_id = row.get("onesignal_id")
                if not onesignal_id:
                    continue
                
                if self.progress and self.progress.is_done(onesignal_id):
                    self.stats["skipped"] += 1
                    continue
                
                tags = {}
                tags_str = row.get("tags", "")
                if tags_str and tags_str.strip() not in ["", "{}", "null"]:
                    try:
                        tags = json.loads(tags_str)
                    except:
                        pass
                
                if tags:
                    self.stats["with_tags"] += 1
                
                yield {
                    "onesignal_id": onesignal_id,
                    "external_user_id": row.get("external_user_id", "").strip() or None,
                    "tags": tags
                }
    
    def build_payload(self, user: dict) -> dict:
        """Build update payload: reset tags + set user_mode."""
        tags = user.get("tags", {})
        external_id = user.get("external_user_id")
        
        new_tags = {}
        
        # Reset tags (set to "") except preserved ones
        for key in tags.keys():
            if key not in self.config.preserve_tags:
                new_tags[key] = ""
        
        # Sync user_mode if enabled
        user_mode_reason = None
        if self.config.sync_user_mode:
            if not external_id:
                new_tags["user_mode"] = self.config.no_external_id_user_mode
                user_mode_reason = "no_external_id"
            elif external_id in self.local_users:
                new_tags["user_mode"] = self.local_users[external_id]
                user_mode_reason = "matched_local"
            else:
                new_tags["user_mode"] = self.config.default_user_mode
                user_mode_reason = "default_mode"
        
        return {
            "payload": {"properties": {"tags": new_tags}},
            "reason": user_mode_reason
        }
    
    def update_user(self, user: dict) -> tuple:
        """Update user via API. Returns (id, success, status, reason)."""
        onesignal_id = user["onesignal_id"]
        external_id = user.get("external_user_id")
        
        # Choose endpoint
        if external_id:
            url = f"{self.BASE_URL}/apps/{self.config.app_id}/users/by/external_id/{external_id}"
        else:
            url = f"{self.BASE_URL}/apps/{self.config.app_id}/users/by/onesignal_id/{onesignal_id}"
        
        data = self.build_payload(user)
        payload = data["payload"]
        reason = data["reason"]
        
        for attempt in range(self.config.api_max_retries):
            try:
                self.rate_limiter.acquire()
                resp = self.session.patch(url, json=payload, timeout=30)
                
                if resp.status_code == 404:
                    return (onesignal_id, True, "not_found", reason)
                
                if resp.status_code == 429:
                    with self.stats_lock:
                        self.stats["retries"] += 1
                    retry_after = int(resp.headers.get("Retry-After", 1))
                    time.sleep(retry_after)
                    continue
                
                if 400 <= resp.status_code < 500:
                    return (onesignal_id, True, "client_error", reason)
                
                if resp.status_code >= 500:
                    raise requests.RequestException(f"Server error {resp.status_code}")
                
                resp.raise_for_status()
                return (onesignal_id, True, "updated", reason)
                
            except requests.RequestException as e:
                with self.stats_lock:
                    self.stats["retries"] += 1
                if attempt < self.config.api_max_retries - 1:
                    time.sleep(self.config.api_retry_delay)
                else:
                    logger.error(f"Failed {onesignal_id}: {e}")
                    return (onesignal_id, False, "error", reason)
        
        return (onesignal_id, False, "error", reason)
    
    def process_users(self, users: Iterator[dict]):
        """Process users with bounded parallelism."""
        start_time = time.time()
        last_log = start_time
        
        max_concurrent = self.config.max_workers * 2
        semaphore = threading.Semaphore(max_concurrent)
        active = [0]
        active_lock = threading.Lock()
        
        def worker(user):
            try:
                oid, success, status, reason = self.update_user(user)
                
                with self.stats_lock:
                    if status == "updated":
                        self.stats["updated"] += 1
                        if reason == "no_external_id":
                            self.stats["no_external_id"] += 1
                        elif reason == "matched_local":
                            self.stats["matched_local"] += 1
                        elif reason == "default_mode":
                            self.stats["default_mode"] += 1
                    elif status == "not_found":
                        self.stats["not_found"] += 1
                    elif status == "client_error":
                        self.stats["client_errors"] += 1
                    else:
                        self.stats["errors"] += 1
                    
                    if success and self.progress:
                        self.progress.mark(oid)
            finally:
                with active_lock:
                    active[0] -= 1
                semaphore.release()
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = []
            user_iter = tqdm(users, desc="Processing", unit="user") if HAS_TQDM else users
            
            for user in user_iter:
                semaphore.acquire()
                with active_lock:
                    active[0] += 1
                
                futures.append(executor.submit(worker, user))
                
                # Log every 10s
                now = time.time()
                if now - last_log >= 10:
                    with self.stats_lock:
                        done = self.stats["updated"] + self.stats["not_found"] + \
                               self.stats["client_errors"] + self.stats["errors"]
                        elapsed = now - start_time
                        rate = done / elapsed if elapsed > 0 else 0
                        with active_lock:
                            act = active[0]
                        logger.info(
                            f"Progress: {done:,} done, {act} active | "
                            f"matched={self.stats['matched_local']:,} "
                            f"default={self.stats['default_mode']:,} "
                            f"no_ext={self.stats['no_external_id']:,} | "
                            f"{rate:.1f}/sec"
                        )
                    if self.progress:
                        self.progress.save()
                    last_log = now
            
            # Wait for remaining
            with active_lock:
                remaining = active[0]
            if remaining > 0:
                logger.info(f"Waiting for {remaining} remaining...")
            
            for f in futures:
                f.result()
        
        if self.progress:
            self.progress.save()
    
    def run(self):
        """Execute the workflow."""
        mode = "FULL RESET" if not self.config.preserve_tags else f"PRESERVE: {self.config.preserve_tags}"
        sync = "YES" if self.config.sync_user_mode else "NO"
        
        logger.info("=" * 70)
        logger.info("OneSignal Tags Manager v4")
        logger.info(f"Mode: {mode}")
        logger.info(f"Sync user_mode: {sync}")
        logger.info(f"Rate: {self.config.max_requests_per_second}/sec, Workers: {self.config.max_workers}")
        logger.info("=" * 70)
        
        csv_path = None
        start = time.time()
        
        try:
            # Load local CSV if syncing
            if not self.load_local_csv():
                return
            
            # Get OneSignal export
            csv_url = self.config.csv_url or self.export_subscribers()
            csv_path = self.download_csv(csv_url)
            
            # Process
            logger.info("Processing users...")
            self.process_users(self.stream_users(csv_path))
            
            self._print_results(time.time() - start)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️ Interrupted! Progress saved.")
            if self.progress:
                self.progress.save()
            self._print_results(time.time() - start)
        except Exception as e:
            logger.error(f"Failed: {e}")
            if self.progress:
                self.progress.save()
            raise
        finally:
            if csv_path and os.path.exists(csv_path):
                os.remove(csv_path)
    
    def _print_results(self, elapsed: float):
        rate = self.stats["updated"] / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 70)
        logger.info("RESULTS")
        logger.info("=" * 70)
        logger.info(f"  Total users:           {self.stats['total']:,}")
        logger.info(f"  With tags:             {self.stats['with_tags']:,}")
        logger.info(f"  Updated:               {self.stats['updated']:,}")
        if self.config.sync_user_mode:
            logger.info(f"    ├─ Matched local:    {self.stats['matched_local']:,}")
            logger.info(f"    ├─ Default (free):   {self.stats['default_mode']:,}")
            logger.info(f"    └─ No external_id:   {self.stats['no_external_id']:,}")
        logger.info(f"  Skipped (resumed):     {self.stats['skipped']:,}")
        logger.info(f"  Not found (404):       {self.stats['not_found']:,}")
        logger.info(f"  Client errors:         {self.stats['client_errors']:,}")
        logger.info(f"  Errors:                {self.stats['errors']:,}")
        logger.info(f"  Retries:               {self.stats['retries']:,}")
        logger.info(f"  Time:                  {elapsed:.1f}s")
        logger.info(f"  Throughput:            {rate:.1f}/sec")
        logger.info("=" * 70)


def load_config(path: str = "config.json") -> Optional[dict]:
    """Load config from JSON."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, path)
    
    if not os.path.exists(config_file):
        sample = {
            "app_id": "YOUR_APP_ID",
            "api_key": "YOUR_API_KEY",
            
            "preserve_tags": [],
            
            "sync_user_mode": False,
            "local_csv_path": "users.csv",
            "local_csv_user_id_column": "user_id",
            "local_csv_user_type_column": "user_type",
            "default_user_mode": "free",
            "no_external_id_user_mode": "unknown",
            
            "max_requests_per_second": 500,
            "max_workers": 50,
            "enable_resume": True,
            "csv_url": None
        }
        with open(config_file, 'w') as f:
            json.dump(sample, f, indent=2)
        logger.error(f"Created sample config: {config_file}")
        logger.error("Please edit and run again.")
        return None
    
    with open(config_file, 'r') as f:
        return json.load(f)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="OneSignal Tags Manager v4")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--clear-progress", action="store_true")
    args = parser.parse_args()
    
    data = load_config(args.config)
    if not data:
        return
    
    if data.get("api_key", "").startswith("YOUR_"):
        logger.error("Update 'api_key' in config.json")
        return
    if data.get("app_id", "").startswith("YOUR_"):
        logger.error("Update 'app_id' in config.json")
        return
    
    rate = min(data.get("max_requests_per_second", 500), 500)
    
    config = Config(
        app_id=data["app_id"],
        api_key=data["api_key"],
        preserve_tags=data.get("preserve_tags", []),
        sync_user_mode=data.get("sync_user_mode", False),
        local_csv_path=data.get("local_csv_path", ""),
        local_csv_user_id_column=data.get("local_csv_user_id_column", "user_id"),
        local_csv_user_type_column=data.get("local_csv_user_type_column", "user_type"),
        default_user_mode=data.get("default_user_mode", "free"),
        no_external_id_user_mode=data.get("no_external_id_user_mode", "unknown"),
        max_requests_per_second=rate,
        max_workers=data.get("max_workers", 50),
        enable_resume=data.get("enable_resume", True),
        csv_url=data.get("csv_url")
    )
    
    manager = OneSignalTagsManager(config)
    
    if args.clear_progress and manager.progress:
        manager.progress.clear()
        logger.info("Progress cleared")
    
    manager.run()


if __name__ == "__main__":
    main()
