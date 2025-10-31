import asyncio
import logging
import os
import sys
from pathlib import Path
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
import re
import time
from urllib.parse import urlparse, parse_qs
from mictlanx.services.router import AsyncRouter

# --- Configuration ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- Helper Functions ---
def sanitize_key(key: str) -> str:
    """
    Removes all non-alphanumeric characters from a string.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', key)

# Paths
WATCH_DIRECTORY = os.getenv("WATCH_DIRECTORY", "/home/alo03/Estadia/Nez-daemon/services/deployer/app/results")
QUARANTINE_DIRECTORY = os.getenv("QUARANTINE_DIRECTORY", "/home/alo03/Estadia/Nez-daemon/services/deployer/app/quarantine")

# MictlanX Configuration
BUCKET_ID = os.getenv("BUCKET_ID", "nez-bucket")
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
MICTLANX_URI = os.getenv("MICTLANX_URI", "")
MICTLANX_ROUTER: AsyncRouter = None  # Will be parsed from MICTLANX_URI

# Worker Configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "500"))
FILE_STABILITY_TIMEOUT = float(os.getenv("FILE_STABILITY_TIMEOUT", "2.0"))

# --- Logging Setup ---
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- URI Parser ---
def parse_mictlanx_uri(uri: str) -> AsyncRouter:
    """
    Parses the MictlanX URI and returns an AsyncRouter instance.
    """
    if not uri:
        raise ValueError("MICTLANX_URI environment variable is not set")
    
    try:
        parsed_uri = urlparse(uri)
        protocol = parse_qs(parsed_uri.query).get("protocol", ["http"])[0]
        router_id = parsed_uri.username
        ip_addr = parsed_uri.hostname
        port = parsed_uri.port
        
        router = AsyncRouter(router_id=router_id, ip_addr=ip_addr, port=port, protocol=protocol)
        logger.info(f"Parsed MictlanX Router: {router}")
        return router
    
    except Exception as e:
        raise ValueError(f"Failed to parse MICTLANX_URI '{uri}': {e}")


# --- File Stability Check ---
async def wait_for_file_stability(file_path: Path, timeout: float = FILE_STABILITY_TIMEOUT):
    """
    Waits for a file to stop changing (no size/mtime changes).
    """
    try:
        last_size = file_path.stat().st_size
        last_mtime = file_path.stat().st_mtime
        
        await asyncio.sleep(timeout)
        
        current_size = file_path.stat().st_size
        current_mtime = file_path.stat().st_mtime
        
        if current_size != last_size or current_mtime != last_mtime:
            logger.debug(f"File {file_path.name} still changing, waiting longer...")
            await wait_for_file_stability(file_path, timeout)
        else:
            logger.debug(f"File {file_path.name} is stable and ready for processing.")
    
    except FileNotFoundError:
        logger.warning(f"File {file_path.name} disappeared during stability check.")
        raise


# --- Health Check ---
# @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)) # Temporarily remove retry
async def health_check():
    """Checks if the mictlanx-service is available before starting."""
    try:
        result = await MICTLANX_ROUTER.get_bucket_metadata(bucket_id=BUCKET_ID)
        if result.is_err:
            error_val = result.err()
            logger.error(f"DEBUG: Type of error_val: {type(error_val)}, Value of error_val: {error_val}")
            if isinstance(error_val, Exception):
                raise error_val
            else:
                raise RuntimeError(f"MictlanX health check failed with unexpected error type: {error_val}")
        logger.info("✓ MictlanX service is healthy and available.")
        return True
    except Exception as e:
        logger.error(f"Health check failed: {e}. Retrying...")
        raise


# --- MictlanX File Operations ---
async def download_file_from_mictlanx(file_path: Path):
    """Downloads a file from MictlanX based on a .mictlanx_download file."""
    try:
        logger.info(f"Processing download request from file: {file_path.name}")
        with open(file_path, 'r') as f:
            key_to_download = f.read().strip()

        if not key_to_download:
            logger.error(f"The download file {file_path.name} is empty.")
            return

        logger.info(f"Attempting to download file with key: {key_to_download}")
        
        # Define a downloads directory to prevent re-uploading downloaded files
        download_dir = Path(WATCH_DIRECTORY).parent / "downloads"
        download_dir.mkdir(exist_ok=True)

        result = await MICTLANX_ROUTER.get_to_file(
            bucket_id=BUCKET_ID,
            key=key_to_download,
            sink_folder_path=str(download_dir),
            filename=key_to_download # Save with the original key as the filename
        )

        if result.is_ok:
            downloaded_file_path = result.ok()
            logger.info(f"Successfully downloaded file to '{downloaded_file_path}'")
            # Clean up the .mictlanx_download file
            file_path.unlink()
        else:
            logger.error(f"Failed to download file with key '{key_to_download}': {result.err()}")

    except Exception as e:
        logger.error(f"An error occurred during the download process: {e}")
        move_to_quarantine(file_path)

async def check_file_existence(client: httpx.AsyncClient, bucket_id: str, file_name: str) -> bool:
    """
    Checks if a file with the given name already exists in the MictlanX bucket.
    """
    try:
        response = await client.get(
            f"{MICTLANX_ROUTER.base_url()}/api/v4/buckets/{bucket_id}/metadata/{file_name}",
            timeout=10.0
        )
        response.raise_for_status()
        logger.info(f"File '{file_name}' already exists in MictlanX.")
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            logger.debug(f"File '{file_name}' does not exist in MictlanX (404 Not Found).")
            return False
        elif e.response.status_code == 500:
            try:
                response_detail = e.response.json().get("detail", "")
                if "404: No available peers" in response_detail:
                    logger.debug(f"File '{file_name}' does not exist in MictlanX (500 with 'No available peers' detail).")
                    return False
                else:
                    logger.error(f"Error checking existence of '{file_name}': HTTP {e.response.status_code} - {e.response.text}")
                    raise
            except ValueError: # Not a valid JSON
                logger.error(f"Error checking existence of '{file_name}': HTTP {e.response.status_code} - {e.response.text}")
                raise
        else:
            logger.error(f"Error checking existence of '{file_name}': HTTP {e.response.status_code} - {e.response.text}")
            raise
    except httpx.RequestError as e:
        logger.error(f"Network error checking existence of '{file_name}': {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
async def upload_file(client: httpx.AsyncClient, file_path: Path):
    """Orchestrates the two-step file upload process."""
    file_name = file_path.name
    file_size = file_path.stat().st_size
    
    # Validations
    if file_size == 0:
        logger.warning(f"Skipping empty file: {file_name}")
        return
    
    max_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    if file_size > max_size_bytes:
        logger.error(f"File {file_name} exceeds maximum size ({MAX_FILE_SIZE_MB}MB). Skipping.")
        raise ValueError(f"File too large: {file_size} bytes")
    
    logger.info(f"Starting upload for {file_name} ({file_size} bytes)")
    
    try:
        # Calculate checksum
        logger.debug(f"Calculating SHA256 checksum for {file_name}")
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        checksum = sha256_hash.hexdigest()
        logger.debug(f"Checksum for {file_name}: {checksum}")
        
        # Generate ball_id (using timestamp + filename hash as fallback if nanoid fails)
        try:
            import nanoid
            ball_id = nanoid.generate()
        except ImportError:
            logger.warning("nanoid not available, using alternative ID generation")
            ball_id = f"{int(time.time())}-{hashlib.md5(file_name.encode()).hexdigest()[:8]}"
        
        # Step 1: Register metadata
        sanitized_file_name = sanitize_key(file_name) # Sanitize the file name
        logger.debug(f"Step 1: Registering metadata for {file_name} (sanitized to: {sanitized_file_name})")
        metadata_payload = {
            "bucket_id": BUCKET_ID,
            "key": sanitized_file_name, # Use the sanitized file name as key
            "ball_id": ball_id,
            "checksum": checksum,
            "size": file_size,
            "producer_id": "nez-watcher",
            "replication_factor": REPLICATION_FACTOR,
            "tags": {"source": "watcher", "timestamp": str(time.time())},
        }
        
        response_meta = await client.post(
            f"{MICTLANX_ROUTER.base_url()}/api/v4/buckets/{BUCKET_ID}/metadata",
            json=metadata_payload,
            timeout=30.0
        )
        response_meta.raise_for_status()
        task_info = response_meta.json()
        
        # Extract task_id from response (using tasks_ids)
        tasks_ids = task_info.get("tasks_ids")
        if not tasks_ids or not isinstance(tasks_ids, list) or not tasks_ids[0]:
            logger.error(f"Response from metadata endpoint: {task_info}")
            raise ValueError("Could not get task_id from 'tasks_ids' in metadata response")
        task_id = tasks_ids[0]
        
        logger.info(f"✓ Metadata registered. Task ID: {task_id}")
        
        # Step 2: Upload file data
        logger.debug(f"Step 2: Uploading file data for task {task_id}")
        with open(file_path, "rb") as f:
            files = {"data": (file_name, f, "application/octet-stream")}
            response_data = await client.post(
                f"{MICTLANX_ROUTER.base_url()}/api/v4/buckets/data/{task_id}",
                files=files,
                timeout=300.0  # 5 minutes for large files
            )
            response_data.raise_for_status()
        
        logger.info(f"✓ Upload complete for {file_name}")
        return file_name
    
    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP {e.response.status_code} error uploading {file_name}: {e.response.text}"
        )
        raise
    except httpx.RequestError as e:
        logger.error(f"Network error uploading {file_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error uploading {file_name}: {e}")
        raise


# --- Quarantine Logic ---
def move_to_quarantine(file_path: Path):
    """Moves a file to the quarantine directory after repeated failures."""
    try:
        quarantine_path = Path(QUARANTINE_DIRECTORY)
        quarantine_path.mkdir(parents=True, exist_ok=True)
        
        destination = quarantine_path / file_path.name
        # Handle name collisions
        counter = 1
        while destination.exists():
            destination = quarantine_path / f"{file_path.stem}_{counter}{file_path.suffix}"
            counter += 1
        
        file_path.rename(destination)
        logger.warning(f"Moved failed file to quarantine: {destination}")
    except Exception as e:
        logger.error(f"Failed to move {file_path.name} to quarantine: {e}")


# --- Worker ---
async def worker(name: str, queue: asyncio.Queue):
    """Processes files from the queue."""
    async with httpx.AsyncClient() as client:
        while True:
            file_path = await queue.get()
            logger.info(f"[{name}] Processing: {file_path.name}")
            
            try:
                if file_path.suffix == '.mictlanx_download':
                    await download_file_from_mictlanx(file_path)
                else:
                    # Wait for file to be completely written
                    await wait_for_file_stability(file_path)

                    # Check if file already exists in MictlanX
                    file_name = file_path.name
                    if await check_file_existence(client, BUCKET_ID, file_name):
                        logger.info(f"[{name}] File '{file_name}' already exists in MictlanX. Skipping upload.")
                        continue # Skip to the next item in the queue

                    # Upload the file
                    await upload_file(client, file_path)
                    logger.info(f"[{name}] ✓ Successfully processed {file_path.name}")
                
                # Optional: Delete or move the file after successful upload
                # file_path.unlink()  # Uncomment to delete after upload
            
            except FileNotFoundError:
                logger.warning(f"[{name}] File disappeared: {file_path.name}")
            
            except RetryError:
                logger.error(f"[{name}] ✗ Failed after all retries: {file_path.name}")
                move_to_quarantine(file_path)
            
            except Exception as e:
                logger.error(f"[{name}] ✗ Unexpected error processing {file_path.name}: {e}")
                move_to_quarantine(file_path)
            
            finally:
                queue.task_done()


# --- Watchdog Event Handler ---
class NewFileHandler(FileSystemEventHandler):
    """Handles file system events and adds new files to the processing queue."""
    
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.queue = queue
        self.loop = loop
        super().__init__()
    
    def on_created(self, event):
        if not event.is_directory:
            file_path = Path(event.src_path)
            logger.info(f"📁 New file detected: {file_path.name}")
            
            # Add to queue asynchronously
            asyncio.run_coroutine_threadsafe(
                self.queue.put(file_path),
                self.loop
            )


# --- Main Execution ---
async def main():
    """Main function to set up and run the watcher."""
    global WATCH_DIRECTORY, MICTLANX_ROUTER
    
    logger.info("=" * 60)
    logger.info("    NEZ-DAEMON WATCHER - MictlanX Integration")
    logger.info("=" * 60)
    
    # Parse command line argument for watch directory (optional override)
    if len(sys.argv) > 1:
        WATCH_DIRECTORY = sys.argv[1]
    
    # Validate watch directory
    if not WATCH_DIRECTORY:
        logger.error("WATCH_DIRECTORY is not set. Please provide it via env var or argument.")
        sys.exit(1)
    
    # Parse MictlanX URI
    try:
        MICTLANX_ROUTER = parse_mictlanx_uri(MICTLANX_URI)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Log configuration
    logger.info(f"Watch Directory: {WATCH_DIRECTORY}")
    logger.info(f"Quarantine Directory: {QUARANTINE_DIRECTORY}")
    logger.info(f"Target Bucket: {BUCKET_ID}")
    logger.info(f"Replication Factor: {REPLICATION_FACTOR}")
    logger.info(f"Max Workers: {MAX_WORKERS}")
    logger.info(f"Max File Size: {MAX_FILE_SIZE_MB}MB")
    logger.info(f"MictlanX Router: {MICTLANX_ROUTER}")
    logger.info("=" * 60)
    
    # Health check
    try:
        await health_check()
    except Exception:
        logger.critical("✗ MictlanX service is unavailable. Shutting down.")
        sys.exit(1)
    
    # Ensure directories exist
    Path(WATCH_DIRECTORY).mkdir(parents=True, exist_ok=True)
    Path(QUARANTINE_DIRECTORY).mkdir(parents=True, exist_ok=True)
    
    # Create processing queue
    file_queue = asyncio.Queue()
    
    # Get the current running event loop
    current_loop = asyncio.get_running_loop()
    
    # Start worker tasks
    tasks = []
    for i in range(MAX_WORKERS):
        task = asyncio.create_task(worker(f"Worker-{i+1}", file_queue))
        tasks.append(task)
    logger.info(f"✓ Started {MAX_WORKERS} worker tasks")
    
    # Start watchdog observer
    event_handler = NewFileHandler(file_queue, current_loop)
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=True)
    observer.start()
    logger.info("✓ Watchdog observer started")
    logger.info("🔍 Now monitoring for new files...")
    
    try:
        # Keep the main loop alive
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("\n⚠ Shutdown signal received...")
    
    finally:
        # Graceful shutdown
        logger.info("Stopping watchdog observer...")
        observer.stop()
        observer.join()
        
        logger.info("Waiting for queue to empty...")
        await file_queue.join()
        
        logger.info("Cancelling worker tasks...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("✓ Watcher shut down gracefully")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)