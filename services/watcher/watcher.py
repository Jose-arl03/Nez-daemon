import asyncio
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import httpx
from mictlanx.services.router import AsyncRouter
from tenacity import RetryError
from watchdog.observers import Observer

# Import refactored modules
from src import config
from src.file_ops import NewFileHandler, move_to_quarantine, wait_for_file_stability
from src.mictlanx_ops import (
    check_file_existence,
    download_file_from_mictlanx,
    health_check,
    sanitize_key,
    upload_file,
)

# --- Logging Setup ---
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- URI Parser ---
def parse_mictlanx_uri(uri: str) -> AsyncRouter:
    """Parses the MictlanX URI and returns an AsyncRouter instance."""
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


# --- Worker ---
async def worker(name: str, queue: asyncio.Queue, router: AsyncRouter):
    """Processes files from the queue."""
    async with httpx.AsyncClient() as client:
        while True:
            file_path = await queue.get()
            logger.info(f"[{name}] Processing: {file_path.name}")
            
            try:
                if file_path.suffix == '.mictlanx_download':
                    logger.info(f"[{name}] Processing download request: {file_path.name}")
                    await download_file_from_mictlanx(router, file_path)
                else:
                    relative_path = str(file_path.relative_to(Path(config.WATCH_DIRECTORY)))
                    logger.info(f"[{name}] Processing file: {relative_path}")
                    
                    await wait_for_file_stability(file_path)

                    sanitized_key = sanitize_key(relative_path)

                    if await check_file_existence(client, router, config.BUCKET_ID, sanitized_key):
                        logger.info(f"[{name}] File '{relative_path}' (key: '{sanitized_key}') already exists. Skipping.")
                        continue

                    await upload_file(client, router, file_path)
                    logger.info(f"[{name}] ✓ Successfully processed {relative_path}")
            
            except FileNotFoundError:
                logger.warning(f"[{name}] File disappeared during processing: {file_path.name}")
            except RetryError:
                logger.error(f"[{name}] ✗ Failed after all retries for: {file_path.name}")
                move_to_quarantine(file_path)
            except Exception as e:
                logger.error(f"[{name}] ✗ Unexpected error processing {file_path.name}: {e}")
                move_to_quarantine(file_path)
            finally:
                queue.task_done()


# --- Main Execution ---
async def main():
    """Main function to set up and run the watcher."""
    logger.info("=" * 60)
    logger.info("    NEZ-DAEMON WATCHER - MictlanX Integration")
    logger.info("=" * 60)
    
    if len(sys.argv) > 1:
        config.WATCH_DIRECTORY = sys.argv[1]
    
    if not config.WATCH_DIRECTORY:
        logger.error("WATCH_DIRECTORY is not set. Please provide it via env var or argument.")
        sys.exit(1)
    
    try:
        router = parse_mictlanx_uri(config.MICTLANX_URI)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    logger.info(f"Watch Directory: {config.WATCH_DIRECTORY}")
    logger.info(f"Quarantine Directory: {config.QUARANTINE_DIRECTORY}")
    logger.info(f"Target Bucket: {config.BUCKET_ID}")
    logger.info(f"Replication Factor: {config.REPLICATION_FACTOR}")
    logger.info(f"Max Workers: {config.MAX_WORKERS}")
    logger.info(f"Max File Size: {config.MAX_FILE_SIZE_MB}MB")
    logger.info(f"MictlanX Router: {router}")
    logger.info("=" * 60)
    
    try:
        await health_check(router)
    except Exception:
        logger.critical("✗ MictlanX service is unavailable. Shutting down.")
        sys.exit(1)
    
    Path(config.WATCH_DIRECTORY).mkdir(parents=True, exist_ok=True)
    Path(config.QUARANTINE_DIRECTORY).mkdir(parents=True, exist_ok=True)
    
    file_queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()
    
    tasks = []
    for i in range(config.MAX_WORKERS):
        task = asyncio.create_task(worker(f"Worker-{i+1}", file_queue, router))
        tasks.append(task)
    logger.info(f"✓ Started {config.MAX_WORKERS} worker tasks")
    
    event_handler = NewFileHandler(file_queue, current_loop)
    observer = Observer()
    observer.schedule(event_handler, config.WATCH_DIRECTORY, recursive=True)
    observer.start()
    logger.info("✓ Watchdog observer started")
    logger.info("🔍 Now monitoring for new files...")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n⚠ Shutdown signal received...")
    finally:
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