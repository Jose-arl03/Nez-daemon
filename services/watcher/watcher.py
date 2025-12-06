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
    handle_download_file_request,
    process_download,
    health_check,
    sanitize_key,
    upload_file,
)
from src.socket_server import start_socket_server

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
    """Processes tasks from the queue."""
    async with httpx.AsyncClient() as client:
        while True:
            task_type, payload = await queue.get()
            
            try:
                if task_type == 'filesystem_event':
                    file_path = payload
                    if file_path.suffix == '.mictlanx_download':
                        await handle_download_file_request(router, file_path)
                    else:
                        relative_path = str(file_path.relative_to(Path(config.WATCH_DIRECTORY)))
                        logger.info(f"[{name}] Processing file upload: {relative_path}")
                        await wait_for_file_stability(file_path)
                        sanitized_key = sanitize_key(relative_path)
                        if await check_file_existence(client, router, config.BUCKET_ID, sanitized_key):
                            logger.info(f"[{name}] File '{relative_path}' (key: '{sanitized_key}') already exists. Skipping.")
                            continue
                        await upload_file(client, router, file_path)
                        logger.info(f"[{name}] ✓ Successfully processed {relative_path}")
                
                elif task_type == 'socket_download_request':
                    request_path = payload
                    logger.info(f"[{name}] Processing socket download request for: {request_path}")
                    await process_download(router, request_path)

            except FileNotFoundError:
                logger.warning(f"[{name}] File disappeared during processing: {payload}")
            except RetryError:
                logger.error(f"[{name}] ✗ Failed after all retries for: {payload}")
                move_to_quarantine(payload)
            except Exception as e:
                logger.error(f"[{name}] ✗ Unexpected error processing {payload}: {e}")
                move_to_quarantine(payload)
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
    
    # --- Log configuration ---
    # ... (omitted for brevity, same as before)

    try:
        await health_check(router)
    except Exception:
        logger.critical("✗ MictlanX service is unavailable. Shutting down.")
        sys.exit(1)
    
    Path(config.WATCH_DIRECTORY).mkdir(parents=True, exist_ok=True)
    Path(config.QUARANTINE_DIRECTORY).mkdir(parents=True, exist_ok=True)
    
    file_queue = asyncio.Queue()
    current_loop = asyncio.get_running_loop()
    
    # Start worker tasks
    worker_tasks = []
    for i in range(config.MAX_WORKERS):
        task = asyncio.create_task(worker(f"Worker-{i+1}", file_queue, router))
        worker_tasks.append(task)
    logger.info(f"✓ Started {config.MAX_WORKERS} worker tasks")
    
    # Start filesystem observer
    event_handler = NewFileHandler(file_queue, current_loop)
    observer = Observer()
    observer.schedule(event_handler, config.WATCH_DIRECTORY, recursive=True)
    observer.start()
    logger.info("✓ Watchdog observer started")

    # Start socket server
    socket_server_task = asyncio.create_task(start_socket_server(file_queue, router))
    logger.info("✓ Socket server started")
    
    logger.info("🔍 Now monitoring for new files and socket requests...")
    
    all_tasks = worker_tasks + [socket_server_task]

    try:
        # Keep the main loop alive by waiting on the tasks
        await asyncio.gather(*all_tasks)
    except KeyboardInterrupt:
        logger.info("\n⚠ Shutdown signal received...")
    finally:
        logger.info("Stopping watchdog observer...")
        observer.stop()
        observer.join()
        
        logger.info("Waiting for queue to empty...")
        await file_queue.join()
        
        logger.info("Cancelling all tasks...")
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        
        logger.info("✓ Watcher shut down gracefully")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
