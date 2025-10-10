#!/usr/bin/env python3
import asyncio
import os
import sys
import time
import hashlib
import sqlite3
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# MictlanX Client
from mictlanx import AsyncClient


DATABASE_NAME = 'file_events.db'

def get_file_hash(filepath):
    """Calculates the SHA-256 hash of a file's contents."""
    if not os.path.exists(filepath) or os.path.isdir(filepath):
        return None
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except (IOError, OSError) as e:
        logging.error(f"Could not hash {filepath}: {e}")
        return None

def create_db_table():
    """Ensures the file_events table exists in the SQLite database."""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                src_path TEXT NOT NULL,
                dest_path TEXT,
                is_directory INTEGER NOT NULL,
                file_hash TEXT,
                mictlanx_status TEXT DEFAULT 'PENDING' -- PENDING, UPLOADED, FAILED
            )
        ''')
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"Could not create table: {e}")
        sys.exit(1)

class WatchdogProducer(FileSystemEventHandler):
    """FileSystemEventHandler that produces file paths for uploading."""
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.queue = queue
        self.loop = loop

    def on_created(self, event):
        if not event.is_directory:
            logging.info(f"Detected new file: {event.src_path}")
            # Schedule the queue insertion on the main event loop in a thread-safe way
            self.loop.call_soon_threadsafe(self.queue.put_nowait, event.src_path)

    def on_modified(self, event):
        # This could also trigger an upload if you want to handle updates
        pass

QUARANTINE_DIR = "_failed_uploads"

async def mictlanx_consumer(queue: asyncio.Queue, client: AsyncClient, path_to_watch: str):
    """Consumes file paths from the queue and uploads them to MictlanX."""
    logging.info("Worker started, waiting for files...")

    # Ensure quarantine directory exists
    quarantine_path = os.path.join(path_to_watch, QUARANTINE_DIR)
    os.makedirs(quarantine_path, exist_ok=True)

    while True:
        try:
            # Wait for a new file path from the queue
            filepath = await queue.get()
            logging.info(f"Got file from queue: {filepath}")

            if not os.path.exists(filepath):
                logging.warning(f"File no longer exists, skipping: {filepath}")
                continue

            # --- MictlanX Upload Logic with Retries ---
            bucket_id = "nez-bucket"
            ball_id = os.path.basename(filepath)
            max_retries = 3
            base_delay = 2  # seconds
            upload_success = False

            for attempt in range(1, max_retries + 1):
                logging.info(f"Uploading {filepath} to MictlanX (Attempt {attempt}/{max_retries})...")
                try:
                    # Read the file content as bytes
                    with open(filepath, 'rb') as f:
                        content = f.read()

                    if not content:
                        logging.warning(f"File {filepath} is empty, skipping upload.")
                        upload_success = True # Mark as success to avoid quarantine
                        break

                    result = await client.put(
                        bucket_id=bucket_id,
                        key=ball_id,
                        value=content,
                        tags={"source_path": filepath}
                    )

                    if result.is_ok:
                        logging.info(f"Successfully uploaded {filepath} to MictlanX.")
                        upload_success = True
                        # After a successful upload, we can delete the original file
                        try:
                            os.remove(filepath)
                            logging.info(f"Removed original file: {filepath}")
                        except OSError as e:
                            logging.exception(f"Failed to remove original file {filepath}: {e}")
                        break  # Exit retry loop on success
                    else:
                        logging.error(f"Failed to upload {filepath} on attempt {attempt}. Reason: {result.unwrap_err()}")

                except FileNotFoundError:
                    logging.warning(f"File {filepath} was not found for upload (it may have been deleted after a previous failed attempt). Aborting retries.")
                    upload_success = True # Treat as handled, no need to quarantine
                    break
                except Exception as e:
                    logging.exception(f"An exception occurred during MictlanX upload for {filepath} on attempt {attempt}: {e}")
                
                # If not the last attempt, wait before retrying
                if attempt < max_retries:
                    delay = base_delay ** attempt
                    logging.info(f"Waiting {delay} seconds before next retry...")
                    await asyncio.sleep(delay)

            # --- Quarantine Logic ---
            if not upload_success:
                logging.error(f"All {max_retries} attempts to upload {filepath} failed. The file might have been deleted by the client.")
                # The file is likely gone at this point, so we just log the failure.
                # If we wanted to be safer, we would copy the file before attempting to upload.
            
            # Mark the task as done
            queue.task_done()

        except asyncio.CancelledError:
            logging.info("Worker cancelled.")
            break
        except Exception as e:
            logging.exception(f"An unexpected error occurred in the consumer: {e}")

async def check_mictlanx_health(client: AsyncClient) -> bool:
    """Performs a health check against the MictlanX service."""
    logging.info("Performing MictlanX health check...")
    for attempt in range(1, 4): # Try 3 times
        try:
            # We try to get metadata for a bucket. Even if it doesn't exist,
            # a specific error is better than a connection timeout.
            result = await client.get_bucket_metadata(bucket_id="nez-bucket")
            if result.is_ok:
                logging.info("MictlanX health check successful. Service is responsive.")
                return True
            else:
                # The service is responsive but returned an error (e.g., bucket not found)
                # This is still a sign of a healthy service.
                logging.warning(f"MictlanX health check: Service is responsive but returned an error: {result.unwrap_err()}")
                return True
        except Exception as e:
            logging.error(f"MictlanX health check failed on attempt {attempt}/3. Error: {e}")
            if attempt < 3:
                await asyncio.sleep(5) # Wait 5 seconds before retrying
    return False

async def main(path_to_watch: str, mictlanx_uri: str):
    """Main function to set up and run the watcher and consumers."""
    # Create a queue for communication between watchdog (producer) and uploader (consumer)
    upload_queue = asyncio.Queue()

    # Initialize the MictlanX client once
    client = AsyncClient(uri=mictlanx_uri, client_id="nez-watcher-daemon")
    logging.info("MictlanX client initialized.")

    # --- Perform Health Check ---
    if not await check_mictlanx_health(client):
        logging.critical("MictlanX service is unreachable. Shutting down.")
        return
    # --------------------------

    # Create consumer tasks that will upload files in parallel
    # You can increase the number of consumers for more parallelism
    num_consumers = 2
    consumers = [
        asyncio.create_task(mictlanx_consumer(upload_queue, client, path_to_watch))
        for _ in range(num_consumers)
    ]
    logging.info(f"{num_consumers} MictlanX consumer workers started.")

    # Get the current event loop for the producer to schedule tasks on
    loop = asyncio.get_running_loop()

    # Set up and start the watchdog observer in a separate thread
    event_handler = WatchdogProducer(upload_queue, loop)
    observer = Observer()
    observer.schedule(event_handler, path_to_watch, recursive=True)
    observer.start()  # This starts a new thread
    logging.info(f"Watchdog observer started for directory: {path_to_watch}")

    try:
        # Keep the main async loop running
        await asyncio.gather(*consumers)
    except KeyboardInterrupt:
        logging.info("Shutdown signal received.")
    finally:
        logging.info("Shutting down...")
        observer.stop()
        observer.join() # Wait for the observer thread to finish
        # Cancel the consumer tasks
        for c in consumers:
            c.cancel()
        # Wait for consumers to finish cancelling
        await asyncio.gather(*consumers, return_exceptions=True)
        logging.info("Shutdown complete.")

if __name__ == "__main__":
    # --- Setup Structured Logging ---
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
        stream=sys.stdout,
    )
    # --------------------------------

    if len(sys.argv) > 1:
        path_to_watch = sys.argv[1]
    else:
        logging.error("Please provide the directory path to watch as the first argument.")
        sys.exit(1)

    # Get MictlanX URI from environment variable, with a default for local testing
    mictlanx_uri = os.getenv("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:60666?/api_version=4&protocol=http")

    if not os.path.isdir(path_to_watch):
        logging.error(f"The provided path '{path_to_watch}' is not a valid directory.")
        sys.exit(1)

    # create_db_table() # We can re-enable this later

    try:
        asyncio.run(main(path_to_watch, mictlanx_uri))
    except Exception as e:
        logging.exception(f"An unexpected error occurred in the main execution: {e}")