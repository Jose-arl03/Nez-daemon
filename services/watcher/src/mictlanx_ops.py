import logging
import hashlib
import re
import time
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from mictlanx.services.router import AsyncRouter

from .config import (
    BUCKET_ID,
    MAX_FILE_SIZE_MB,
    REPLICATION_FACTOR,
    WATCH_DIRECTORY,
)
from .file_ops import move_to_quarantine

logger = logging.getLogger(__name__)


def sanitize_key(key: str) -> str:
    """Removes all non-alphanumeric characters from a string."""
    return re.sub(r'[^a-zA-Z0-9]', '', key)


async def health_check(router: AsyncRouter):
    """Checks if the mictlanx-service is available before starting."""
    try:
        result = await router.get_bucket_metadata(bucket_id=BUCKET_ID)
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


async def download_file_from_mictlanx(router: AsyncRouter, file_path: Path):
    """Downloads a file or folder from MictlanX by searching metadata tags."""
    try:
        logger.info(f"Processing download request from file: {file_path.name}")
        with open(file_path, 'r') as f:
            request_path = f.read().strip()

        if not request_path:
            logger.error(f"The download file {file_path.name} is empty.")
            return

        download_dir = Path(WATCH_DIRECTORY).parent / "downloads"
        download_dir.mkdir(exist_ok=True)

        if request_path.endswith('/'):
            logger.info(f"Folder download requested for: {request_path}")
            all_metadata_result = await router.get_bucket_metadata(bucket_id=BUCKET_ID)
            if all_metadata_result.is_err:
                logger.error(f"Could not retrieve bucket metadata for folder download: {all_metadata_result.err()}")
                return

            files_to_download = []
            try:
                all_metadata = all_metadata_result.ok().unwrap().balls
                for meta in all_metadata:
                    original_path = meta.tags.get("path")
                    if original_path and original_path.startswith(request_path):
                        files_to_download.append({"key": meta.key, "path": original_path})
            except Exception as e:
                logger.error(f"Could not parse bucket metadata response. Error: {e}")
                return

            if not files_to_download:
                logger.warning(f"No files found in MictlanX with path prefix: {request_path}")
                return

            logger.info(f"Found {len(files_to_download)} files to download for folder {request_path}")
            for file_info in files_to_download:
                local_file_path = download_dir / file_info["path"]
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"  -> Downloading {file_info['key']} to {local_file_path}")
                dl_result = await router.get_to_file(
                    bucket_id=BUCKET_ID,
                    key=file_info["key"],
                    sink_folder_path=str(local_file_path.parent),
                    filename=local_file_path.name,
                )
                if dl_result.is_err:
                    logger.error(f"  -> Failed to download {file_info['key']}: {dl_result.err()}")
            logger.info(f"✓ Folder download complete for {request_path}")
        else:
            logger.info(f"Single file download requested for: {request_path}")
            mictlanx_key = sanitize_key(request_path)
            local_file_path = download_dir / request_path
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            result = await router.get_to_file(
                bucket_id=BUCKET_ID,
                key=mictlanx_key,
                sink_folder_path=str(local_file_path.parent),
                filename=local_file_path.name,
            )
            if result.is_ok:
                downloaded_file_path = result.ok()
                logger.info(f"✓ Successfully downloaded file to '{downloaded_file_path}'")
            else:
                logger.error(f"Failed to download file with key '{mictlanx_key}': {result.err()}")
    except Exception as e:
        logger.error(f"An error occurred during the download process: {e}")
        move_to_quarantine(file_path)
    finally:
        if file_path.exists():
            file_path.unlink()


async def check_file_existence(client: httpx.AsyncClient, router: AsyncRouter, bucket_id: str, file_name: str) -> bool:
    """Checks if a file with the given name already exists in the MictlanX bucket."""
    try:
        response = await client.get(
            f"{router.base_url()}/api/v4/buckets/{bucket_id}/metadata/{file_name}",
            timeout=10.0,
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
            except ValueError:
                logger.error(f"Error checking existence of '{file_name}': HTTP {e.response.status_code} - {e.response.text}")
                raise
        else:
            logger.error(f"Error checking existence of '{file_name}': HTTP {e.response.status_code} - {e.response.text}")
            raise
    except httpx.RequestError as e:
        logger.error(f"Network error checking existence of '{file_name}': {e}")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
async def upload_file(client: httpx.AsyncClient, router: AsyncRouter, file_path: Path):
    """Orchestrates the two-step file upload process."""
    file_size = file_path.stat().st_size
    relative_path = str(file_path.relative_to(Path(WATCH_DIRECTORY)))

    if file_size == 0:
        logger.warning(f"Skipping empty file: {relative_path}")
        return

    max_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    if file_size > max_size_bytes:
        logger.error(f"File {relative_path} exceeds maximum size ({MAX_FILE_SIZE_MB}MB). Skipping.")
        raise ValueError(f"File too large: {file_size} bytes")

    logger.info(f"Starting upload for {relative_path} ({file_size} bytes)")

    try:
        logger.debug(f"Calculating SHA256 checksum for {relative_path}")
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        checksum = sha256_hash.hexdigest()
        logger.debug(f"Checksum for {relative_path}: {checksum}")

        try:
            import nanoid
            ball_id = nanoid.generate()
        except ImportError:
            logger.warning("nanoid not available, using alternative ID generation")
            ball_id = f"{int(time.time())}-{hashlib.md5(file_path.name.encode()).hexdigest()[:8]}"

        sanitized_key = sanitize_key(relative_path)

        logger.debug(f"Step 1: Registering metadata for {relative_path}. Sanitized Key: '{sanitized_key}'")

        tags = {"source": "watcher", "timestamp": str(time.time()), "path": relative_path}

        metadata_payload = {
            "bucket_id": BUCKET_ID,
            "key": sanitized_key,
            "ball_id": ball_id,
            "checksum": checksum,
            "size": file_size,
            "producer_id": "nez-watcher",
            "replication_factor": REPLICATION_FACTOR,
            "tags": tags,
        }

        response_meta = await client.post(
            f"{router.base_url()}/api/v4/buckets/{BUCKET_ID}/metadata",
            json=metadata_payload,
            timeout=30.0,
        )
        response_meta.raise_for_status()
        task_info = response_meta.json()

        tasks_ids = task_info.get("tasks_ids")
        if not tasks_ids or not isinstance(tasks_ids, list) or not tasks_ids[0]:
            logger.error(f"Response from metadata endpoint: {task_info}")
            raise ValueError("Could not get task_id from 'tasks_ids' in metadata response")
        task_id = tasks_ids[0]

        logger.info(f"✓ Metadata registered. Task ID: {task_id}")

        logger.debug(f"Step 2: Uploading file data for task {task_id}")
        with open(file_path, "rb") as f:
            files = {"data": (file_path.name, f, "application/octet-stream")}
            response_data = await client.post(
                f"{router.base_url()}/api/v4/buckets/data/{task_id}",
                files=files,
                timeout=300.0,
            )
            response_data.raise_for_status()

        logger.info(f"✓ Upload complete for {relative_path}")
        return relative_path
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP {e.response.status_code} error uploading {relative_path}: {e.response.text}")
        raise
    except httpx.RequestError as e:
        logger.error(f"Network error uploading {relative_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error uploading {relative_path}: {e}")
        raise
