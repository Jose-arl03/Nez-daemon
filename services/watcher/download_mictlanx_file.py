import asyncio
import httpx
import os
import logging
import sys
import argparse
from pathlib import Path

# --- Configuration ---
# Set default log level, can be overridden by LOG_LEVEL environment variable
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- Logging Setup ---
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- URI Parser (adapted from watcher.py) ---
def parse_mictlanx_uri(uri: str) -> str:
    """
    Parses the MictlanX URI and returns the base service URL.
    Example: mictlanx://mictlanx-router-0@host.docker.internal:60666?api_version=4&protocol=http
    Returns: http://host.docker.internal:60666
    """
    if not uri:
        raise ValueError("MICTLANX_URI environment variable is not set.")

    try:
        # Default protocol
        protocol = "http"
        # Extract protocol from query parameters if present
        if "?" in uri:
            query_part = uri.split("", 1)[1]
            # Simple parsing for key=value pairs
            params = {}
            for qc in query_part.split("&"):
                if "=" in qc:
                    key, value = qc.split("=", 1)
                    params[key] = value
            protocol = params.get("protocol", "http")

        # Extract host and port
        if "@" in uri:
            # Format: mictlanx://name@host:port?params
            host_port = uri.split("@")[1].split("?")[0]
        else:
            # Format: mictlanx://host:port?params
            host_port = uri.split("://", 1)[1].split("?")[0] # Corrected split

        service_url = f"{protocol}://{host_port}"
        logger.info(f"Parsed MictlanX Service URL: {service_url}")
        return service_url
    except Exception as e:
        logger.error(f"Failed to parse MICTLANX_URI '{uri}': {e}")
        sys.exit(1)

async def download_file(
    mictlanx_service_url: str,
    bucket_id: str,
    key: str,
    output_path: Path
) -> bool:
    """
    Downloads a file from MictlanX to the specified output path.

    Args:
        mictlanx_service_url: The base URL of the MictlanX service.
        bucket_id: The ID of the bucket where the file is stored.
        key: The sanitized key of the file to download.
        output_path: The local path where the file should be saved.

    Returns:
        True if the download was successful, False otherwise.
    """
    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"Attempting to download file '{key}' from bucket '{bucket_id}'...")
            response = await client.get(
                f"{mictlanx_service_url}/mictlanx/api/v4/buckets/{bucket_id}/{key}",
                timeout=300.0  # 5 minutes for large files
            )
            response.raise_for_status() # Raise an exception for 4xx/5xx responses

            # Ensure the output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the content to the file
            with open(output_path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

            logger.info(f"✓ Successfully downloaded '{key}' to '{output_path}'")
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP {e.response.status_code} error downloading '{key}': {e.response.text}")
            return False
        except httpx.RequestError as e:
            logger.error(f"Network error downloading '{key}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading '{key}': {e}")
            return False

async def main():
    parser = argparse.ArgumentParser(
        description="Download a file from MictlanX.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--mictlanx-uri",
        type=str,
        default=os.getenv("MICTLANX_URI", ""),
        help="MictlanX URI (e.g., mictlanx://host:port?protocol=http). Can also be set via MICTLANX_URI env var."
    )
    parser.add_argument(
        "--bucket-id",
        type=str,
        default=os.getenv("BUCKET_ID", "nez-bucket"),
        help="The ID of the MictlanX bucket. Can also be set via BUCKET_ID env var."
    )
    parser.add_argument(
        "--key",
        type=str,
        required=True,
        help="The sanitized key of the file to download from MictlanX."
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        required=True,
        help="The local path where the downloaded file will be saved."
    )

    args = parser.parse_args()

    if not args.mictlanx_uri:
        logger.error("MictlanX URI is not provided. Please set --mictlanx-uri or MICTLANX_URI environment variable.")
        sys.exit(1)

    mictlanx_service_url = parse_mictlanx_uri(args.mictlanx_uri)

    logger.info(f"Starting download for key '{args.key}' from bucket '{args.bucket_id}'...")
    success = await download_file(
        mictlanx_service_url,
        args.bucket_id,
        args.key,
        args.output_path
    )

    if success:
        logger.info("Download process completed successfully.")
    else:
        logger.error("Download process failed.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())