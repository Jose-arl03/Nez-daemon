import os
from pathlib import Path

# --- Logging Configuration ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# --- Path Configuration ---
# Make paths relative to the main script file.
# Assumes the main script is in the parent directory of this 'src' folder.
_SCRIPT_DIR = Path(__file__).resolve().parent.parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent

# Default paths for watch and quarantine directories
_WATCH_DIR_DEFAULT = _PROJECT_ROOT / "services" / "deployer" / "app" / "results"
_QUARANTINE_DIR_DEFAULT = _PROJECT_ROOT / "services" / "deployer" / "app" / "quarantine"

# Paths
WATCH_DIRECTORY = os.getenv("WATCH_DIRECTORY", str(_WATCH_DIR_DEFAULT))
QUARANTINE_DIRECTORY = os.getenv("QUARANTINE_DIRECTORY", str(_QUARANTINE_DIR_DEFAULT))

# MictlanX Configuration
BUCKET_ID = os.getenv("BUCKET_ID", "nez-bucket")
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
MICTLANX_URI = os.getenv("MICTLANX_URI", "")
MICTLANX_ROUTER = None  # Will be populated at runtime

# Worker Configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "500"))
FILE_STABILITY_TIMEOUT = float(os.getenv("FILE_STABILITY_TIMEOUT", "2.0"))
