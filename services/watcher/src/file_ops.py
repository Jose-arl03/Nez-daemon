import asyncio
import logging
from pathlib import Path
from watchdog.events import FileSystemEventHandler

# Import config variables
from .config import FILE_STABILITY_TIMEOUT, QUARANTINE_DIRECTORY

logger = logging.getLogger(__name__)

async def wait_for_file_stability(file_path: Path, timeout: float = FILE_STABILITY_TIMEOUT):
    """Waits for a file to stop changing (no size/mtime changes)."""
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
