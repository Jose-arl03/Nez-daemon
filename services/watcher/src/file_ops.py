import asyncio
import logging
import shutil
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
        
        shutil.move(str(file_path), str(destination))
        logger.warning(f"Moved failed file to quarantine: {destination}")
    except Exception as e:
        logger.error(f"Failed to move {file_path} to quarantine: {e}")

class NewFileHandler(FileSystemEventHandler):
    """Handles file system events and adds new files to the processing queue."""
    
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.queue = queue
        self.loop = loop
        super().__init__()
    
    def on_created(self, event):
        """
        Called when a file or directory is created.
        Puts a ('filesystem_event', path) task onto the queue.
        """
        src_path = Path(event.src_path)
        if event.is_directory:
            logger.info(f"📁 New directory detected: {src_path}. Scanning for files...")
            # Use a small delay to allow files to be fully moved/created inside the new directory
            async def delayed_scan():
                await asyncio.sleep(1.0)
                for file_path in src_path.rglob('*'):
                    if file_path.is_file():
                        logger.info(f"  - Queuing file from directory: {file_path}")
                        self.queue.put_nowait(('filesystem_event', file_path))
            asyncio.run_coroutine_threadsafe(delayed_scan(), self.loop)
        else:
            # It's a single file
            logger.info(f"📁 New file detected: {src_path}")
            self.queue.put_nowait(('filesystem_event', src_path))
