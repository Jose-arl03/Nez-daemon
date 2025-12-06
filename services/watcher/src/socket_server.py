import asyncio
import logging
import os
import json
from mictlanx.services.router import AsyncRouter # Import AsyncRouter
from .mictlanx_ops import process_download # Import process_download

logger = logging.getLogger(__name__)

SOCKET_PATH = "/tmp/nez_watcher.sock"

async def handle_download_file(router: AsyncRouter, path: str) -> str:
    """Handles file download requests using MictlanX."""
    logger.info(f"Attempting to download file: {path}")
    try:
        await process_download(router, path)
        return f"File download request for '{path}' processed."
    except Exception as e:
        logger.error(f"Error downloading file '{path}': {e}")
        return f"Error downloading file '{path}': {e}"

async def handle_download_directory(router: AsyncRouter, path: str) -> str:
    """Handles directory download requests using MictlanX."""
    logger.info(f"Attempting to download directory: {path}")
    try:
        # Ensure path ends with '/' for directory download in process_download
        if not path.endswith('/'):
            path += '/'
        await process_download(router, path)
        return f"Directory download request for '{path}' processed."
    except Exception as e:
        logger.error(f"Error downloading directory '{path}': {e}")
        return f"Error downloading directory '{path}': {e}"

async def handle_query_existence(router: AsyncRouter, path: str) -> str:
    """Handles file/directory existence queries."""
    logger.info(f"Querying existence of: {path}")
    # This will check existence on the host's mounted /tmp
    exists = os.path.exists(path) 
    return f"Existence query for '{path}': {'Exists' if exists else 'Does not exist'}."


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, queue: asyncio.Queue, router: AsyncRouter):
    """Handles a single client connection to the socket."""
    addr = writer.get_extra_info('peername')
    logger.info(f"Socket connection received: {addr}")
    response_message = {"status": "error", "message": "Unknown error."}

    try:
        data = await reader.read(1024)
        if data:
            message_str = data.decode().strip()
            logger.info(f"Received socket message: '{message_str}'")
            
            try:
                request = json.loads(message_str)
                action = request.get("action")
                path = request.get("path")

                if not action or not path:
                    response_message = {"status": "error", "message": "Invalid request format. 'action' and 'path' are required."}
                else:
                    if action == "download_file":
                        result = await handle_download_file(router, path)
                        response_message = {"status": "success", "action": action, "path": path, "result": result}
                    elif action == "download_directory":
                        result = await handle_download_directory(router, path)
                        response_message = {"status": "success", "action": action, "path": path, "result": result}
                    elif action == "query_existence":
                        result = await handle_query_existence(router, path)
                        response_message = {"status": "success", "action": action, "path": path, "result": result}
                    else:
                        response_message = {"status": "error", "message": f"Unknown action: {action}"}

            except json.JSONDecodeError:
                response_message = {"status": "error", "message": "Invalid JSON format."}
            except Exception as e:
                response_message = {"status": "error", "message": f"Error processing request: {e}"}
            
            # Send JSON response back to client
            try:
                writer.write(json.dumps(response_message).encode() + b"\n")
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                logger.warning(f"Client disconnected before server could send response. This is safe to ignore.")

    except Exception as e:
        logger.error(f"Error in socket connection handler: {e}")
    finally:
        logger.info(f"Closing socket connection: {addr}")
        try:
            writer.close()
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError):
            # This can also happen if the client is gone.
            pass

async def start_socket_server(queue: asyncio.Queue, router: AsyncRouter):
    """Starts the Unix domain socket server."""
    # Clean up old socket file if it exists
    if os.path.exists(SOCKET_PATH):
        try:
            if os.path.isdir(SOCKET_PATH):
                os.rmdir(SOCKET_PATH)
                logger.info(f"Removed old socket directory: {SOCKET_PATH}")
            else:
                os.remove(SOCKET_PATH)
                logger.info(f"Removed old socket file: {SOCKET_PATH}")
        except OSError as e:
            logger.error(f"Error removing socket path {SOCKET_PATH}: {e}")
            return

    # Create a partial function to pass the queue and router to the handler
    connection_handler = lambda r, w: handle_connection(r, w, queue, router)

    server = await asyncio.start_unix_server(connection_handler, path=SOCKET_PATH)

    # Set permissions for the socket file to allow external access
    os.chmod(SOCKET_PATH, 0o666)

    addr = server.sockets[0].getsockname()
    logger.info(f"Socket server listening on {addr}")

    async with server:
        await server.serve_forever()