import asyncio
import logging
import os

logger = logging.getLogger(__name__)

SOCKET_PATH = "/tmp/nez_watcher.sock"

async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, queue: asyncio.Queue):
    """Handles a single client connection to the socket."""
    addr = writer.get_extra_info('peername')
    logger.info(f"Socket connection received: {addr}")

    try:
        data = await reader.read(1024)
        if data:
            message = data.decode().strip()
            logger.info(f"Received socket message: '{message}'")
            
            if message:
                queue.put_nowait(('socket_download_request', message))
                try:
                    writer.write(b"OK: Download request queued.\n")
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

async def start_socket_server(queue: asyncio.Queue):
    """Starts the Unix domain socket server."""
    # Clean up old socket file if it exists
    if os.path.exists(SOCKET_PATH):
        try:
            os.remove(SOCKET_PATH)
            logger.info(f"Removed old socket file: {SOCKET_PATH}")
        except OSError as e:
            logger.error(f"Error removing socket file {SOCKET_PATH}: {e}")
            return

    # Create a partial function to pass the queue to the handler
    connection_handler = lambda r, w: handle_connection(r, w, queue)

    server = await asyncio.start_unix_server(connection_handler, path=SOCKET_PATH)

    addr = server.sockets[0].getsockname()
    logger.info(f"Socket server listening on {addr}")

    async with server:
        await server.serve_forever()