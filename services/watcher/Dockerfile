FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the watcher script
COPY watcher.py .

# Create necessary directories
RUN mkdir -p /app/watch_dir /app/quarantine

# Environment variables with defaults
ENV WATCH_DIRECTORY=/app/watch_dir \
    QUARANTINE_DIRECTORY=/app/quarantine \
    BUCKET_ID=nez-bucket \
    REPLICATION_FACTOR=3 \
    MAX_WORKERS=5 \
    MAX_FILE_SIZE_MB=500 \
    FILE_STABILITY_TIMEOUT=2.0 \
    LOG_LEVEL=INFO \
    MICTLANX_URI=""

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run the watcher
CMD ["python", "-u", "watcher.py"]