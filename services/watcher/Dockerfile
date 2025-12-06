# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file for the watcher service
# Note: Paths are relative to the build context root (the 'Estadia' directory)
COPY Nez-daemon/services/watcher/requirements.txt .

# Copy the mictlanx-client library source code
COPY mictlanx-client/ /app/mictlanx-client/

# Install the mictlanx-client library first, as it's a local dependency.
# This command also installs its own dependencies from TestPyPI.
RUN pip install --extra-index-url https://test.pypi.org/simple/ -e /app/mictlanx-client/

# Install the rest of the watcher's specific dependencies
RUN pip install -r requirements.txt

# Copy the watcher's refactored source code into the container
COPY Nez-daemon/services/watcher/ /app/watcher/

# Set the command to run the application
CMD ["python", "/app/watcher/watcher.py"]
