# Developer Guide: Watcher Service and C++ Client

This document provides a comprehensive technical guide for developers working with the Nez-daemon Watcher service and its C++ client.

## 1. Service Setup and Configuration

### 1.1. System Dependencies
Before running the service or compiling the client, ensure you have the following installed:
*   **Python 3.8+** with `pip`
*   **CMake 3.10+**
*   A C++ compiler supporting C++11 (e.g., **g++**)

### 1.2. Python Dependencies
The service's Python dependencies are listed in `requirements.txt`. Install them using:
```bash
pip install -r Nez-daemon/services/watcher/requirements.txt
```

### 1.3. Service Configuration (Environment Variables)

The `watcher.py` service is configured via environment variables. These are defined in `src/config.py`.

| Variable                 | Default Value                                           | Description                                                                                                                               |
| ------------------------ | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `LOG_LEVEL`              | `INFO`                                                  | Sets the logging level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`).                                                                         |
| `WATCH_DIRECTORY`        | `.../deployer/app/results`                              | The absolute path to the directory the service will monitor for new files to upload.                                                      |
| `QUARANTINE_DIRECTORY`   | `.../deployer/app/quarantine`                           | The directory where files that fail processing are moved.                                                                                 |
| `MICTLANX_URI`           | **(None)**                                              | **Required.** The URI for the MictlanX router. Format: `mictlanx://<router_id>@<ip_addr>:<port>?protocol=<http_or_https>`.                    |
| `BUCKET_ID`              | `nez-bucket`                                            | The target bucket ID in MictlanX for all file operations.                                                                                 |
| `REPLICATION_FACTOR`     | `3`                                                     | The desired replication factor for files uploaded to MictlanX.                                                                            |
| `MAX_WORKERS`            | `5`                                                     | The number of concurrent asynchronous workers for processing tasks from the queue.                                                        |
| `MAX_FILE_SIZE_MB`       | `500`                                                   | The maximum size in megabytes for a single file to be uploaded. Files larger than this will be rejected.                                     |
| `FILE_STABILITY_TIMEOUT` | `2.0`                                                   | The time in seconds to wait after a file modification is detected before processing it, to ensure it's not still being written. |

### 1.4. Running the Service
Set the required `MICTLANX_URI` environment variable and run the main script:
```bash
export MICTLANX_URI="mictlanx://router1@127.0.0.1:8000?protocol=http"
python Nez-daemon/services/watcher/watcher.py
```
The service will start, and you should see log output indicating that the workers, watchdog observer, and socket server are running.

## 2. Unix Socket API Reference

The service exposes a simple RPC-style API over a Unix socket located at `/tmp/nez_watcher.sock`. Clients must send a single-line JSON request and will receive a single-line JSON response.

---


### Action: `query_existence`
Checks if a file or directory exists at a given path. **Note:** In the current implementation, this checks for existence on the *host's local filesystem*, not within MictlanX.

*   **`path` parameter**: An absolute or relative path to a file or directory.
*   **Success Response**:
    ```json
    {"status": "success", "action": "query_existence", "path": "/path/to/check", "result": "Existence query for '/path/to/check': Exists."}
    ```
*   **Error Response**:
    ```json
    {"status": "error", "message": "Invalid request format. 'action' and 'path' are required."}
    ```

---


### Action: `download_file`
Requests the download of a single file from MictlanX to the local `downloads/` directory.

*   **`path` parameter**: The **exact original path** of the file as it was uploaded, which is stored in the MictlanX metadata `path` tag.
*   **Success Response**:
    ```json
    {"status": "success", "action": "download_file", "path": "my_folder/report.pdf", "result": "File download request for 'my_folder/report.pdf' processed."}
    ```
*   **Error Response (File not found in MictlanX):**
    ```json
    {"status": "error", "message": "Error processing request: No file found in MictlanX with path: my_folder/report.pdf"}
    ```

---


### Action: `download_directory`
Requests the download of an entire directory (and its sub-contents) from MictlanX to the local `downloads/` directory.

*   **`path` parameter**: The path prefix of the directory. For example, `my_project/` will download all files whose original path started with `my_project/`. A trailing slash is recommended for clarity.
*   **Success Response**:
    ```json
    {"status": "success", "action": "download_directory", "path": "my_project/", "result": "Directory download request for 'my_project/' processed."}
    ```
*   **Error Response**:
    ```json
    {"status": "error", "message": "Error processing request: Could not retrieve bucket metadata for folder download: ..."}
    ```

## 3. C++ Client Developer Guide

The C++ client provides a reference implementation for communicating with the socket API.

### 3.1. Compilation
The project uses CMake and requires a C++11 compliant compiler.
```bash
# From Nez-daemon/services/watcher/client_cpp/
mkdir -p build && cd build
cmake ..
make
```

### 3.2. Complete Code Example (`main.cpp`)
Below is a complete, well-commented example that can be used as a starting point for a custom client.

```cpp
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h> 
#include <cstring> // For memset and perror

// The well-known path for the Unix socket
#define SOCKET_PATH "/tmp/nez_watcher.sock"
#define BUFFER_SIZE 2048

int main(int argc, char *argv[]) {
    // 1. Validate Arguments and Construct JSON
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <action> <path>" << std::endl;
        std::cerr << "Actions: query_existence, download_file, download_directory" << std::endl;
        return 1;
    }
    std::string action = argv[1];
    std::string path = argv[2];
    // Manually construct the JSON string. For a more robust client, use a JSON library.
    std::string json_message = "{\"action\": \"" + action + "\", \"path\": \"" + path + "\"}";

    // 2. Create Socket
    int client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd == -1) {
        perror("socket error");
        return 1;
    }

    // 3. Set up server address structure
    struct sockaddr_un server_addr;
    memset(&server_addr, 0, sizeof(struct sockaddr_un)); // Zero out the structure
    server_addr.sun_family = AF_UNIX; // Specify Unix domain socket
    // Copy the socket path to the address structure
    strncpy(server_addr.sun_path, SOCKET_PATH, sizeof(server_addr.sun_path) - 1);

    // 4. Connect to the server
    if (connect(client_fd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_un)) == -1) {
        perror("connect error");
        close(client_fd);
        return 1;
    }
    std::cout << "--> Connected to " << SOCKET_PATH << std::endl;
    std::cout << "--> Sending: " << json_message << std::endl;

    // 5. Send the request
    if (write(client_fd, json_message.c_str(), json_message.length()) == -1) {
        perror("write error");
        close(client_fd);
        return 1;
    }

    // 6. Receive the response
    char buffer[BUFFER_SIZE];
    ssize_t bytes_received = read(client_fd, buffer, BUFFER_SIZE - 1);
    if (bytes_received == -1) {
        perror("read error");
    } else if (bytes_received > 0) {
        buffer[bytes_received] = '\0'; // Null-terminate the received data
        std::cout << "<-- Received: " << buffer << std::endl;
    }

    // 7. Close the socket
    close(client_fd);

    return 0;
}
```

## 4. Advanced Topics & Internals

This section covers important behaviors and design choices of the service.

### 4.1. Concurrency Model
The service uses Python's `asyncio` library, which is an **event-driven, single-threaded concurrency model**. It can handle thousands of concurrent I/O-bound operations (like waiting for network responses from MictlanX or handling socket connections) efficiently without using multiple threads. This avoids issues related to Python's Global Interpreter Lock (GIL) but means that CPU-bound tasks within the Python code will block the entire event loop.

### 4.2. Duplicate File Handling (Idempotency)
The Watcher service is **idempotent** for file uploads. Before uploading a file, it first checks if a file with the same *sanitized key* already exists in MictlanX.
*   If the key **exists**, the upload is skipped, and a log message is generated.
*   If the key **does not exist**, the upload proceeds.

This prevents duplicate data from being uploaded if the service is restarted or if the same file is added to the `WATCH_DIRECTORY` multiple times.

### 4.3. File Stability Check
When monitoring a filesystem, it's common for a "file created" event to fire before the file has been completely written. To prevent uploading incomplete files, the service implements a stability check:
1.  A new file is detected.
2.  The service waits for `FILE_STABILITY_TIMEOUT` seconds (default: 2.0s).
3.  After the timeout, it checks if the file's modification time has changed again.
4.  Processing only begins once the file has remained unmodified for the duration of the timeout, indicating that the write operation is complete.

### 4.4. Error Handling and Quarantine
If a file upload fails after multiple retries (e.g., due to persistent network errors or MictlanX service issues), the service will **move the failed file to the `QUARANTINE_DIRECTORY`**. This is a critical feature that prevents a failing file from blocking the processing queue indefinitely. A developer or administrator can then inspect the files in quarantine to diagnose the problem manually.

### 4.5. Known Limitations
*   **`query_existence` Scope**: As noted in the API reference, this action only checks for the file's existence on the **local filesystem** where the Watcher is running, not within the MictlanX cluster.
*   **Key Collisions**: The `sanitize_key` function removes all non-alphanumeric characters to create a valid key for MictlanX. It is theoretically possible for two different file paths to sanitize to the same key (e.g., `data/report-v1.txt` and `data/report_v1.txt` could both become `datareportv1txt`). In this scenario, the second file would be treated as a duplicate of the first.
*   **C++ Client JSON Handling**: The reference C++ client constructs JSON strings manually. For production use, it is highly recommended to use a dedicated C++ JSON library (e.g., `nlohmann/json`, `jsoncpp`) to handle special characters and ensure valid formatting.