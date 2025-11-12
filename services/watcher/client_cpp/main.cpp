#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring> // For memset

#define SOCKET_PATH "/tmp/nez_watcher.sock"
#define BUFFER_SIZE 1024

int main(int argc, char *argv[]) { // Modified main function signature
    int client_fd;
    struct sockaddr_un server_addr;
    char buffer[BUFFER_SIZE];
    std::string json_message;

    // Parse command-line arguments
    if (argc == 3) {
        std::string action = argv[1];
        std::string path = argv[2];
        json_message = "{\"action\": \"" + action + "\", \"path\": \"" + path + "\"}";
    } else {
        std::cerr << "Usage: " << argv[0] << " <action> <path>" << std::endl;
        std::cerr << "Actions: query_existence, download_file, download_directory" << std::endl;
        std::cerr << "Example: " << argv[0] << " download_directory prueba_k/" << std::endl;
        return 1; // Exit if incorrect arguments
    }

    // 1. Create socket
    client_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd == -1) {
        perror("socket");
        return 1;
    }

    // 2. Set up server address
    memset(&server_addr, 0, sizeof(struct sockaddr_un));
    server_addr.sun_family = AF_UNIX;
    strncpy(server_addr.sun_path, SOCKET_PATH, sizeof(server_addr.sun_path) - 1);

    // 3. Connect to server
    if (connect(client_fd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_un)) == -1) {
        perror("connect");
        close(client_fd);
        return 1;
    }

    std::cout << "Connected to Unix socket server: " << SOCKET_PATH << std::endl;

    // 4. Send the dynamically constructed JSON message
    if (write(client_fd, json_message.c_str(), json_message.length()) == -1) {
        perror("write");
        close(client_fd);
        return 1;
    }
    std::cout << "Sent message: '" << json_message << "'" << std::endl;

    // 5. Receive response
    ssize_t bytes_received = read(client_fd, buffer, BUFFER_SIZE - 1);
    if (bytes_received == -1) {
        perror("read");
        close(client_fd);
        return 1;
    }
    buffer[bytes_received] = '\0'; // Null-terminate the received data
    std::cout << "Received response: '" << buffer << "'" << std::endl;

    // 6. Close socket
    close(client_fd);
    std::cout << "Connection closed." << std::endl;

    return 0;
}
