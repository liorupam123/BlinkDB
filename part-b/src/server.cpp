/**
 * @file server.cpp
 * @brief Implementation of the BLINK DB server.
 */

#include "server.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <iostream>
#include <string.h>

const int MAX_EVENTS = 64; ///< Maximum number of events to process at once.
const int BACKLOG = 128; ///< Maximum number of pending connections in the listen queue.
const int BUFFER_SIZE = 4096; ///< Buffer size for reading client data.

BlinkServer::BlinkServer(int port, StorageEngine& storage_engine)
    : port_(port), storage_engine_(storage_engine) {}

BlinkServer::~BlinkServer() {
    stop();
}

/**
 * @brief Starts the server.
 * @return True if the server starts successfully, false otherwise.
 */
bool BlinkServer::start() {
    if (running_) {
        return true;
    }
    if (!startListening()) {
        return false;
    }
    if (!initEpoll()) {
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }
    running_ = true;
    server_thread_ = std::thread(&BlinkServer::serverLoop, this);
    std::cout << "BLINK DB server started on port " << port_ << std::endl;
    return true;
}

/**
 * @brief Stops the server and cleans up resources.
 */
void BlinkServer::stop() {
    if (!running_) {
        return;
    }
    running_ = false;
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    for (auto& [fd, _] : clients_) {
        close(fd);
    }
    clients_.clear();
    if (server_fd_ != -1) {
        close(server_fd_);
        server_fd_ = -1;
    }
    if (epoll_fd_ != -1) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
    std::cout << "BLINK DB server stopped" << std::endl;
}

/**
 * @brief Main server loop to handle client connections and requests.
 */
void BlinkServer::serverLoop() {
    struct epoll_event events[MAX_EVENTS];
    while (running_) {
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
        if (num_events == -1) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "epoll_wait error: " << strerror(errno) << std::endl;
            break;
        }
        for (int i = 0; i < num_events; i++) {
            if (events[i].data.fd == server_fd_) {
                acceptClient();
            } else {
                if (events[i].events & EPOLLIN) {
                    handleClient(events[i].data.fd);
                }
                if (events[i].events & (EPOLLHUP | EPOLLERR)) {
                    closeClient(events[i].data.fd);
                }
            }
        }
    }
}

/**
 * @brief Accepts a new client connection.
 */
void BlinkServer::acceptClient() {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            std::cerr << "Failed to accept client connection: " << strerror(errno) << std::endl;
        }
        return;
    }
    int flags = fcntl(client_fd, F_GETFL, 0);
    if (flags == -1) {
        std::cerr << "Failed to get client socket flags: " << strerror(errno) << std::endl;
        close(client_fd);
        return;
    }
    if (fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        std::cerr << "Failed to set client socket non-blocking mode: " << strerror(errno) << std::endl;
        close(client_fd);
        return;
    }
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = client_fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &event) == -1) {
        std::cerr << "Failed to add client socket to epoll: " << strerror(errno) << std::endl;
        close(client_fd);
        return;
    }
    clients_[client_fd] = {client_fd, "", ""};
    std::cout << "New client connected: " << client_fd << std::endl;
}

/**
 * @brief Checks if the server is running.
 * @return True if the server is running, false otherwise.
 */
bool BlinkServer::isRunning() const {
    return running_;
}

/**
 * @brief Starts listening for incoming connections.
 * @return True if the server starts listening successfully, false otherwise.
 */
bool BlinkServer::startListening() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == -1) {
        std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
        return false;
    }
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        std::cerr << "Failed to set socket options: " << strerror(errno) << std::endl;
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }
    int flags = fcntl(server_fd_, F_GETFL, 0);
    if (flags == -1) {
        std::cerr << "Failed to get socket flags: " << strerror(errno) << std::endl;
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }
    if (fcntl(server_fd_, F_SETFL, flags | O_NONBLOCK) == -1) {
        std::cerr << "Failed to set non-blocking mode: " << strerror(errno) << std::endl;
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        std::cerr << "Failed to bind to port " << port_ << ": " << strerror(errno) << std::endl;
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }
    if (listen(server_fd_, BACKLOG) == -1) {
        std::cerr << "Failed to listen on port " << port_ << ": " << strerror(errno) << std::endl;
        close(server_fd_);
        server_fd_ = -1;
        return false;
    }
    return true;
}

/**
 * @brief Initializes the epoll instance for handling multiple connections.
 * @return True if epoll is initialized successfully, false otherwise.
 */
bool BlinkServer::initEpoll() {
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ == -1) {
        std::cerr << "Failed to create epoll instance: " << strerror(errno) << std::endl;
        return false;
    }
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &event) == -1) {
        std::cerr << "Failed to add server socket to epoll: " << strerror(errno) << std::endl;
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }
    return true;
}

/**
 * @brief Handles client requests.
 * @param client_fd The file descriptor of the client.
 */
void BlinkServer::handleClient(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        std::cerr << "Client not found: " << client_fd << std::endl;
        return;
    }
    ClientConnection& client = it->second;
    char buffer[BUFFER_SIZE];
    ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE);
    if (bytes_read <= 0) {
        if (bytes_read == 0 || errno != EAGAIN) {
            closeClient(client_fd);
        }
        return;
    }
    client.buffer.append(buffer, bytes_read);
    size_t consumed = 0;
    while (true) {
        size_t local_consumed = 0;
        auto command = resp::Value::deserialize(client.buffer, local_consumed);
        if (!command) {
            break;
        }
        auto response = processCommand(*command);
        sendResponse(client_fd, response);
        client.buffer.erase(0, local_consumed);
        consumed += local_consumed;
        if (client.buffer.empty()) {
            break;
        }
    }
}

/**
 * @brief Processes a RESP command received from a client.
 * @param command The RESP command.
 * @return The RESP response.
 */
resp::Value BlinkServer::processCommand(const resp::Value& command) {
    auto array = command.getArray();
    if (!array || array->empty()) {
        return resp::Value::createError("Invalid command format");
    }
    auto cmd_type = (*array)[0].getString();
    if (!cmd_type) {
        return resp::Value::createError("Command must be a string");
    }
    std::string cmd = *cmd_type;
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    if (cmd == "SET") {
        if (array->size() < 3) {
            return resp::Value::createError("SET command requires key and value arguments");
        }
        auto key = (*array)[1].getString();
        auto value = (*array)[2].getString();
        if (!key || !value) {
            return resp::Value::createError("SET command requires string arguments");
        }
        if (storage_engine_.set(*key, *value)) {
            return resp::Value::createSimpleString("OK");
        } else {
            return resp::Value::createError("Failed to set key");
        }
    } else if (cmd == "GET") {
        if (array->size() < 2) {
            return resp::Value::createError("GET command requires a key argument");
        }
        auto key = (*array)[1].getString();
        if (!key) {
            return resp::Value::createError("GET command requires a string key");
        }
        std::string value;
        if (storage_engine_.get(*key, value)) {
            return resp::Value::createBulkString(value);
        } else {
            return resp::Value::createNullBulkString();
        }
    } else if (cmd == "DEL") {
        if (array->size() < 2) {
            return resp::Value::createError("DEL command requires a key argument");
        }
        auto key = (*array)[1].getString();
        if (!key) {
            return resp::Value::createError("DEL command requires a string key");
        }
        if (storage_engine_.del(*key)) {
            return resp::Value::createInteger(1);
        } else {
            return resp::Value::createInteger(0);
        }
    } else {
        return resp::Value::createError("Unknown command: " + cmd);
    }
}

/**
 * @brief Sends a RESP response to a client.
 * @param client_fd The file descriptor of the client.
 * @param response The RESP response to send.
 */
void BlinkServer::sendResponse(int client_fd, const resp::Value& response) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }
    std::string response_str = response.serialize();
    ssize_t bytes_sent = write(client_fd, response_str.c_str(), response_str.length());
    if (bytes_sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        std::cerr << "Failed to send response to client " << client_fd << ": " << strerror(errno) << std::endl;
        closeClient(client_fd);
    }
}

/**
 * @brief Closes a client connection.
 * @param client_fd The file descriptor of the client.
 */
void BlinkServer::closeClient(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    close(client_fd);
    clients_.erase(it);
    std::cout << "Client disconnected: " << client_fd << std::endl;
}