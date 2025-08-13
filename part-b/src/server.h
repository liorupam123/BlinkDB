/**
 * @file server.h
 * @brief Header file for the BLINK DB server.
 * 
 * This file contains the declaration of the BlinkServer class, which handles
 * network connections using epoll for efficient I/O multiplexing.
 */

#ifndef SERVER_H
#define SERVER_H

#include "storage_engine.h"
#include "resp.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>

/**
 * @class BlinkServer
 * @brief Implements a TCP server for BLINK DB.
 * 
 * This class provides server functionality for the BLINK DB system,
 * handling client connections using epoll for I/O multiplexing.
 */
class BlinkServer {
public:
    /**
     * @brief Constructor.
     * @param port Port to listen on.
     * @param storage_engine Reference to the storage engine.
     */
    BlinkServer(int port, StorageEngine& storage_engine);
    
    /**
     * @brief Destructor.
     */
    ~BlinkServer();
    
    /**
     * @brief Starts the server.
     * @return True if the server started successfully, false otherwise.
     */
    bool start();
    
    /**
     * @brief Stops the server.
     */
    void stop();
    
    /**
     * @brief Checks if the server is running.
     * @return True if the server is running, false otherwise.
     */
    bool isRunning() const;

private:
    /**
     * @struct ClientConnection
     * @brief Structure to store client connection information.
     */
    struct ClientConnection {
        int fd;                   ///< File descriptor for the client connection.
        std::string buffer;       ///< Input buffer for the client.
        std::string output_buffer; ///< Output buffer for the client.
    };
    
    /**
     * @brief Initializes the epoll instance.
     * @return True if successful, false otherwise.
     */
    bool initEpoll();
    
    /**
     * @brief Starts listening on the configured port.
     * @return True if successful, false otherwise.
     */
    bool startListening();
    
    /**
     * @brief Accepts new client connections.
     */
    void acceptClient();
    
    /**
     * @brief Handles data from a client.
     * @param client_fd File descriptor of the client.
     */
    void handleClient(int client_fd);
    
    /**
     * @brief Processes a client request.
     * @param command The RESP command to process.
     * @return The RESP response to send to the client.
     */
    resp::Value processCommand(const resp::Value& command);
    
    /**
     * @brief Sends a response to a client.
     * @param client_fd File descriptor of the client.
     * @param response The RESP response to send.
     */
    void sendResponse(int client_fd, const resp::Value& response);
    
    /**
     * @brief Closes a client connection.
     * @param client_fd File descriptor of the client.
     */
    void closeClient(int client_fd);
    
    /**
     * @brief Main server loop for handling events.
     */
    void serverLoop();
    
    int port_;                                 ///< Port on which the server listens.
    StorageEngine& storage_engine_;            ///< Reference to the storage engine.
    int server_fd_ = -1;                       ///< File descriptor for the server socket.
    int epoll_fd_ = -1;                        ///< File descriptor for the epoll instance.
    std::atomic<bool> running_ = {false};      ///< Flag indicating whether the server is running.
    std::thread server_thread_;                ///< Thread for the main server loop.
    std::unordered_map<int, ClientConnection> clients_; ///< Map of active client connections.
};

#endif // SERVER_H