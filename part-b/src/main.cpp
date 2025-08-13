/**
 * @file main.cpp
 * @brief Entry point for the BLINK DB server application.
 *
 * This file initializes the BLINK DB server, sets up signal handling, and starts the server
 * to handle client requests. It uses the LSM-based storage engine for efficient data management.
 */

#include "storage_engine.h"
#include "server.h"
#include <iostream>
#include <signal.h>
#include <unistd.h>

// Global server pointer for signal handling
BlinkServer* g_server = nullptr;

/**
 * @brief Signal handler for gracefully shutting down the server.
 * 
 * This function is called when the server receives a termination signal (e.g., SIGINT or SIGTERM).
 * It stops the server and performs cleanup.
 * 
 * @param signal The signal number received.
 */
void signalHandler(int signal) {
    std::cout << "Received signal " << signal << ", shutting down..." << std::endl;
    if (g_server) {
        g_server->stop();
    }
}

/**
 * @brief Main function for the BLINK DB server application.
 * 
 * This function parses command-line arguments, initializes the storage engine and server,
 * sets up signal handlers, and starts the server to handle client requests.
 * 
 * Supported command-line options:
 * - `--port PORT`: Set the server port (default: 9001).
 * - `--memory SIZE`: Set the maximum memory size in MB for the storage engine (default: 100MB).
 * - `--help`: Display usage information.
 * 
 * @param argc The number of command-line arguments.
 * @param argv The array of command-line arguments.
 * @return 0 on successful execution, or a non-zero value on error.
 */
int main(int argc, char* argv[]) {
    // Parse command line arguments
    int port = 9001; // Default port
    size_t memory_size = 100 * 1024 * 1024; // Default 100MB
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--memory" && i + 1 < argc) {
            memory_size = std::stoull(argv[++i]) * 1024 * 1024; // Convert MB to bytes
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --port PORT       Set server port (default: 9001)" << std::endl;
            std::cout << "  --memory SIZE     Set max memory size in MB (default: 100)" << std::endl;
            std::cout << "  --help            Display this help message" << std::endl;
            return 0;
        }
    }
    
    // Create storage engine with optimized LSM Tree implementation
    std::cout << "Initializing LSM storage engine with " << (memory_size / (1024 * 1024)) << "MB memory limit..." << std::endl;
    StorageEngine storage_engine(memory_size);
    
    // Create server
    std::cout << "Creating server on port " << port << "..." << std::endl;
    BlinkServer server(port, storage_engine);
    g_server = &server;
    
    // Set up signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    
    // Start server
    if (!server.start()) {
        std::cerr << "Failed to start server" << std::endl;
        return 1;
    }
    
    std::cout << "BLINK DB server is running. Press Ctrl+C to stop." << std::endl;
    
    // Wait for server to stop
    while (server.isRunning()) {
        sleep(1);
    }
    
    return 0;
}