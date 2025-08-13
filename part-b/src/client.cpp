/**
 * @file client.cpp
 * @brief Simple client for the BLINK DB server.
 * 
 * This file contains a simple client implementation for testing the BLINK DB server.
 * It connects to the server and allows the user to send commands interactively.
 */

#include "resp.h"
#include <iostream>
#include <string>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <algorithm> // Added for std::transform

/**
 * @brief Print usage information for the client.
 * @param progname The name of the program.
 */
void printUsage(const char* progname) {
    std::cout << "Usage: " << progname << " [OPTIONS]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --host HOST       Set server host (default: 127.0.0.1)" << std::endl;
    std::cout << "  --port PORT       Set server port (default: 9001)" << std::endl;
    std::cout << "  --help            Display this help message" << std::endl;
}

/**
 * @brief Parse a command string into tokens.
 * @param cmd The command string to parse.
 * @return A vector of tokens extracted from the command string.
 */
std::vector<std::string> parseCommand(const std::string& cmd) {
    std::vector<std::string> tokens;
    std::string token;
    bool in_quotes = false;
    std::stringstream ss;
    
    for (char c : cmd) {
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == ' ' && !in_quotes) {
            if (!ss.str().empty()) {
                tokens.push_back(ss.str());
                ss.str("");
            }
        } else {
            ss << c;
        }
    }
    
    if (!ss.str().empty()) {
        tokens.push_back(ss.str());
    }
    
    return tokens;
}

/**
 * @brief Format a RESP response for display.
 * @param value The RESP value to format.
 * @return A formatted string representation of the RESP value.
 */
std::string formatResponse(const resp::Value& value) {
    switch (value.getType()) {
        case resp::Type::SimpleString: {
            auto str = value.getString();
            return str ? *str : "ERROR";
        }
        
        case resp::Type::Error: {
            auto str = value.getString();
            return str ? "ERROR: " + *str : "ERROR";
        }
        
        case resp::Type::Integer: {
            auto num = value.getInteger();
            return num ? "(" + std::to_string(*num) + ")" : "ERROR";
        }
        
        case resp::Type::BulkString: {
            if (value.isNull()) {
                return "NULL";
            }
            auto str = value.getString();
            return str ? "\"" + *str + "\"" : "ERROR";
        }
        
        case resp::Type::Array: {
            if (value.isNull()) {
                return "NULL ARRAY";
            }
            
            auto array = value.getArray();
            if (!array) {
                return "ERROR";
            }
            
            std::string result = "Array[" + std::to_string(array->size()) + "]:\n";
            for (size_t i = 0; i < array->size(); i++) {
                result += "  " + std::to_string(i) + ") " + formatResponse((*array)[i]) + "\n";
            }
            return result;
        }
        
        default:
            return "UNKNOWN TYPE";
    }
}

/**
 * @brief Main function for the BLINK DB client.
 * 
 * This function connects to the BLINK DB server, sends commands interactively,
 * and displays the server's responses.
 * 
 * @param argc The number of command-line arguments.
 * @param argv The array of command-line arguments.
 * @return 0 on successful execution, or a non-zero value on error.
 */
int main(int argc, char* argv[]) {
    // Parse command line arguments
    std::string host = "127.0.0.1";
    int port = 9001;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            printUsage(argv[0]);
            return 0;
        }
    }
    
    // Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
        return 1;
    }
    
    // Connect to server
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address: " << host << std::endl;
        close(sock);
        return 1;
    }
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        std::cerr << "Failed to connect to " << host << ":" << port << ": " << strerror(errno) << std::endl;
        close(sock);
        return 1;
    }
    
    std::cout << "Connected to BLINK DB server at " << host << ":" << port << std::endl;
    std::cout << "Type 'quit' to exit" << std::endl;
    
    // Main loop
    std::string line;
    char buffer[4096];
    
    while (true) {
        std::cout << "BLINK> ";
        std::getline(std::cin, line);
        
        if (line == "quit" || line == "exit") {
            break;
        }
        
        // Skip empty lines
        if (line.empty()) {
            continue;
        }
        
        // Parse command
        auto tokens = parseCommand(line);
        if (tokens.empty()) {
            continue;
        }
        
        // Convert to uppercase
        std::string cmd = tokens[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
        
        // Create RESP command
        std::vector<resp::Value> cmd_array;
        for (const auto& token : tokens) {
            cmd_array.push_back(resp::Value::createBulkString(token));
        }
        
        resp::Value command = resp::Value::createArray(cmd_array);
        std::string cmd_str = command.serialize();
        
        // Send command
        ssize_t bytes_sent = send(sock, cmd_str.c_str(), cmd_str.length(), 0);
        if (bytes_sent <= 0) {
            std::cerr << "Failed to send command: " << strerror(errno) << std::endl;
            break;
        }
        
        // Receive response
        ssize_t bytes_recv = recv(sock, buffer, sizeof(buffer) - 1, 0);
        if (bytes_recv <= 0) {
            std::cerr << "Connection closed by server" << std::endl;
            break;
        }
        
        buffer[bytes_recv] = '\0';
        
        // Parse response
        size_t consumed = 0;
        auto response = resp::Value::deserialize(buffer, consumed);
        
        if (!response) {
            std::cerr << "Failed to parse response" << std::endl;
            continue;
        }
        
        // Print response
        std::cout << formatResponse(*response) << std::endl;
    }
    
    close(sock);
    return 0;
}