CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -pthread -march=native

SRC_DIR = src
BUILD_DIR = build
DOC_DIR = docs

# Source files for the optimized LSM storage engine
STORAGE_SRC = $(SRC_DIR)/storage_engine.cpp

# Source files for server
SERVER_SRC = $(SRC_DIR)/main.cpp \
             $(SRC_DIR)/server.cpp \
             $(SRC_DIR)/resp.cpp \
             $(STORAGE_SRC)

# Source files for client
CLIENT_SRC = $(SRC_DIR)/client.cpp \
             $(SRC_DIR)/resp.cpp

# Object files
SERVER_OBJ = $(SERVER_SRC:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)
CLIENT_OBJ = $(CLIENT_SRC:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)

# Executables
SERVER_EXE = blinkdb_server
CLIENT_EXE = blinkdb_client

# Default target
all: $(SERVER_EXE) $(CLIENT_EXE)

# Run target - builds and runs the server
run: $(SERVER_EXE)
	./$(SERVER_EXE)

# Run client target
run-client: $(CLIENT_EXE)
	./$(CLIENT_EXE)

$(SERVER_EXE): $(SERVER_OBJ)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(CLIENT_EXE): $(CLIENT_OBJ)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# Doxygen
docs:
	@mkdir -p $(DOC_DIR)
	doxygen Doxyfile

# Clean
clean:
	rm -rf $(BUILD_DIR) $(SERVER_EXE) $(CLIENT_EXE)

clean-docs:
	rm -rf $(DOC_DIR)

clean-all: clean clean-docs

# Run benchmarks
benchmark: $(SERVER_EXE)
	./run_benchmarks.sh

# Dependencies
$(BUILD_DIR)/main.o: $(SRC_DIR)/storage_engine.h $(SRC_DIR)/server.h
$(BUILD_DIR)/server.o: $(SRC_DIR)/server.h $(SRC_DIR)/storage_engine.h $(SRC_DIR)/resp.h
$(BUILD_DIR)/storage_engine.o: $(SRC_DIR)/storage_engine.h
$(BUILD_DIR)/resp.o: $(SRC_DIR)/resp.h
$(BUILD_DIR)/client.o: $(SRC_DIR)/resp.h

.PHONY: all run run-client docs clean clean-docs clean-all benchmark