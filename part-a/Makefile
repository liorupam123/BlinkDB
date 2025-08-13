# Makefile for BlinkDB (Corrected for src directory)

# Compiler and flags
CXX = g++
# Added -pthread for std::thread and -g for debugging symbols
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread -g

# Linker flags for threading and filesystem libraries
LDFLAGS = -lstdc++fs

# --- Project Structure ---
# Target executable will be placed in the 'bin' directory
BIN_DIR = bin
TARGET = $(BIN_DIR)/blink_db

# Source files are located in the 'src' directory
SRC_DIR = src

# --- File Lists ---
# Find all .cpp files in the source directory
SRCS = $(wildcard $(SRC_DIR)/*.cpp)

# Create a list of object files (.o) in the current directory
# Example: src/main.cpp becomes main.o
OBJS = $(patsubst $(SRC_DIR)/%.cpp,%.o,$(SRCS))

# List all header files in the source directory for dependency tracking
DEPS = $(wildcard $(SRC_DIR)/*.h)

# --- Targets ---

# Default target: build the executable
all: $(TARGET)

# Link the executable from the object files
$(TARGET): $(OBJS)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "✅ BlinkDB executable created successfully at $(TARGET)"

# Generic rule to compile source files from src/ into object files in the root directory
# Example: Compiles src/main.cpp into main.o
%.o: $(SRC_DIR)/%.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Run the database REPL
run: all
	./$(TARGET)

# Clean up build artifacts (all .o files and the executable in bin/)
clean:
	rm -f *.o
	rm -f -r $(BIN_DIR)
	@echo "🧹 Cleaned up build artifacts."

# Phony targets are not files
.PHONY: all run clean