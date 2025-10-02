# Makefile for nats-cbench

# Paths
NATS_C_DIR = ../../nats.c
NATS_BUILD_DIR = $(NATS_C_DIR)/build
NATS_LIB_DIR = $(NATS_BUILD_DIR)/lib
NATS_INCLUDE_DIR = $(NATS_C_DIR)/src

# Compiler settings
CC = gcc
CFLAGS = -Wall -Wextra -O3 -std=c99
INCLUDES = -I$(NATS_INCLUDE_DIR)
LDFLAGS = -L$(NATS_LIB_DIR) -Wl,-rpath,$(realpath $(NATS_LIB_DIR))
LIBS = -lnats -lpthread -lm

# Target
TARGET = nats-cbench
SOURCE = nats-cbench.c

# Default target
all: $(TARGET)

$(TARGET): $(SOURCE)
	@echo "Building $(TARGET)..."
	@echo "NATS C library path: $(NATS_LIB_DIR)"
	@echo "NATS include path: $(NATS_INCLUDE_DIR)"
	@if [ ! -f "$(NATS_LIB_DIR)/libnats.dylib" ] && [ ! -f "$(NATS_LIB_DIR)/libnats.so" ]; then \
		echo "Error: NATS C library not found. Please build nats.c first."; \
		echo "Run: cd $(NATS_C_DIR) && mkdir -p build && cd build && cmake .. && make"; \
		exit 1; \
	fi
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TARGET) $(SOURCE) $(LDFLAGS) $(LIBS)
	@echo "Built $(TARGET) successfully!"

clean:
	rm -f $(TARGET)

install: $(TARGET)
	cp $(TARGET) /usr/local/bin/

test: $(TARGET)
	@echo "Running basic test..."
	./$(TARGET) pub test.bench --size 10 --msgs 1000 --clients 1 --no-progress

help:
	@echo "Available targets:"
	@echo "  all      - Build the benchmark tool (default)"
	@echo "  clean    - Remove built files"
	@echo "  install  - Install to /usr/local/bin"
	@echo "  test     - Run a basic test"
	@echo "  help     - Show this help"

.PHONY: all clean install test help