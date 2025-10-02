# NATS Metal Python

High-performance Python NATS client using the NATS C library with Cython bindings for maximum speed.

## Overview

NATS Metal Python provides Python bindings to the NATS C client library, offering near-C performance for Python applications. The implementation includes both Cython (high-performance) and ctypes (fallback) bindings.

## Performance

NATS Metal delivers exceptional performance compared to pure Python NATS clients:

| Implementation            | Performance (msgs/sec) | Speedup    |
|---------------------------|------------------------|------------|
| **Cython + publish_many** | **4,380,200**          | **20x**    |
| **Cython**                | **2,400,559**          | **15x**    |
| **C Benchmark**           | **3,111,593**          | *baseline* |
| *ctypes (fallback)*       | *~150,000*             | *1x*       |

*Benchmark: 100,000 messages × 1KB on macOS ARM64*

## Features

- **High Performance**: Cython bindings achieve 77% of pure C performance
- **Automatic Fallback**: Uses ctypes if Cython unavailable
- **Enhanced API**: Includes flush, connection status methods
- **Compatible**: Drop-in replacement for existing NATS Python clients

## Installation

### Prerequisites

1. **Build NATS C Library**:
   ```bash
   cd nats.c
   mkdir -p build && cd build
   cmake ..
   make
   ```

2. **Install Python Dependencies**:
   ```bash
   pip install cython setuptools
   ```

### Build Cython Extension

```bash
python3 setup.py build_ext --inplace
```

## Usage

### Basic Usage

```python
from nats.metal import NATS

# Connect to NATS server
nc = NATS.connect("localhost:4222")

# Publish a message
nc.publish("test.subject", b"Hello World")

# Close connection
nc.close()
```

### Advanced Features

```python
# Check connection status
if nc.is_closed():
    print("Connection closed")

if nc.is_reconnecting():
    print("Reconnecting...")

# Flush pending messages
nc.flush()

# Flush with timeout
nc.flush_timeout(5000)  # 5 seconds
```

## Benchmarking

### Python Benchmark Tool

```bash
# High-performance benchmark
python3 examples/bench.py pub test.subject --size 1KB --msgs 100000 --clients 3

# Disable progress bar
python3 examples/bench.py pub test.subject --size 0 --msgs 1000000 --clients 1 --no-progress
```

### C Benchmark Tool

For maximum performance comparison:

```bash
cd examples/nats-cbench
make
./nats-cbench pub test.subject --size 1024 --msgs 100000 --clients 1
```

## Development

### Building from Source

```bash
git clone <repository>
cd metal-nats.py

# Build NATS C library
cd nats.c && mkdir -p build && cd build && cmake .. && make && cd ../..

# Build Cython extension
python3 setup.py build_ext --inplace

# Test installation
python3 -c "import nats.metal; print('Success')"
```

### Testing

```bash
# Basic functionality test
python3 examples/basic.py

# Performance comparison
python3 examples/bench_cython.py

# Full benchmark
python3 examples/bench.py pub test --size 1KB --msgs 10000 --clients 1
```

## Requirements

- **Python 3.6+**
- **NATS C Library** (included as submodule)
- **Cython** (for high-performance bindings)
- **GCC/Clang** (for compilation)

## License

Apache 2.0 License (same as NATS C library)

