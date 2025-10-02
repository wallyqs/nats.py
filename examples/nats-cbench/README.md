# NATS C Benchmark Tool

A high-performance benchmark tool written in C using the NATS C client directly.

## Building

```bash
make
```

This will build the `nats-cbench` executable. The Makefile automatically links against the NATS C library built in `../../nats.c/build/lib`.

## Usage

```bash
./nats-cbench pub <subject> [options]
```

### Options

- `--server, -s <url>` - NATS server URL (default: nats://localhost:4222)
- `--size <size>` - Message size in bytes (default: 128)
- `--msgs <count>` - Total number of messages to publish (default: 100000)
- `--clients <count>` - Number of concurrent clients (default: 1)
- `--no-progress` - Disable progress output
- `--help, -h` - Show help

### Examples

```bash
# High-performance test with zero-byte messages
./nats-cbench pub foo --size 0 --msgs 1000000 --clients 1

# Multi-client test
./nats-cbench pub test.subject --size 1024 --msgs 100000 --clients 3

# Large message test
./nats-cbench pub benchmark --size 10240 --msgs 10000 --clients 2
```

## Output Format

The tool outputs results in the same format as `nats bench`:

```
Pub stats: 647409 msgs/sec ~ 0B/sec
 [1] 6784704 msgs/sec ~ 0B/sec (10000 msgs)

Completed in 0.02s
```

For multi-client tests, it shows per-client statistics and summary stats:

```
Pub stats: 14902 msgs/sec ~ 5.50 GB/sec
 [1] 4989 msgs/sec ~ 1.84 GB/sec (33333 msgs)
 [2] 4967 msgs/sec ~ 1.83 GB/sec (33334 msgs)
 [3] 4967 msgs/sec ~ 1.83 GB/sec (33333 msgs)
 min 4967 | avg 4974 | max 4989 | stddev 10 msgs

Completed in 6.71s
```

## Performance

This C implementation provides maximum performance by:

- Using the NATS C client directly (no language bindings overhead)
- Compiled with `-O3` optimization
- Multi-threaded with pthread for concurrent clients
- Minimal memory allocations during benchmarking
- Direct system calls for timing

This tool is ideal for:
- Performance baseline measurements
- Comparing against other NATS client implementations
- High-throughput testing scenarios
- Validating NATS server performance

## Dependencies

- NATS C client library (built from `../../nats.c`)
- pthread (for multi-client support)
- Standard C math library