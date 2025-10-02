#!/usr/bin/env python3
"""
NATS Metal Benchmark Tool

Benchmark tool modeled after the `nats bench` utility for testing publish performance.
"""

import argparse
import sys
import time
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from nats.metal import NATS, NATSError

@dataclass
class BenchResult:
    client_id: int
    messages_sent: int
    total_time: float
    bytes_sent: int
    msgs_per_sec: float
    bytes_per_sec: float
    errors: int

class ProgressBar:
    def __init__(self, total: int, width: int = 50):
        self.total = total
        self.width = width
        self.current = 0
        self.start_time = time.time()
        
    def update(self, count: int):
        self.current = count
        elapsed = time.time() - self.start_time
        rate = count / elapsed if elapsed > 0 else 0
        
        progress = count / self.total
        filled = int(self.width * progress)
        bar = '█' * filled + '░' * (self.width - filled)
        
        percent = progress * 100
        
        sys.stdout.write(f'\r[{bar}] {percent:5.1f}% {count:,}/{self.total:,} msgs ({rate:,.0f} msg/s)')
        sys.stdout.flush()
        
    def finish(self):
        self.update(self.total)
        sys.stdout.write('\n')
        sys.stdout.flush()

def parse_size(size_str: str) -> int:
    """Parse size string like '1KB', '10MB', etc."""
    size_str = size_str.upper().strip()
    
    multipliers = {
        'B': 1,
        'K': 1024, 'KB': 1024,
        'M': 1024*1024, 'MB': 1024*1024,
        'G': 1024*1024*1024, 'GB': 1024*1024*1024
    }
    
    # Extract number and unit
    i = 0
    while i < len(size_str) and (size_str[i].isdigit() or size_str[i] == '.'):
        i += 1
    
    if i == 0:
        raise ValueError(f"Invalid size format: {size_str}")
    
    number = float(size_str[:i])
    unit = size_str[i:] if i < len(size_str) else 'B'
    
    if unit not in multipliers:
        raise ValueError(f"Unknown size unit: {unit}")
    
    return int(number * multipliers[unit])

def format_bytes(bytes_count: int) -> str:
    """Format bytes in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_count < 1024:
            return f"{bytes_count:.1f}{unit}"
        bytes_count /= 1024
    return f"{bytes_count:.1f}TB"

def format_number(num: int) -> str:
    """Format number with commas."""
    return f"{num:,}"

def publish_worker(client_id: int, subject: str, payload: bytes, num_msgs: int, 
                  server_url: str, progress_callback: Optional[callable] = None) -> BenchResult:
    """Worker function for publishing messages in a thread."""
    try:
        # Connect to NATS
        nc = NATS.connect(server_url)
        
        messages_sent = 0
        errors = 0
        start_time = time.time()
        last_reported = 0
        
        for i in range(num_msgs):
            try:
                nc.publish(subject, payload)
                messages_sent += 1
                
                # Report incremental progress every 100 messages
                if progress_callback and messages_sent - last_reported >= 100:
                    increment = messages_sent - last_reported
                    progress_callback(increment)
                    last_reported = messages_sent
                    
            except NATSError as e:
                errors += 1
                print(f"Client {client_id}: Publish error: {e}", file=sys.stderr)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Final progress update for remaining messages
        if progress_callback and messages_sent > last_reported:
            remaining = messages_sent - last_reported
            progress_callback(remaining)
        
        # Close connection
        nc.close()
        
        bytes_sent = messages_sent * len(payload)
        msgs_per_sec = messages_sent / total_time if total_time > 0 else 0
        bytes_per_sec = bytes_sent / total_time if total_time > 0 else 0
        
        return BenchResult(
            client_id=client_id,
            messages_sent=messages_sent,
            total_time=total_time,
            bytes_sent=bytes_sent,
            msgs_per_sec=msgs_per_sec,
            bytes_per_sec=bytes_per_sec,
            errors=errors
        )
        
    except Exception as e:
        print(f"Client {client_id}: Fatal error: {e}", file=sys.stderr)
        return BenchResult(
            client_id=client_id,
            messages_sent=0,
            total_time=0,
            bytes_sent=0,
            msgs_per_sec=0,
            bytes_per_sec=0,
            errors=1
        )

def run_publish_benchmark(args) -> List[BenchResult]:
    """Run the publish benchmark."""
    # Parse message size
    try:
        msg_size = parse_size(args.size)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return []
    
    # Create payload
    payload = b'x' * msg_size
    
    # Divide messages across clients
    msgs_per_client = args.msgs // args.clients
    remaining_msgs = args.msgs % args.clients
    
    # Create list of message counts for each client
    client_msg_counts = [msgs_per_client] * args.clients
    for i in range(remaining_msgs):
        client_msg_counts[i] += 1
    
    print(f"Running publish benchmark:")
    print(f"  Subject: {args.subject}")
    print(f"  Clients: {format_number(args.clients)}")
    print(f"  Total messages: {format_number(args.msgs)}")
    print(f"  Messages per client: {format_number(msgs_per_client)} ({remaining_msgs} clients get +1)" if remaining_msgs > 0 else f"  Messages per client: {format_number(msgs_per_client)}")
    print(f"  Message size: {format_bytes(msg_size)}")
    print(f"  Total data: {format_bytes(msg_size * args.msgs)}")
    print()
    
    # Progress tracking
    total_msgs = args.msgs
    progress_bar = ProgressBar(total_msgs) if not args.no_progress else None
    progress_lock = threading.Lock()
    total_completed = [0]  # Use list for mutable reference
    
    def progress_callback(msgs_sent: int):
        if progress_bar:
            with progress_lock:
                total_completed[0] += msgs_sent
                progress_bar.update(total_completed[0])
    
    # Run benchmark
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        # Submit all client tasks
        future_to_client = {
            executor.submit(
                publish_worker, 
                client_id, 
                args.subject, 
                payload, 
                client_msg_counts[client_id],
                args.server,
                progress_callback
            ): client_id 
            for client_id in range(args.clients)
        }
        
        # Collect results
        for future in as_completed(future_to_client):
            result = future.result()
            results.append(result)
    
    end_time = time.time()
    
    if progress_bar:
        progress_bar.finish()
    
    # Calculate aggregate statistics
    total_time = end_time - start_time
    total_msgs_sent = sum(r.messages_sent for r in results)
    total_bytes_sent = sum(r.bytes_sent for r in results)
    total_errors = sum(r.errors for r in results)
    
    overall_msgs_per_sec = total_msgs_sent / total_time if total_time > 0 else 0
    overall_bytes_per_sec = total_bytes_sent / total_time if total_time > 0 else 0
    
    # Sort results by client_id for consistent ordering
    results.sort(key=lambda x: x.client_id)
    
    # Per-client statistics
    client_rates = [r.msgs_per_sec for r in results if r.msgs_per_sec > 0]
    
    print(f"\nPub stats: {overall_msgs_per_sec:,.0f} msgs/sec ~ {format_bytes(int(overall_bytes_per_sec))}/sec")
    
    # Print per-connection stats
    for result in results:
        client_bytes_per_sec = result.bytes_per_sec
        print(f" [{result.client_id + 1}] {result.msgs_per_sec:,.0f} msgs/sec ~ {format_bytes(int(client_bytes_per_sec))}/sec ({result.messages_sent:,} msgs)")
    
    # Print summary statistics
    if client_rates and len(client_rates) > 1:
        min_rate = min(client_rates)
        max_rate = max(client_rates)
        avg_rate = statistics.mean(client_rates)
        stddev_rate = statistics.stdev(client_rates)
        print(f" min {min_rate:,.0f} | avg {avg_rate:,.0f} | max {max_rate:,.0f} | stddev {stddev_rate:.0f} msgs")
    elif client_rates:
        print(f" {client_rates[0]:,.0f} msgs/sec (single client)")
    
    if total_errors > 0:
        print(f"\nErrors: {format_number(total_errors)}")
    
    print(f"\nCompleted in {total_time:.2f}s")
    
    return results

def main():
    parser = argparse.ArgumentParser(
        description="NATS Metal Benchmark Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 examples/bench.py pub foo --size 1KB --msgs 10000 --clients 1
  python3 examples/bench.py pub test.subject --size 396529 --msgs 100000 --clients 3
        """
    )
    
    # Global options
    parser.add_argument('--server', '-s', default='localhost:4222',
                       help='NATS server URL (default: localhost:4222)')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Publish command
    pub_parser = subparsers.add_parser('pub', help='Run publish benchmark')
    pub_parser.add_argument('subject', help='Subject to publish to')
    pub_parser.add_argument('--size', default='128', 
                           help='Message size (e.g., 128, 1KB, 10MB) (default: 128)')
    pub_parser.add_argument('--msgs', type=int, default=100000,
                           help='Total number of messages to publish (divided across clients) (default: 100000)')
    pub_parser.add_argument('--clients', type=int, default=1,
                           help='Number of concurrent clients (default: 1)')
    pub_parser.add_argument('--no-progress', action='store_true',
                           help='Disable progress bar')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == 'pub':
        try:
            results = run_publish_benchmark(args)
            if not results:
                return 1
                
        except KeyboardInterrupt:
            print("\nBenchmark interrupted by user")
            return 1
        except Exception as e:
            print(f"Benchmark failed: {e}", file=sys.stderr)
            return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
