#!/usr/bin/env python3
"""
Test script to showcase Cython publish_many performance.
"""

import time
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from nats.metal import NATS

def test_publish_many():
    print("Testing Cython publish_many performance...")
    
    # Connect
    nc = NATS.connect("localhost:4222")
    
    # Test parameters
    subject = "test.batch"
    payload = b"x" * 1024  # 1KB payload
    msg_count = 100000
    
    print(f"Publishing {msg_count:,} messages of {len(payload)} bytes each...")
    
    # Test individual publishes
    start = time.time()
    for i in range(msg_count):
        nc.publish(subject, payload)
    end = time.time()
    
    individual_time = end - start
    individual_rate = msg_count / individual_time
    
    print(f"Individual publish: {individual_rate:,.0f} msgs/sec in {individual_time:.3f}s")
    
    # Test batch publish
    start = time.time()
    nc.publish_many(subject, payload, msg_count)
    end = time.time()
    
    batch_time = end - start
    batch_rate = msg_count / batch_time
    
    print(f"Batch publish_many: {batch_rate:,.0f} msgs/sec in {batch_time:.3f}s")
    print(f"Speedup: {batch_rate / individual_rate:.1f}x faster")
    
    nc.close()

if __name__ == "__main__":
    test_publish_many()