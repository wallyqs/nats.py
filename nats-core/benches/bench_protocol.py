"""Benchmarks for NATS protocol encoding and parsing operations."""

import pytest
from nats.client.protocol import command
from nats.client.protocol.message import parse_headers


def test_bench_encode_connect(benchmark):
    """Benchmark encoding CONNECT command with basic connection info."""
    connect_info = {
        "verbose": False,
        "pedantic": False,
        "tls_required": False,
        "name": "test-client",
        "lang": "python",
        "version": "1.0.0",
        "protocol": 1,
    }

    benchmark(command.encode_connect, connect_info)


@pytest.mark.parametrize("size", [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192])
def test_bench_encode_pub_with_payload(benchmark, size):
    """Benchmark encoding PUB command with various payload sizes."""
    subject = "test.subject"
    payload = b"x" * size

    benchmark(command.encode_pub, subject, payload)


def test_bench_encode_pub_with_reply(benchmark):
    """Benchmark encoding PUB command with reply subject."""
    subject = "test.subject"
    payload = b"hello world"
    reply = "reply.subject"

    benchmark(command.encode_pub, subject, payload, reply=reply)


def test_bench_encode_hpub_single_header(benchmark):
    """Benchmark encoding HPUB command with single header."""
    subject = "test.subject"
    payload = b"hello world"
    headers = {"X-Custom": "value"}

    benchmark(command.encode_hpub, subject, payload, headers=headers)


def test_bench_encode_hpub_multiple_headers(benchmark):
    """Benchmark encoding HPUB command with multiple headers."""
    subject = "test.subject"
    payload = b"hello world"
    headers = {
        "X-Custom-1": "value1",
        "X-Custom-2": "value2",
        "X-Custom-3": "value3",
        "Content-Type": "application/json",
        "X-Request-ID": "12345-67890-abcdef",
    }

    benchmark(command.encode_hpub, subject, payload, headers=headers)


def test_bench_encode_hpub_multivalue_headers(benchmark):
    """Benchmark encoding HPUB command with multi-value headers."""
    subject = "test.subject"
    payload = b"hello world"
    headers = {
        "X-Custom": ["value1", "value2", "value3"],
        "X-Tags": ["tag1", "tag2", "tag3", "tag4"],
    }

    benchmark(command.encode_hpub, subject, payload, headers=headers)


def test_bench_encode_hpub_with_reply(benchmark):
    """Benchmark encoding HPUB command with reply subject and headers."""
    subject = "test.subject"
    payload = b"hello world"
    reply = "reply.subject"
    headers = {"X-Custom": "value"}

    benchmark(command.encode_hpub, subject, payload, reply=reply, headers=headers)


def test_bench_encode_sub(benchmark):
    """Benchmark encoding SUB command."""
    subject = "test.subject"
    sid = "1"

    benchmark(command.encode_sub, subject, sid)


def test_bench_encode_sub_with_queue(benchmark):
    """Benchmark encoding SUB command with queue group."""
    subject = "test.subject"
    sid = "1"
    queue = "test-queue"

    benchmark(command.encode_sub, subject, sid, queue)


def test_bench_encode_unsub(benchmark):
    """Benchmark encoding UNSUB command."""
    sid = "1"

    benchmark(command.encode_unsub, sid)


def test_bench_encode_unsub_with_max(benchmark):
    """Benchmark encoding UNSUB command with max_msgs."""
    sid = "1"
    max_msgs = 100

    benchmark(command.encode_unsub, sid, max_msgs)


def test_bench_encode_ping(benchmark):
    """Benchmark encoding PING command."""
    benchmark(command.encode_ping)


def test_bench_encode_pong(benchmark):
    """Benchmark encoding PONG command."""
    benchmark(command.encode_pong)


# --- Header parsing benchmarks ---


def test_bench_parse_headers_status_only(benchmark):
    """Benchmark parsing status-only headers (fast path).

    This is the most common pattern in JetStream for heartbeats,
    no-messages responses, and timeouts.
    """
    data = b"NATS/1.0 100\r\n\r\n"
    benchmark(parse_headers, data)


def test_bench_parse_headers_status_with_description(benchmark):
    """Benchmark parsing status headers with description (fast path)."""
    data = b"NATS/1.0 503 No Responders\r\n\r\n"
    benchmark(parse_headers, data)


def test_bench_parse_headers_no_status(benchmark):
    """Benchmark parsing headers without status line."""
    data = b"NATS/1.0\r\n\r\n"
    benchmark(parse_headers, data)


def test_bench_parse_headers_single(benchmark):
    """Benchmark parsing a single header."""
    data = b"NATS/1.0\r\nX-Custom: value\r\n\r\n"
    benchmark(parse_headers, data)


def test_bench_parse_headers_multiple(benchmark):
    """Benchmark parsing multiple headers."""
    data = b"NATS/1.0\r\nX-Custom-1: value1\r\nX-Custom-2: value2\r\nX-Custom-3: value3\r\nContent-Type: application/json\r\nX-Request-ID: 12345-67890-abcdef\r\n\r\n"
    benchmark(parse_headers, data)


def test_bench_parse_headers_multivalue(benchmark):
    """Benchmark parsing multi-value headers."""
    data = b"NATS/1.0\r\nX-Custom: value1\r\nX-Custom: value2\r\nX-Custom: value3\r\nX-Tags: tag1\r\nX-Tags: tag2\r\nX-Tags: tag3\r\nX-Tags: tag4\r\n\r\n"
    benchmark(parse_headers, data)


def test_bench_parse_headers_jetstream_heartbeat(benchmark):
    """Benchmark parsing JetStream idle heartbeat with status and headers.

    This pattern is common in JetStream consumer heartbeat messages.
    """
    data = b"NATS/1.0 100\r\nNats-Last-Consumer: 1016\r\nNats-Last-Stream: 1024\r\n\r\n"
    benchmark(parse_headers, data)
