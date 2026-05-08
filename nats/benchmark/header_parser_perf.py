"""Microbench for NATS header parsing.

Compares the legacy `email.parser.BytesParser`-based path, the byte-level
`Client._parse_header_lines` (introduced for #491 / #924), and the optional
`fast_mail_parser` opt-in path.

Run with:

    uv run python nats/benchmark/header_parser_perf.py

`fast_mail_parser` is only included if installed (`pip install fast-mail-parser`).
"""

import sys
import timeit
from email.parser import BytesParser

from nats.aio.client import Client as NATS

try:
    from fast_mail_parser import parse_email
except ImportError:
    parse_email = None

_CRLF_ = b"\r\n"


_legacy_hdr_parser = BytesParser()


def legacy_bytes_parser(raw: bytes) -> dict:
    # Mirrors the pre-#924 path: BytesParser.parsebytes + strip().
    return {k.strip(): v.strip() for k, v in _legacy_hdr_parser.parsebytes(raw).items()}


def new_byte_parser(raw: bytes) -> dict:
    return NATS._parse_header_lines(raw)


def fast_parser(raw: bytes) -> dict:
    return parse_email(raw).headers


SMALL = b"foo: bar\r\nNats-Msg-Id: ABC123\r\n\r\n"
TYPICAL = (
    b"Nats-Msg-Id: 01HXYZ-ABC-DEF-1234\r\n"
    b"Nats-Stream: ORDERS\r\n"
    b"Nats-Sequence: 12345\r\n"
    b"Content-Type: application/json\r\n"
    b"X-Trace-Id: 7d4f9a8b6c5e4d3f2a1b0c9d8e7f6a5b\r\n"
    b"\r\n"
)
LARGE = b"".join(b"X-Header-%d: value-%d\r\n" % (i, i) for i in range(20)) + b"\r\n"
NON_ASCII = b"Nats-Msg-Id: ABC\xc2\xa3DEF\r\nName: Sv\xc3\xa5gertorp\r\nfoo: bar\r\n\r\n"

CASES = [
    ("small (2 hdrs)", SMALL),
    ("typical (5 hdrs)", TYPICAL),
    ("large (20 hdrs)", LARGE),
    ("non-ascii (3 hdrs)", NON_ASCII),
]


def fmt(ops_per_sec: float) -> str:
    if ops_per_sec >= 1e6:
        return f"{ops_per_sec / 1e6:7.2f} M ops/s"
    return f"{ops_per_sec / 1e3:7.1f} K ops/s"


def main(iterations: int = 200_000) -> None:
    print(f"Python {sys.version.split()[0]}")
    print(f"fast_mail_parser available: {parse_email is not None}")
    print(f"iterations per case: {iterations}")
    print()

    parsers = [
        ("BytesParser (pre-#924)", legacy_bytes_parser),
        ("_parse_header_lines (new)", new_byte_parser),
    ]
    if parse_email is not None:
        parsers.append(("fast_mail_parser", fast_parser))

    width = max(len(name) for name, _ in parsers)
    baseline_fn = new_byte_parser

    for case_name, raw in CASES:
        print(f"--- {case_name} ({len(raw)} bytes) ---")
        baseline = baseline_fn(raw)
        for name, fn in parsers:
            try:
                result = fn(raw)
            except Exception as e:
                print(f"  {name:<{width}}    ERROR {type(e).__name__}: {e}")
                continue
            note = "ok" if len(result) == len(baseline) else f"DIFF len={len(result)}/{len(baseline)}"
            elapsed = timeit.timeit(lambda fn=fn, raw=raw: fn(raw), number=iterations)
            print(f"  {name:<{width}}  {fmt(iterations / elapsed)}  [{note}]")
        print()


if __name__ == "__main__":
    main()
