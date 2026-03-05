"""Tests for Base._parse_utc_iso timestamp parsing."""

import datetime

from nats.js.api import Base


class TestParseUtcIso:
    """Test _parse_utc_iso handles variable fractional second precision from Go."""

    def test_9_digit_nanoseconds(self):
        """Go's time.RFC3339Nano produces up to 9 fractional digits."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.203730123Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203730, tzinfo=datetime.timezone.utc
        )

    def test_6_digit_microseconds(self):
        """Standard microsecond precision (6 digits) should work as-is."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.203730Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203730, tzinfo=datetime.timezone.utc
        )

    def test_5_digit_fractional_seconds(self):
        """5-digit fractional seconds (the case reported in PR #796)."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.20373Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203730, tzinfo=datetime.timezone.utc
        )

    def test_4_digit_fractional_seconds(self):
        """4-digit fractional seconds should be padded to 6 digits."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.2037Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203700, tzinfo=datetime.timezone.utc
        )

    def test_3_digit_milliseconds(self):
        """3-digit fractional seconds (millisecond precision)."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.203Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203000, tzinfo=datetime.timezone.utc
        )

    def test_2_digit_fractional_seconds(self):
        """2-digit fractional seconds should be padded to 6 digits."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.20Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 200000, tzinfo=datetime.timezone.utc
        )

    def test_1_digit_fractional_seconds(self):
        """1-digit fractional seconds should be padded to 6 digits."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.2Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 200000, tzinfo=datetime.timezone.utc
        )

    def test_7_digit_fractional_seconds(self):
        """7-digit fractional seconds should be truncated to 6."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.2037301Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203730, tzinfo=datetime.timezone.utc
        )

    def test_utc_offset_instead_of_z(self):
        """Timestamp with explicit +00:00 offset instead of Z."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.20373+00:00")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 203730, tzinfo=datetime.timezone.utc
        )

    def test_result_is_utc(self):
        """Result should always be in UTC timezone."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.203730Z")
        assert result.tzinfo == datetime.timezone.utc

    def test_zero_fractional_seconds(self):
        """Fractional seconds of all zeros."""
        result = Base._parse_utc_iso("2025-12-15T16:55:37.000000Z")
        assert result == datetime.datetime(
            2025, 12, 15, 16, 55, 37, 0, tzinfo=datetime.timezone.utc
        )
