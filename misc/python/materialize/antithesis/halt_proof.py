# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Read complete process halt records from a Kubernetes termination message."""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import UTC, datetime

MAX_BYTES = 4096
_DECIMAL = r"(?:0|[1-9][0-9]*)"
_FRAME = re.compile(
    rf"MZHALT1 ({_DECIMAL}) 166 ({_DECIMAL}) ({_DECIMAL}) ({_DECIMAL}) "
    r"([0-9a-f]+) ([0-9a-f]+) END\n"
)


@dataclass(frozen=True)
class HaltProof:
    recorded_at: str
    location: str
    message: str
    recorded_times: tuple[str, ...]


def timestamp_interval(value: str | None) -> tuple[int, int] | None:
    """Return the timestamp's precision interval in nanoseconds since the epoch."""
    if value is None:
        return None
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if match is None:
        return None
    try:
        seconds = datetime.fromisoformat(match[1] + match[3]).astimezone(UTC)
    except ValueError:
        return None
    fraction = match[2] or ""
    start = calendar.timegm(seconds.utctimetuple()) * 1_000_000_000
    start += int(fraction.ljust(9, "0"))
    return start, start + 10 ** (9 - len(fraction))


def validate_lifetime(
    proof: HaltProof, started: str | None, finished: str | None
) -> None:
    """Require the halt record to lie within its container lifetime."""
    start, end = timestamp_interval(started), timestamp_interval(finished)
    if start is None or end is None or start[0] >= end[1]:
        raise ValueError("halt record is outside its container lifetime")
    for value in proof.recorded_times:
        recorded = timestamp_interval(value)
        if recorded is None or recorded[0] < start[0] or recorded[1] > end[1]:
            raise ValueError("halt record is outside its container lifetime")


def parse(text: str) -> HaltProof:
    """Reject truncated, malformed, or conflicting termination records."""
    try:
        if not text or len(text.encode("ascii")) >= MAX_BYTES:
            raise ValueError("halt termination message is empty or oversized")
    except UnicodeError as error:
        raise ValueError("halt termination frame is not ASCII") from error
    records = []
    for sequence, line in enumerate(text.splitlines(keepends=True)):
        match = _FRAME.fullmatch(line)
        if match is None or int(match[1]) != sequence:
            raise ValueError("halt termination frames are incomplete or unordered")
        stamp, location_size, message_size = map(int, match.group(2, 3, 4))
        if (
            stamp >= 2**128
            or not 0 < location_size <= MAX_BYTES
            or not 0 < message_size <= MAX_BYTES
        ):
            raise ValueError("halt termination field is outside its range")
        try:
            location_bytes = bytes.fromhex(match[5])
            message_bytes = bytes.fromhex(match[6])
            if (len(location_bytes), len(message_bytes)) != (
                location_size,
                message_size,
            ):
                raise ValueError("halt termination field length differs")
            location = location_bytes.decode("utf8")
            message = message_bytes.decode("utf8")
            seconds, nanos = divmod(stamp, 1_000_000_000)
            instant = datetime.fromtimestamp(seconds, UTC)
        except (ValueError, OverflowError, OSError) as error:
            raise ValueError("halt termination field cannot be decoded") from error
        recorded_at = instant.strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"
        if records and (location, message) != records[0][1:]:
            raise ValueError("halt termination records conflict")
        records.append((recorded_at, location, message))
    if not records:
        raise ValueError("halt termination frame is missing")
    recorded_at, location, message = records[0]
    return HaltProof(
        recorded_at, location, message, tuple(record[0] for record in records)
    )
