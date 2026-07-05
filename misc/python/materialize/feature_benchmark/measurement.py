# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class MeasurementUnit(Enum):
    UNKNOWN = "?"
    SECONDS = "s"
    NANOSECONDS = "ns"
    COUNT = "#"
    MEGABYTE = "MB"

    def __str__(self):
        return str(self.value)


@dataclass
class WallclockDuration:
    duration: float
    unit: MeasurementUnit

    def is_equal_or_after(self, other: WallclockDuration) -> bool:
        assert self.unit == other.unit
        return self.duration >= other.duration


class MeasurementType(Enum):
    WALLCLOCK = auto()
    MEMORY_MZ = auto()
    MEMORY_CLUSTERD = auto()
    # Per-iteration peak RSS sampled from the cgroup memory.peak file. Reset
    # before each measurement so the value reflects the high-water mark of the
    # workload rather than steady-state.
    MEMORY_PEAK_MZ = auto()
    MEMORY_PEAK_CLUSTERD = auto()

    def __str__(self) -> str:
        # Short display names so the report's TYPE column stays narrow.
        # Existing wallclock / memory_mz / memory_clusterd labels are
        # preserved for compatibility with tooling that parses reports.
        return _DISPLAY_NAMES.get(self, self.name.lower())

    def is_amount(self) -> bool:
        return self in {
            MeasurementType.MEMORY_MZ,
            MeasurementType.MEMORY_CLUSTERD,
            MeasurementType.MEMORY_PEAK_MZ,
            MeasurementType.MEMORY_PEAK_CLUSTERD,
        }

    def is_lower_value_better(self) -> bool:
        return True


_DISPLAY_NAMES: dict[MeasurementType, str] = {
    MeasurementType.MEMORY_PEAK_MZ: "peak_mz",
    MeasurementType.MEMORY_PEAK_CLUSTERD: "peak_clusterd",
}


@dataclass
class Measurement:
    type: MeasurementType
    value: float
    unit: MeasurementUnit
    notes: str | None = None

    def __str__(self) -> str:
        return f"{self.value:>11.3f}({self.type})"
