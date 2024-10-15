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

    def __str__(self) -> str:
        return self.name.lower()

    def is_amount(self) -> bool:
        return self in {
            MeasurementType.MEMORY_MZ,
            MeasurementType.MEMORY_CLUSTERD,
        }

    def is_lower_value_better(self) -> bool:
        return True


@dataclass
class Measurement:
    type: MeasurementType
    value: float
    unit: MeasurementUnit
    notes: str | None = None

    def __str__(self) -> str:
        return f"{self.value:>11.3f}({self.type})"
