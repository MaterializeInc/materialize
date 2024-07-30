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


class WallclockUnit(Enum):
    SECONDS = auto()
    NANOSECONDS = auto()


@dataclass
class WallclockMeasurement:
    duration: float
    unit: WallclockUnit

    def is_equal_or_after(self, other: WallclockMeasurement) -> bool:
        assert self.unit == other.unit
        return self.duration >= other.duration


class MeasurementType(Enum):
    WALLCLOCK = auto()
    MEMORY_MZ = auto()
    MEMORY_CLUSTERD = auto()
    MESSAGES = auto()

    def __str__(self) -> str:
        return self.name.lower()


@dataclass
class Measurement:
    type: MeasurementType
    value: float
    notes: str | None = None

    def __str__(self) -> str:
        return f"{self.value:>11.3f}({self.type})"
