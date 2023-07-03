# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum, auto


class MeasurementType(Enum):
    WALLCLOCK = auto()
    MEMORY = auto()

    def __str__(self) -> str:
        return self.name.lower()


class Measurement:
    def __init__(self, type: MeasurementType, value: float) -> None:
        self.type = type
        self.value = value

    def __str__(self) -> str:
        return f"{self.value:>11.3f}({self.type})"
