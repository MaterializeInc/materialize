# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass
from math import floor, sqrt

from materialize.scalability.workload.workload import Workload


@dataclass
class BenchmarkConfiguration:
    workload_classes: list[type[Workload]]
    exponent_base: float
    min_concurrency: int
    max_concurrency: int
    count: int
    verbose: bool

    def get_count_for_concurrency(self, concurrency: int) -> int:
        return floor(self.count * sqrt(concurrency))
