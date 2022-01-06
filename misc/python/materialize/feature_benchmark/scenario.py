# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.feature_benchmark.measurement_source import (
    Assert,
    Dummy,
    MeasurementSource,
)


class Scenario:
    __name__: str

    SHARED: Optional[MeasurementSource] = None
    INIT: Optional[MeasurementSource] = None

    BEFORE: MeasurementSource = Dummy()
    BENCHMARK: MeasurementSource = Assert()
