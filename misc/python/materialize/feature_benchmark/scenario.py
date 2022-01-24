# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Union

from materialize.feature_benchmark.measurement_source import (
    Assert,
    Dummy,
    MeasurementSource,
)


class RootScenario:
    __name__: str

    SHARED: Optional[Union[MeasurementSource, List[MeasurementSource]]] = None
    INIT: Optional[MeasurementSource] = None

    BEFORE: MeasurementSource = Dummy()
    BENCHMARK: MeasurementSource = Assert()


# Used for benchmarks that are expected to run by default, e.g. in CI
class Scenario(RootScenario):
    pass


# Used for scenarios that need to be explicitly run from the command line using --root-scenario ScenarioBig
class ScenarioBig(RootScenario):
    pass
