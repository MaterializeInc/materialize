# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
import textwrap
import time
from collections.abc import Callable

from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.measurement import (
    MeasurementUnit,
    WallclockDuration,
)


class MeasurementSource:
    def __init__(self) -> None:
        self._executor: Executor | None = None

    def run(
        self,
        executor: Executor | None = None,
    ) -> list[WallclockDuration]:
        raise NotImplementedError


class Td(MeasurementSource):
    """Use testdrive to run the queries under benchmark and extract the timing information
    out of the testdrive output. The output looks like this:

    > /* A */ CREATE ...
    rows match; continuing at ts 1639561166.4809854
    > /* B */ SELECT ...
    rows didn't match; sleeping to see if dataflow catches up
    rows match; continuing at ts 1639561175.6951854

    So we fish for the /* A */ and /* B */ markers and the timestamps reported for each

    """

    def __init__(self, td_str: str, dedent: bool = True) -> None:
        self._td_str = textwrap.dedent(td_str) if dedent else td_str
        self._executor: Executor | None = None

    def run(
        self,
        executor: Executor | None = None,
    ) -> list[WallclockDuration]:
        assert not (executor is not None and self._executor is not None)
        executor = executor or self._executor
        assert executor

        # Print each query once so that it is easier to reproduce regressions
        # based on just the logs from CI
        if executor.add_known_fragment(self._td_str):
            print(self._td_str)

        td_output = executor.Td(self._td_str)

        lines = td_output.splitlines()
        lines = [l for l in lines if l]

        timestamps = []
        for marker in ["A", "B"]:
            timestamp = self._get_time_for_marker(lines, marker)
            if timestamp is not None:
                timestamps.append(WallclockDuration(timestamp, MeasurementUnit.SECONDS))

        return timestamps

    def _get_time_for_marker(self, lines: list[str], marker: str) -> None | float:
        matched_line_id = None
        for id, line in enumerate(lines):
            if f"/* {marker} */" in line:
                if "rows match" in lines[id + 1]:
                    matched_line_id = id + 1
                elif "rows match" in lines[id + 2]:
                    assert "rows didn't match" in lines[id + 1]
                    matched_line_id = id + 2
                else:
                    raise RuntimeError("row match not found")

        if not matched_line_id:
            # Marker /* ... */ not found
            return None

        matched_line = lines[matched_line_id]
        regex = re.search("at ts ([0-9.]+)", matched_line)
        assert regex, f"'at ts' string not found on line '{matched_line}'"
        return float(regex.group(1))


class Lambda(MeasurementSource):
    # Execute a lambda, such as Mz restart, within a benchmark() block and record the end timestamp
    def __init__(self, _lambda: Callable) -> None:
        self._lambda = _lambda

    def run(
        self,
        executor: Executor | None = None,
    ) -> list[WallclockDuration]:
        e = executor or self._executor
        assert e is not None
        e.Lambda(self._lambda)
        return [WallclockDuration(time.time(), MeasurementUnit.SECONDS)]
