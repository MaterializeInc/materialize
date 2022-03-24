# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
import time
from typing import Callable, List, Optional, Union

from materialize.feature_benchmark.executor import Executor

Timestamp = float


class MeasurementSource:
    def __init__(self) -> None:
        self._executor: Optional[Executor] = None

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> Union[None, Timestamp, List[Timestamp]]:
        assert False


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

    def __init__(self, td_str: str) -> None:
        self._td_str = td_str
        self._executor: Optional[Executor] = None

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> List[Timestamp]:
        assert not (executor is None and self._executor is None)
        assert not (executor is not None and self._executor is not None)

        td_output = getattr((executor if executor else self._executor), "Td")(
            self._td_str
        )

        lines = td_output.splitlines()
        lines = [l for l in lines if l]

        timestamps = []
        for marker in ["A", "B"]:
            timestamp = self._get_time_for_marker(lines, marker)
            if timestamp is not None:
                timestamps.append(timestamp)

        return timestamps

    def _get_time_for_marker(
        self, lines: List[str], marker: str
    ) -> Union[None, Timestamp]:
        matched_line_id = None
        for id, line in enumerate(lines):
            if f"/* {marker} */" in line:
                if "rows match" in lines[id + 1]:
                    matched_line_id = id + 1
                elif "rows match" in lines[id + 2]:
                    assert "rows didn't match" in lines[id + 1]
                    matched_line_id = id + 2
                else:
                    assert False

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
        executor: Optional[Executor] = None,
    ) -> Timestamp:
        e = executor if executor else self._executor
        assert e is not None
        e.Lambda(self._lambda)
        return time.time()
