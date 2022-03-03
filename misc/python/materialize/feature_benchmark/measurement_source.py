# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from typing import Iterator, List, Optional

from materialize.feature_benchmark.executor import Executor


class MeasurementSource:
    def __init__(self) -> None:
        self._data: List[float] = []
        self._executor: Optional[Executor] = None

    def __iter__(self) -> Iterator[float]:
        self._i = 0
        return self

    def __next__(self) -> float:
        if self._i < len(self._data):
            data = self._data[self._i]
            assert data is not None
            self._i += 1
            return data
        else:
            raise StopIteration

    def __call__(self, executor: Executor) -> "MeasurementSource":
        self._executor = executor
        return self

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> Optional[float]:
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

    def __iter__(self) -> Iterator[float]:
        return self

    def __next__(self) -> float:
        val = self.run()
        assert val is not None
        return val

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> Optional[float]:
        assert not (executor is None and self._executor is None)
        assert not (executor is not None and self._executor is not None)

        td_output = getattr((executor if executor else self._executor), "Td")(
            self._td_str
        )

        lines = td_output.splitlines()
        lines = [l for l in lines if l]

        start_time = self._get_time_for_marker(lines, "A")
        end_time = self._get_time_for_marker(lines, "B")
        diff = end_time - start_time
        return diff

    def _get_time_for_marker(self, lines: List[str], marker: str) -> float:
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

        assert matched_line_id, f"Unable to find marker /* {marker} */"
        matched_line = lines[matched_line_id]
        regex = re.search("at ts ([0-9.]+)", matched_line)
        assert regex, f"'at ts' string not found on line '{matched_line}'"
        return float(regex.group(1))


class FileMeasurementSource(MeasurementSource):
    """Read measurements from a file. Used for testing purposes"""

    def __init__(self, file_name: str) -> None:
        file_name = file_name
        file = open(file_name)
        lines = file.readlines()
        self._data = [float(line.strip("\n")) for line in lines]


class AbsoluteFactorMeasurementSource(MeasurementSource):
    """Report measurements that are offset from some other set of measurements by
    a constant factor. Used for testing purposes.
    """

    def __init__(self, source: MeasurementSource, factor: float) -> None:
        self._data = [d + factor for d in source]


class RelativeFactorMeasurementSource(MeasurementSource):
    """Report measurements that are offset from some other set of measurements by
    some relative factor. Used for testing purposes.
    """

    def __init__(self, source: MeasurementSource, factor: float) -> None:
        self._data = [d * factor for d in source]


class Dummy(MeasurementSource):
    """Returns a constant stream of 1s"""

    def __iter__(self) -> Iterator[float]:
        return self

    def __next__(self) -> float:
        return 1.0


class Assert(MeasurementSource):
    """Asserts when iterated over"""

    def __iter__(self) -> Iterator[float]:
        return self

    def __next__(self) -> float:
        assert False
