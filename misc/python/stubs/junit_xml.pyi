# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import TextIO

class TestCase:
    def __init__(
        self,
        name: str,
        classname: str | None = ...,
        elapsed_sec: float | None = ...,
        allow_multiple_subelements: bool = ...,
    ): ...
    def add_skipped_info(self, message: str, output: str | None = None) -> None: ...
    def add_error_info(self, message: str, output: str | None = None) -> None: ...
    def add_failure_info(self, message: str, output: str | None = None) -> None: ...

class TestSuite:
    test_cases: list[TestCase]
    def __init__(self, name: str, test_cases: list[TestCase] | None = None): ...

def to_xml_report_string(test_suites: list[TestSuite]) -> str: ...
def to_xml_report_file(f: TextIO, test_suites: list[TestSuite]) -> None: ...
