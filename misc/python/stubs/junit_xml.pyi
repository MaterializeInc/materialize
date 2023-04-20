# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, TextIO

class TestCase:
    def __init__(
        self,
        name: str,
        classname: Optional[str] = ...,
        elapsed_sec: Optional[float] = ...,
        allow_multiple_subelements: bool = ...,
    ): ...
    def add_error_info(self, message: str, output: Optional[str] = None) -> None: ...
    def add_failure_info(self, message: str, output: Optional[str] = None) -> None: ...

class TestSuite:
    test_cases: List[TestCase]
    def __init__(self, name: str, test_cases: Optional[List[TestCase]] = None): ...

def to_xml_report_string(test_suites: List[TestSuite]) -> str: ...
def to_xml_report_file(f: TextIO, test_suites: List[TestSuite]) -> None: ...
