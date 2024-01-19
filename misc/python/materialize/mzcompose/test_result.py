# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class TestResult:
    duration: float
    errors: list[TestFailureDetails]

    def is_successful(self) -> bool:
        return len(self.errors) == 0


@dataclass
class TestFailureDetails:
    message: str
    details: str | None
    location: str | None

    def get_error_file(self) -> str | None:
        if self.location is None:
            return None

        file_name = self.location
        file_name = re.sub(r":\d+", "", file_name)

        if "/" in file_name:
            file_name = file_name[file_name.rindex("/") + 1 :]

        return file_name
