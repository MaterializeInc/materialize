# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re

from materialize.output_consistency.output.format_constants import (
    COMMENT_PREFIX,
    OUTPUT_LINE_SEPARATOR_MAJOR,
    OUTPUT_LINE_SEPARATOR_MINOR,
)


class BaseOutputPrinter:
    def print_empty_line(self) -> None:
        print()

    def print_major_separator(self) -> None:
        self._print_text(OUTPUT_LINE_SEPARATOR_MAJOR)

    def print_minor_separator(self) -> None:
        self._print_text(OUTPUT_LINE_SEPARATOR_MINOR)

    def _print_executable(self, sql: str) -> None:
        print(sql)

    def _print_text(self, text: str) -> None:
        adjusted_text = re.sub("\n", f"\n{COMMENT_PREFIX}", text)
        adjusted_text = f"{COMMENT_PREFIX}{adjusted_text}"

        print(adjusted_text)
