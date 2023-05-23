# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.output.format_constants import (
    COMMENT_PREFIX,
    CONTENT_SEPARATOR_1,
)


class OutputPrinter:
    def print_sql(self, sql: str) -> None:
        self.__print_executable(sql)
        self.print_empty_line()

    def print_non_executable_sql(self, sql: str) -> None:
        self.__print_text(sql)

    def print_info(self, text: str) -> None:
        self.__print_text(text)

    def print_error(self, error_message: str) -> None:
        self.__print_text(error_message)

    def print_separator(self) -> None:
        self.__print_text(CONTENT_SEPARATOR_1)

    def print_test_summary(self, summary: ConsistencyTestSummary) -> None:
        self.__print_text(f"Test summary: {summary}")

    def print_status(self, status_message: str) -> None:
        self.__print_text(status_message)

    def print_config(self, config: ConsistencyTestConfiguration) -> None:
        config_properties = vars(config)
        self.__print_text("Configuration is:")
        self.__print_text(
            "\n".join(f"  {item[0]} = {item[1]}" for item in config_properties.items())
        )
        self.print_empty_line()

    def print_empty_line(self) -> None:
        print()

    def __print_executable(self, sql: str) -> None:
        print(sql)

    def __print_text(self, text: str) -> None:
        adjusted_text = re.sub("\n", f"\n{COMMENT_PREFIX} ", text)
        adjusted_text = f"{COMMENT_PREFIX} {adjusted_text}"

        print(adjusted_text)
