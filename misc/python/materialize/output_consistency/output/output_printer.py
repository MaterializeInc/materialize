# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.output_consistency.common.configuration import (
    ConsistencyTestConfiguration,
)
from materialize.output_consistency.execution.test_summary import ConsistencyTestSummary
from materialize.output_consistency.input_data.test_input_data import (
    ConsistencyTestInputData,
)
from materialize.output_consistency.output.base_output_printer import BaseOutputPrinter
from materialize.output_consistency.output.reproduction_code_printer import (
    ReproductionCodePrinter,
)
from materialize.output_consistency.validation.validation_message import ValidationError


class OutputPrinter(BaseOutputPrinter):
    def __init__(self, input_data: ConsistencyTestInputData):
        self.reproduction_code_printer = ReproductionCodePrinter(input_data)

    def print_sql(self, sql: str) -> None:
        self._print_executable(sql)
        self.print_empty_line()

    def print_non_executable_sql(self, sql: str) -> None:
        self._print_text(sql)

    def print_info(self, text: str) -> None:
        self._print_text(text)

    def print_error(self, error_message: str) -> None:
        self._print_text(error_message)

    def print_test_summary(self, summary: ConsistencyTestSummary) -> None:
        self._print_text(f"Test summary: {summary}")

    def print_status(self, status_message: str) -> None:
        self._print_text(status_message)

    def print_config(self, config: ConsistencyTestConfiguration) -> None:
        config_properties = vars(config)
        self._print_text("Configuration is:")
        self._print_text(
            "\n".join(f"  {item[0]} = {item[1]}" for item in config_properties.items())
        )
        self.print_empty_line()

    def print_reproduction_code(self, errors: List[ValidationError]) -> None:
        self.reproduction_code_printer.print_reproduction_code(errors)
