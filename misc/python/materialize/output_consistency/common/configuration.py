# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.output_consistency.execution.query_output_mode import (
    QueryOutputMode,
)
from materialize.output_consistency.input_data.scenarios.evaluation_scenario import (
    EvaluationScenario,
)


class ConsistencyTestConfiguration:
    def __init__(
        self,
        scenario: EvaluationScenario,
        queries_per_tx: int,
        use_autocommit: bool,
        max_cols_per_query: int,
        max_pending_expressions: int,
        random_seed: str,
        split_and_retry_on_db_error: bool,
        dry_run: bool,
        verbose_output: bool,
        print_reproduction_code: bool,
        max_runtime_in_sec: int,
        max_iterations: int,
        max_failures_until_abort: int,
        avoid_expressions_expecting_db_error: bool,
        disable_predefined_queries: bool,
        query_output_mode: QueryOutputMode,
        vertical_join_tables: int,
    ):
        self.scenario = scenario
        self.queries_per_tx = queries_per_tx
        self.use_autocommit = use_autocommit
        self.max_cols_per_query = max_cols_per_query
        self.max_pending_expressions = max_pending_expressions
        self.random_seed = random_seed
        self.split_and_retry_on_db_error = split_and_retry_on_db_error
        self.dry_run = dry_run
        self.verbose_output = verbose_output
        self.print_reproduction_code = print_reproduction_code
        self.max_runtime_in_sec = max_runtime_in_sec
        self.max_iterations = max_iterations
        self.max_failures_until_abort = max_failures_until_abort
        self.avoid_expressions_expecting_db_error = avoid_expressions_expecting_db_error
        self.disable_predefined_queries = disable_predefined_queries
        self.query_output_mode = query_output_mode
        self.vertical_join_tables = vertical_join_tables

    def validate(self) -> None:
        if self.max_runtime_in_sec == 0 and self.max_iterations == 0:
            raise RuntimeError(
                "Either 'max_runtime_in_sec' or 'max_iterations' must not be 0"
            )
