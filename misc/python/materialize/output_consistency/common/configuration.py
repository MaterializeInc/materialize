# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


class ConsistencyTestConfiguration:
    def __init__(
        self,
        queries_per_tx: int,
        use_autocommit: bool,
        max_cols_per_query: int,
        max_pending_expressions: int,
        random_seed: str,
        split_and_retry_on_db_error: bool,
        dry_run: bool,
        fail_fast: bool,
        verbose_output: bool,
        print_reproduction_code: bool,
        max_runtime_in_sec: int,
        max_iterations: int,
        avoid_expressions_expecting_db_error: bool,
        skip_postgres_incompatible_types: bool,
    ):
        self.queries_per_tx = queries_per_tx
        self.use_autocommit = use_autocommit
        self.max_cols_per_query = max_cols_per_query
        self.max_pending_expressions = max_pending_expressions
        self.random_seed = random_seed
        self.split_and_retry_on_db_error = split_and_retry_on_db_error
        self.dry_run = dry_run
        self.fail_fast = fail_fast
        self.verbose_output = verbose_output
        self.print_reproduction_code = print_reproduction_code
        self.max_runtime_in_sec = max_runtime_in_sec
        self.max_iterations = max_iterations
        self.avoid_expressions_expecting_db_error = avoid_expressions_expecting_db_error
        self.skip_postgres_incompatible_types = skip_postgres_incompatible_types

    def validate(self) -> None:
        if self.max_runtime_in_sec == 0 and self.max_iterations == 0:
            raise RuntimeError(
                "Either 'max_runtime_in_sec' or 'max_iterations' must not be 0"
            )
