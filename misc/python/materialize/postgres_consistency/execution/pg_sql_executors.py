# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.execution.evaluation_strategy import (
    EvaluationStrategy,
    EvaluationStrategyKey,
)
from materialize.output_consistency.execution.sql_executor import SqlExecutor
from materialize.output_consistency.execution.sql_executors import SqlExecutors


class PgSqlExecutors(SqlExecutors):
    def __init__(self, mz_executor: SqlExecutor, pg_executor: SqlExecutor):
        super().__init__(mz_executor)
        self.pg_executor = pg_executor

    def get_executor(self, strategy: EvaluationStrategy) -> SqlExecutor:
        if strategy.identifier == EvaluationStrategyKey.POSTGRES:
            return self.pg_executor

        return super().get_executor(strategy)

    def get_database_infos(self) -> str:
        return (
            f"Using {self.executor.name} in version '{self.executor.query_version()}'. "
            f"Using {self.pg_executor.name} in version '{self.pg_executor.query_version()}'."
        )
