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


class MultiMzSqlExecutors(SqlExecutors):
    def __init__(self, executor1: SqlExecutor, executor2: SqlExecutor):
        super().__init__(executor1)
        self.executor2 = executor2

    def get_executor(self, strategy: EvaluationStrategy) -> SqlExecutor:
        if strategy.identifier in [
            EvaluationStrategyKey.MZ_DATAFLOW_RENDERING_OTHER_DB,
            EvaluationStrategyKey.MZ_CONSTANT_FOLDING_OTHER_DB,
        ]:
            return self.executor2

        return super().get_executor(strategy)

    def get_database_infos(self) -> str:
        return (
            f"Using {self.executor.name} in version '{self.executor.query_version()}'. "
            f"Using {self.executor2.name} in version '{self.executor2.query_version()}'."
        )

    def uses_different_versions(self) -> bool:
        return self.executor.query_version() != self.executor2.query_version()
