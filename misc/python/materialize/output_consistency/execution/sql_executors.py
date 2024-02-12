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
)
from materialize.output_consistency.execution.sql_executor import SqlExecutor


class SqlExecutors:
    def __init__(self, executor: SqlExecutor):
        self.executor = executor

    def get_executor(self, strategy: EvaluationStrategy) -> SqlExecutor:
        return self.executor

    def get_database_infos(self) -> str:
        return (
            f"Using {self.executor.name} in version '{self.executor.query_version()}'."
        )
