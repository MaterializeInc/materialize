# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.execution.evaluation_strategy import (
    DataFlowRenderingEvaluation,
    EvaluationStrategyKey,
)
from materialize.postgres_consistency.execution.pg_dialect_adjuster import (
    PgSqlDialectAdjuster,
)


class PgEvaluation(DataFlowRenderingEvaluation):
    def __init__(self) -> None:
        super().__init__()
        self.identifier = EvaluationStrategyKey.POSTGRES
        self.name = "Postgres evaluation"
        self.object_name_base = "t_pg"
        self.simple_db_object_name = "postgres_evaluation"
        self.sql_adjuster = PgSqlDialectAdjuster()
