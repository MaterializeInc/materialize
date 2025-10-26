# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum, auto


class Source(Enum):
    TABLE = auto()


class TransactionIsolation(Enum):
    SERIALIZABLE = "serializable"
    STRICT_SERIALIZABLE = "strict serializable"

    def __str__(self) -> str:
        return self.value


class Schema:
    def __init__(
        self,
        source: Source = Source.TABLE,
        schema: str = "scalability",
        create_index: bool = True,
        transaction_isolation: TransactionIsolation | None = None,
        cluster_name: str | None = None,
        object_count: int = 1,
    ) -> None:
        self.schema = schema
        self.source = source
        self.create_index = create_index
        self.transaction_isolation = transaction_isolation
        self.cluster_name = cluster_name
        self.object_count = object_count

    def init_sqls(self) -> list[str]:
        init_sqls = self.connect_sqls() + [
            f"DROP SCHEMA IF EXISTS {self.schema} CASCADE;",
            f"CREATE SCHEMA {self.schema};",
            "DROP TABLE IF EXISTS t1;",
        ]
        if self.source == Source.TABLE:
            for t in range(1, self.object_count + 1):
                init_sqls.extend(
                    [
                        f"CREATE TABLE t{t} (f1 INTEGER DEFAULT 1);",
                        f"INSERT INTO t{t} DEFAULT VALUES;",
                        # f"CREATE OR REPLACE MATERIALIZED VIEW mv{t} AS SELECT count(*) AS count FROM t{t};",
                    ]
                )

                # Create indexes and wait until they are queryable. Compute can
                # delay execution of dataflows when inputs are not yet
                # available. This would lead to a large outlier on the first
                # query of the actual workload, when it has to wait for the
                # index to be ready.
                if self.create_index:
                    init_sqls.append(f"CREATE INDEX i{t} ON t{t} (f1);")
                    # init_sqls.append(f"CREATE INDEX mv_i{t} ON mv{t} (count);")
                    init_sqls.append(f"SELECT f1 from t{t};")
                    # init_sqls.append(f"SELECT count from mv{t};")

        return init_sqls

    def connect_sqls(self) -> list[str]:
        init_sqls = [f"SET SCHEMA = {self.schema};"]
        if self.cluster_name is not None:
            init_sqls.append(f"SET CLUSTER = {self.cluster_name};")

        if self.transaction_isolation is not None:
            init_sqls.append(
                f"SET transaction_isolation = '{self.transaction_isolation}';"
            )

        return init_sqls
