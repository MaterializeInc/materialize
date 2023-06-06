# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.util import MzVersion


class RenameReplica(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.58.0-dev")

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE rename_replica_table (f1 INTEGER);
                > INSERT INTO rename_replica_table VALUES (1);

                > CREATE CLUSTER rename_replica REPLICAS ();

                > SET cluster=rename_replica
                > CREATE DEFAULT INDEX ON rename_replica_table;
                > CREATE MATERIALIZED VIEW rename_replica_view AS SELECT COUNT(f1) FROM rename_replica_table;

                > INSERT INTO rename_replica_table VALUES (2);
                > CREATE CLUSTER REPLICA rename_replica.replica1 SIZE '2-2';
                > INSERT INTO rename_replica_table VALUES (3);
                > CREATE CLUSTER REPLICA rename_replica.replica2 SIZE '2-2';
                > INSERT INTO rename_replica_table VALUES (4);
                > ALTER CLUSTER REPLICA rename_replica.replica1 RENAME TO replica_new1
                """,
                """
                > INSERT INTO rename_replica_table VALUES (5);
                > ALTER CLUSTER REPLICA rename_replica.replica2 RENAME TO replica_new2;
                > INSERT INTO rename_replica_table VALUES (6);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=rename_replica

                > SELECT * FROM rename_replica_table;
                1
                2
                3
                4
                5
                6

                > SELECT * FROM rename_replica_view;
                6
           """
            )
        )
