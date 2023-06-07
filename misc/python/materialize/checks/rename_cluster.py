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


class RenameCluster(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.58.0-dev")

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE rename_cluster1_table (f1 INTEGER);
                > CREATE TABLE rename_cluster2_table (f1 INTEGER);

                > INSERT INTO rename_cluster1_table VALUES (123);
                > INSERT INTO rename_cluster2_table VALUES (234);

                > CREATE CLUSTER rename_cluster1 REPLICAS (replica1 (SIZE '2-2'));
                > CREATE CLUSTER rename_cluster2 REPLICAS (replica1 (SIZE '2-2'));

                > SET cluster=rename_cluster1
                > CREATE DEFAULT INDEX ON rename_cluster1_table;
                > CREATE MATERIALIZED VIEW rename_cluster1_view AS SELECT SUM(f1) FROM rename_cluster1_table;

                > SET cluster=rename_cluster2
                > CREATE DEFAULT INDEX ON rename_cluster2_table;
                > CREATE MATERIALIZED VIEW rename_cluster2_view AS SELECT SUM(f1) FROM rename_cluster2_table;

                > ALTER CLUSTER rename_cluster1 RENAME TO rename_cluster_new1;
                """,
                """

                > ALTER CLUSTER rename_cluster2 RENAME TO rename_cluster_new2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=rename_cluster_new1

                > SET cluster=rename_cluster_new2

                > SET cluster=default

                > SELECT * FROM rename_cluster1_table;
                123

                > SELECT * FROM rename_cluster1_view;
                123

                > SELECT * FROM rename_cluster2_table;
                234

                > SELECT * FROM rename_cluster2_view;
                234
           """
            )
        )
