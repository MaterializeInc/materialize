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
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD


def schema() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


class AlterIndex(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schema()
            + dedent(
                """
                > CREATE TABLE alter_index_table (f1 STRING);
                > CREATE DEFAULT INDEX ON alter_index_table;
                > INSERT INTO alter_index_table SELECT 'A' || generate_series FROM generate_series(1,10000);

                $ kafka-create-topic topic=alter-index

                $ kafka-ingest format=avro topic=alter-index schema=${schema} repeat=10000
                {"f1": "A${kafka-ingest.iteration}"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE alter_index_source
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-index-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE

                > CREATE DEFAULT INDEX ON alter_index_source
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schema() + dedent(s))
            for s in [
                """
                > INSERT INTO alter_index_table SELECT 'B' || generate_series FROM generate_series(1,10000);
                $ kafka-ingest format=avro topic=alter-index schema=${schema} repeat=10000
                {"f1": "B${kafka-ingest.iteration}"}

                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_index_options = true
                ALTER SYSTEM SET enable_logical_compaction_window = true

                > ALTER INDEX alter_index_table_primary_idx SET (LOGICAL COMPACTION WINDOW = '1ms');
                > ALTER INDEX alter_index_source_primary_idx SET (LOGICAL COMPACTION WINDOW = '1ms');

                > INSERT INTO alter_index_table SELECT 'C' || generate_series FROM generate_series(1,10000);
                $ kafka-ingest format=avro topic=alter-index schema=${schema} repeat=10000
                {"f1": "C${kafka-ingest.iteration}"}
                """,
                """
                # When upgrading from old version without roles the indexes are
                # owned by default_role, thus we have to change the owner
                # before altering them:
                $[version>=4700] postgres-execute connection=postgres://mz_system:materialize@materialized:6877
                ALTER INDEX alter_index_table_primary_idx OWNER TO materialize;
                ALTER INDEX alter_index_source_primary_idx OWNER TO materialize;

                > INSERT INTO alter_index_table SELECT 'D' || generate_series FROM generate_series(1,10000);
                $ kafka-ingest format=avro topic=alter-index schema=${schema} repeat=10000
                {"f1": "D${kafka-ingest.iteration}"}

                > ALTER INDEX alter_index_table_primary_idx SET (LOGICAL COMPACTION WINDOW = '1h');
                > ALTER INDEX alter_index_source_primary_idx SET (LOGICAL COMPACTION WINDOW = '1h');

                > INSERT INTO alter_index_table SELECT 'E' || generate_series FROM generate_series(1,10000);
                $ kafka-ingest format=avro topic=alter-index schema=${schema} repeat=10000
                {"f1": "E${kafka-ingest.iteration}"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT LEFT(f1,1), COUNT(*), COUNT(DISTINCT f1) FROM alter_index_table GROUP BY LEFT(f1,1);
                A 10000 10000
                B 10000 10000
                C 10000 10000
                D 10000 10000
                E 10000 10000

                > SELECT LEFT(f1,1), COUNT(*), COUNT(DISTINCT f1) FROM alter_index_source GROUP BY LEFT(f1,1);
                A 10000 10000
                B 10000 10000
                C 10000 10000
                D 10000 10000
                E 10000 10000
           """
            )
        )
