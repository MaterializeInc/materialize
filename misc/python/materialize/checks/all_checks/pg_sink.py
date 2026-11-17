# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


@externally_idempotent(False)
class PgSink(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.52.0-dev")

    def _roundtrip(self, phase: int) -> str:
        return "".join(dedent(f"""
                > CREATE SINK pg_sink_{kind}{phase} FROM pg_sink_{kind}
                  INTO POSTGRES CONNECTION pg_sink_conn (SCHEMA 'pgsink', TABLE '{kind}{phase}')
                  {"KEY (a) NOT ENFORCED" if kind == "keyed" else ""}

                > SELECT write_frontier > 0 FROM mz_internal.mz_frontiers
                  WHERE object_id = (SELECT id FROM mz_sinks WHERE name = 'pg_sink_{kind}{phase}')
                true

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER TABLE pgsink.{kind}{phase} REPLICA IDENTITY FULL;
                CREATE PUBLICATION pg_sink_{kind}{phase} FOR TABLE pgsink.{kind}{phase};

                > CREATE SOURCE pg_sink_{kind}{phase}_source FROM POSTGRES CONNECTION pg_sink_conn
                  (PUBLICATION 'pg_sink_{kind}{phase}')
                > CREATE TABLE pg_sink_{kind}{phase}_readback FROM SOURCE pg_sink_{kind}{phase}_source
                  (REFERENCE pgsink.{kind}{phase})

                > SELECT * FROM
                  ((SELECT * FROM pg_sink_{kind} EXCEPT ALL SELECT * FROM pg_sink_{kind}{phase}_readback)
                  UNION ALL
                  (SELECT * FROM pg_sink_{kind}{phase}_readback EXCEPT ALL SELECT * FROM pg_sink_{kind}))
            """) for kind in ["keyed", "keyless"])

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE SCHEMA pgsink;

                > CREATE SECRET pg_sink_pass AS 'postgres'
                > CREATE CONNECTION pg_sink_conn TO POSTGRES
                  (HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pg_sink_pass)
                > CREATE TABLE pg_sink_keyed (a int NOT NULL, b text)
                > INSERT INTO pg_sink_keyed VALUES (1, 'dup'), (2, 'dup'), (3, 'dup'), (4, NULL)
                > CREATE MATERIALIZED VIEW pg_sink_keyless AS SELECT b FROM pg_sink_keyed
            """) + self._roundtrip(0))

    def manipulate(self) -> list[Testdrive]:
        return [Testdrive(dedent(f"""
                    > UPDATE pg_sink_keyed SET b = 'phase{phase}' WHERE a = 1
                    > DELETE FROM pg_sink_keyed WHERE a = {phase + 1}
                    > INSERT INTO pg_sink_keyed VALUES ({phase + 4}, 'dup')
                """) + self._roundtrip(phase)) for phase in [1, 2]]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent("""
                > SELECT * FROM pg_sink_keyed
                1 phase2
                4 <null>
                5 dup
                6 dup
            """)
            + "".join(dedent(f"""
                    > SELECT * FROM
                      ((SELECT * FROM pg_sink_{kind} EXCEPT ALL SELECT * FROM pg_sink_{kind}{phase}_readback)
                      UNION ALL
                      (SELECT * FROM pg_sink_{kind}{phase}_readback EXCEPT ALL SELECT * FROM pg_sink_{kind}))
                """) for phase in range(3) for kind in ["keyed", "keyless"])
        )
