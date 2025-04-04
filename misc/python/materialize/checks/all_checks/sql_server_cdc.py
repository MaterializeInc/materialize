# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random
from textwrap import dedent
from typing import Any

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.sql_server import SqlServer


class SqlServerCdcBase:
    base_version: MzVersion
    current_version: MzVersion
    wait: bool
    suffix: str
    repeats: int
    expects: int

    def __init__(self, wait: bool, **kwargs: Any) -> None:
        self.wait = wait
        self.repeats = 1024 if wait else 16384
        self.expects = 97350 if wait else 1633350
        self.suffix = f"_{str(wait).lower()}"
        super().__init__(**kwargs)  # forward unused args to Check

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.140.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_sql_server_source = true;

                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                CREATE DATABASE test IF NOT EXISTS;
                USE test;

                > CREATE SECRET sql_server_password_{self.suffix} AS '{SqlServer.DEFAULT_SA_PASSWORD}';

                > CREATE CONNECTION sql_server_connection_{self.suffix} TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_server_password_{self.suffix}
                  )
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                $postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_sql_server_source = true;

                > VALIDATE CONNECTION sql_server_password_{self.suffix};
                """,
                f"""
                $postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_sql_server_source = true;

                > DROP CONNECTION sql_server_password_{self.suffix};
                > CREATE CONNECTION sql_server_connection2_{self.suffix} TO SQL SERVER (
                    HOST 'sql-server',
                    DATABASE test,
                    USER {SqlServer.DEFAULT_USER},
                    PASSWORD SECRET sql_server_password_{self.suffix}
                  );
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        sql = dedent(
            f"""
            $postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
            ALTER SYSTEM SET enable_sql_server_source = true;

            > VALIDATE CONNECTION sql_server_connection2_{self.suffix};
            """
        )

        return Testdrive(sql)


@externally_idempotent(False)
class SqlServerCdc(SqlServerCdcBase, Check):
    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        super().__init__(wait=True, base_version=base_version, rng=rng)
