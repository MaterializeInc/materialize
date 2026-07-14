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
from materialize.checks.checks import Check

MZ_SYSTEM = "$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}"

# The version in which the planner started lowercasing quoted variable-name
# identifiers, making quoted spellings like "MAX_KAFKA_CONNECTIONS" address
# the same variable (and the same durable config entry) as unquoted ones.
QUOTED_NAMES_VERSION_NUM = 2603400


class SystemVarSpelling(Check):
    """ALTER SYSTEM SET/RESET must address the same durable config entry
    regardless of how the variable name is spelled: unquoted names are
    lowercased by the lexer and quoted names by the planner.

    NOTE: The in-memory variable state resolves names case-insensitively, so
    within one process lifetime it masks a durable key desync: SHOW reads the
    correct value even when a durable removal missed its key, and the stale
    entry only surfaces at the next boot. The leading SHOWs in validate() are
    therefore the load-bearing assertions: each validate() run boot-checks the
    durable state that the previous one left behind. A desync also requires
    mixed spellings, because SET and RESET using the same spelling are
    self-consistent even under a broken normalization. That is why every
    SET/RESET pair below deliberately mixes spellings.

    This check uses `max_kafka_connections` and `max_postgres_connections`
    because ConfigureMz overwrites most other limit variables on every
    StartMz. The numeric values are arbitrary non-default markers.
    """

    def initialize(self) -> Testdrive:
        # Runs on the base version in upgrade scenarios. Plant a durable
        # config entry via the unquoted spelling for validate() to address
        # via quoted spellings.
        return Testdrive(dedent(f"""
                {MZ_SYSTEM}
                ALTER SYSTEM SET max_kafka_connections = 471
                """))

    def manipulate(self) -> list[Testdrive]:
        # The planted entry must survive each restart/upgrade phase. Runs on
        # old released binaries in upgrade scenarios, so unquoted only.
        check_present = "> SHOW max_kafka_connections\n471"
        return [Testdrive(check_present), Testdrive(check_present)]

    def validate(self) -> Testdrive:
        # The quoted spellings are skipped on binaries that predate the
        # planner normalization (validate() runs on the rolled-back old
        # binary in PreflightCheckRollback). The quoted section restores the
        # exact durable state it found, keeping validate() idempotent and the
        # leading SHOWs valid on every run.
        return Testdrive(dedent(f"""
                > SHOW max_kafka_connections
                471

                > SHOW max_postgres_connections
                1000

                $ skip-if
                SELECT mz_version_num() < {QUOTED_NAMES_VERSION_NUM}

                {MZ_SYSTEM}
                ALTER SYSTEM RESET "MAX_KAFKA_CONNECTIONS"

                > SHOW max_kafka_connections
                1000

                {MZ_SYSTEM}
                ALTER SYSTEM SET "max_KAFKA_connections" = 471

                > SHOW max_kafka_connections
                471

                {MZ_SYSTEM}
                ALTER SYSTEM SET "MAX_POSTGRES_CONNECTIONS" = 628

                > SHOW max_postgres_connections
                628

                {MZ_SYSTEM}
                ALTER SYSTEM RESET max_postgres_connections

                > SHOW max_postgres_connections
                1000

                {MZ_SYSTEM}
                ALTER SYSTEM SET max_postgres_connections = 629

                {MZ_SYSTEM}
                ALTER SYSTEM RESET "MAX_POSTGRES_CONNECTIONS"

                > SHOW max_postgres_connections
                1000

                $ skip-end
                """))
