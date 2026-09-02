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


class Secrets(Check):
    """Secrets as catalog objects: creation from literals and expressions,
    value rotation via ALTER SECRET ... AS, rename, and SHOW SECRETS.

    Secret values are not observable through SQL by design, so rotation is
    only verified to succeed. End-to-end use of secrets is covered by the
    connection-based checks (kafka_protocols.py, pg_cdc.py, ...).
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SECRET secrets_s1 AS 'initial_value_1'
            > CREATE SECRET secrets_s2 AS decode('aW5pdGlhbF92YWx1ZV8y', 'base64')
            > COMMENT ON SECRET secrets_s1 IS 'secrets check comment'
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER SECRET secrets_s1 AS 'rotated_value_1'
                > ALTER SECRET secrets_s2 RENAME TO secrets_s2_renamed
                > CREATE SECRET secrets_s3 AS 'phase_1_value'
                """,
                """
                > ALTER SECRET secrets_s3 AS decode('cGhhc2VfMl92YWx1ZQ==', 'base64')
                > CREATE SECRET secrets_s4 AS 'phase_2_value'
                > ALTER SECRET secrets_s1 AS 'rotated_value_1_again'
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SHOW SECRETS LIKE 'secrets_%'
            secrets_s1 "secrets check comment"
            secrets_s2_renamed ""
            secrets_s3 ""
            secrets_s4 ""

            > SELECT name FROM mz_secrets WHERE name LIKE 'secrets_%'
            secrets_s1
            secrets_s2_renamed
            secrets_s3
            secrets_s4

            > SELECT c.comment
              FROM mz_internal.mz_comments c
              JOIN mz_secrets s ON c.id = s.id
              WHERE s.name = 'secrets_s1'
            "secrets check comment"

            # Rotation still works on a secret that has survived the scenario.
            > ALTER SECRET secrets_s1 AS 'validate_rotation'
            """))
