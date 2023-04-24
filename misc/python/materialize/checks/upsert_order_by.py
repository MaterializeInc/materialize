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


class UpsertOrderBy(Check):
    """Test source with an explicit ORDER BY. The resulting data should not be affected by e.g. restarts or upgrades."""

    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.52.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ kafka-create-topic topic=upsert-order-by

                $ kafka-ingest format=bytes topic=upsert-order-by key-format=bytes key-terminator=: timestamp=500
                key:original_value

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE SOURCE upsert_order_by
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-order-by-${testdrive.seed}')
                  KEY FORMAT TEXT
                  VALUE FORMAT TEXT
                  INCLUDE OFFSET as o, TIMESTAMP as ts
                  ENVELOPE UPSERT ( ORDER BY ( ts, o))

                > SELECT COUNT(*) FROM upsert_order_by
                1
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                # Must be ignored due to timestamp too old
                $ kafka-ingest format=bytes topic=upsert-order-by key-format=bytes key-terminator=: timestamp=100
                key:value_timestamp_too_old
                """,
                """
                # Must be ignored due to timestamp too old
                $ kafka-ingest format=bytes topic=upsert-order-by key-format=bytes key-terminator=: timestamp=200
                key:value_timestamp_too_old

                $ kafka-ingest format=bytes topic=upsert-order-by key-format=bytes key-terminator=: timestamp=999
                end_marker:end_marker
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                # Only the original upsert at timestamp = 500 should be reflected
                > SELECT key, text, ts FROM upsert_order_by
                key original_value "1970-01-01 00:00:00.500"
                end_marker end_marker "1970-01-01 00:00:00.999"
                """
            )
        )
