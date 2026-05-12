# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the Antithesis UPSERT-envelope Kafka source.

Used by all drivers that exercise UPSERT semantics. The topic is pre-created
via the Kafka admin client (broker auto-create only triggers on producer
write, but CREATE SOURCE does a metadata fetch that fails fast otherwise).
The source/connection are created at most once across all drivers
(CREATE ... IF NOT EXISTS).
"""

from __future__ import annotations

import logging
import os

from helper_kafka import ensure_topic
from helper_pg import create_source_idempotent, execute_retry

LOG = logging.getLogger("antithesis.helper_upsert_source")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")

CONNECTION_NAME = "antithesis_kafka_conn"
TOPIC_UPSERT_TEXT = "antithesis-upsert-text"
SOURCE_UPSERT_TEXT = "upsert_text_src"


def ensure_kafka_connection() -> None:
    execute_retry(
        f"CREATE CONNECTION IF NOT EXISTS {CONNECTION_NAME} "
        f"TO KAFKA (BROKER '{KAFKA_BROKER}', SECURITY PROTOCOL = 'PLAINTEXT')"
    )


def ensure_upsert_text_source() -> None:
    """Create the upsert-envelope source over a text key/value Kafka topic.

    The resulting source has columns `key TEXT NOT NULL` and `text TEXT`.
    """
    ensure_kafka_connection()
    ensure_topic(TOPIC_UPSERT_TEXT)
    create_source_idempotent(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_UPSERT_TEXT} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM KAFKA CONNECTION {CONNECTION_NAME} (TOPIC '{TOPIC_UPSERT_TEXT}') "
        f"KEY FORMAT TEXT VALUE FORMAT TEXT "
        f"ENVELOPE UPSERT",
        SOURCE_UPSERT_TEXT,
    )
    LOG.info("upsert source %s ready (topic=%s)", SOURCE_UPSERT_TEXT, TOPIC_UPSERT_TEXT)
