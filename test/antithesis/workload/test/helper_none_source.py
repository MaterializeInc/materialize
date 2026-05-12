# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the Antithesis NONE-envelope (append-only) Kafka source.

Used by drivers that exercise the append-only contract. The source has columns
`text TEXT, partition INTEGER, offset BIGINT` — `partition` and `offset` are
the Kafka metadata projected via `INCLUDE PARTITION, OFFSET`, which give us
the per-`(partition, offset)` uniqueness check called out in
`kafka-source-no-data-duplication.md`.
"""

from __future__ import annotations

import logging
import os

from helper_kafka import ensure_topic
from helper_pg import execute_retry
from helper_upsert_source import ensure_kafka_connection

LOG = logging.getLogger("antithesis.helper_none_source")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")

TOPIC_NONE_TEXT = "antithesis-none-text"
SOURCE_NONE_TEXT = "none_text_src"


def ensure_none_text_source() -> None:
    """Create the append-only source over a text-valued Kafka topic.

    Resulting columns: `text TEXT NOT NULL, partition INTEGER, offset BIGINT`.
    Reuses the shared `antithesis_kafka_conn` Kafka connection so multiple
    drivers don't proliferate connections.
    """
    ensure_kafka_connection()
    # CREATE SOURCE issues a Kafka metadata fetch that fails fast if the topic
    # is missing; broker auto-create only fires on a producer write, which
    # comes later in the driver. Pre-create via admin client so the metadata
    # fetch succeeds on the first run.
    ensure_topic(TOPIC_NONE_TEXT)
    execute_retry(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_NONE_TEXT} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM KAFKA CONNECTION antithesis_kafka_conn (TOPIC '{TOPIC_NONE_TEXT}') "
        f"FORMAT TEXT "
        f"INCLUDE PARTITION, OFFSET "
        f"ENVELOPE NONE"
    )
    LOG.info(
        "none-envelope source %s ready (topic=%s)", SOURCE_NONE_TEXT, TOPIC_NONE_TEXT
    )
