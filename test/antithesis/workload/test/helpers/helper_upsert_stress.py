# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the INC-936 upsert-stress Kafka source.

Separate from `helper_upsert_source` so the stress topology (multi-
partition, tiny shared key space hammered by concurrent drivers) does
not collide with the existing `antithesis-upsert-text` source the
kafka-group drivers run their property checks against.

The "invalid upsert state" panic (INC-936) surfaces when the upsert
operator's per-(key, ts) consolidation observes a `diff_sum` outside
{-1, 0, 1}. The conditions most likely to produce that, per the
incident write-up: many partitions, concurrent upserts on the same key,
and fault-injected clusterd restarts. The source created here uses
`NUM_PARTITIONS` partitions; the parallel hammer driver supplies the
concurrency and the global fault-orchestrator supplies the restarts.
"""

from __future__ import annotations

import logging
import os

from helper_kafka import ensure_topic
from helper_pg import create_source_idempotent
from helper_upsert_source import CONNECTION_NAME, ensure_kafka_connection
from helper_upsert_stress_const import (
    NUM_PARTITIONS,
    SOURCE_UPSERT_STRESS,
    TOPIC_UPSERT_STRESS,
)

# Re-export so existing callers (`first_upsert_stress_setup` and any
# future Test Composer driver that needs the source/topic identifiers)
# don't have to know the constants live in their own module now. The
# import-time side effect is what matters here.
__all__ = [
    "NUM_PARTITIONS",
    "SOURCE_UPSERT_STRESS",
    "TOPIC_UPSERT_STRESS",
    "ensure_upsert_stress_source",
]

LOG = logging.getLogger("antithesis.helper_upsert_stress")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")


def ensure_upsert_stress_source() -> None:
    """Create the INC-936 stress source: multi-partition upsert envelope.

    Schema matches `helper_upsert_source` (key TEXT NOT NULL, text TEXT)
    so any shared per-key analysis works across both sources. The
    connection is reused — only one Kafka connection per workload image.
    """
    LOG.info(
        "ensure_upsert_stress_source: starting (source=%s topic=%s partitions=%d)",
        SOURCE_UPSERT_STRESS,
        TOPIC_UPSERT_STRESS,
        NUM_PARTITIONS,
    )
    ensure_kafka_connection()
    ensure_topic(TOPIC_UPSERT_STRESS, num_partitions=NUM_PARTITIONS)
    create_source_idempotent(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_UPSERT_STRESS} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM KAFKA CONNECTION {CONNECTION_NAME} (TOPIC '{TOPIC_UPSERT_STRESS}') "
        f"KEY FORMAT TEXT VALUE FORMAT TEXT "
        f"ENVELOPE UPSERT",
        SOURCE_UPSERT_STRESS,
    )
    LOG.info("ensure_upsert_stress_source: ready (source=%s)", SOURCE_UPSERT_STRESS)
