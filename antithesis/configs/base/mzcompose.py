# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Source-of-truth composition for the `base` Antithesis scenario.

This is the minimal SQL + Kafka topology: Materialize plus a Kafka-compatible
broker plus Schema Registry plus a long-lived `antithesis-test-driver` client
that hosts Test Composer commands and emits `setup_complete` once the system
is up. Other scenarios (`mysql_mt_replicas`, `pg_cdc`, …) live in sibling
directories under `antithesis/configs/`; see
`antithesis/scratchbook/scenario-strategy.md` for when to add one.

The compose file at `antithesis/configs/base/docker-compose.yaml` is the
artifact `snouty validate`, `snouty launch`, and the Antithesis platform
consume. Keep it generated and committed; regenerate it with:

    bin/pyactivate antithesis/bin/render-compose-yaml.py --scenario base

(That helper invokes `bin/mzcompose --find base config` to render the
canonical compose form, then layers on the Antithesis-required attributes
mzcompose does not emit by default.)
"""

from __future__ import annotations

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service, ServiceConfig
from materialize.mzcompose.services.materialized import MaterializeEmulator
from materialize.mzcompose.services.redpanda import Redpanda

# ---------------------------------------------------------------------------
# Service definitions
# ---------------------------------------------------------------------------


# Use Redpanda as a single-process Kafka + Schema Registry. It exposes both via
# the `kafka` and `schema-registry` network aliases, which is what the existing
# upsert_sources workload helper expects.
REDPANDA = Redpanda(auto_create_topics=True)

# `MaterializeEmulator` is the smallest viable Materialize service for the
# Antithesis bring-up: it boots with sane defaults, file-backed persist, and
# the embedded metadata store. Topology-level variations (external metadata,
# Minio-backed persist, multi-`materialized`) belong in a separate launch.
MATERIALIZED = MaterializeEmulator()


class AntithesisTestDriver(Service):
    """Long-lived client container that hosts the Antithesis Test Composer
    commands under `/opt/antithesis/test/v1/`.

    Pulls the workload tree and Materialize Python helpers in via the
    `antithesis-test-driver` mzbuild image. Container/host names are pinned so
    Antithesis can attribute logs and faults consistently.
    """

    def __init__(self, name: str = "antithesis-test-driver") -> None:
        config: ServiceConfig = {
            "mzbuild": "antithesis-test-driver",
            "container_name": name,
            "hostname": name,
            "platform": "linux/amd64",
            "environment": [
                "NO_COLOR=1",
                "FORCE_COLOR=0",
                "PY_COLORS=0",
                "TERM=dumb",
                "MZ_ROOT=/opt/materialize",
                "PYTHONPATH=/opt/materialize/misc/python",
                "MZ_TESTDRIVE=testdrive",
                "MZ_MATERIALIZE_URL=postgres://materialize@materialized:6875",
                "MZ_MATERIALIZE_INTERNAL_URL=postgres://mz_system@materialized:6877",
                "MZ_KAFKA_ADDR=kafka:9092",
                "MZ_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
                "MZ_TESTDRIVE_DEFAULT_TIMEOUT=60s",
                "MZ_HOST=materialized",
                "MZ_SQL_PORT=6875",
                "MZ_HTTP_PORT=6878",
                "KAFKA_HOST=kafka",
                "KAFKA_PORT=9092",
                "SR_HOST=schema-registry",
                "SR_PORT=8081",
            ],
            "depends_on": {
                "materialized": {"condition": "service_started"},
                "redpanda": {"condition": "service_started"},
            },
        }
        super().__init__(name=name, config=config)


SERVICES = [
    MATERIALIZED,
    REDPANDA,
    AntithesisTestDriver(),
]


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------


def workflow_default(c: Composition) -> None:
    """Bring the harness up locally and emit `setup_complete`.

    Intended for quick smoke-checks; `snouty validate` performs the same kind
    of bring-up under its own timeout budget.
    """

    c.up("materialized", "redpanda", "antithesis-test-driver")
    c.sql("SELECT 1")
