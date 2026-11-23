# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Runs the Antithesis driver against a local Materialize, without Antithesis and
without faults, so that the workload and its checks are tested in CI.
"""

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio, minio_blob_uri
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.redpanda import Redpanda

SERVICES = [
    Postgres(),
    Redpanda(),
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Materialized(
        external_metadata_store=True,
        metadata_store="postgres-metadata",
        external_blob_store=True,
    ),
    Service(
        "driver",
        {
            "mzbuild": "antithesis-driver",
            "environment": [
                "PG_URL=postgres://postgres:postgres@postgres:5432/postgres",
                "CONSENSUS_URI=postgres://root@postgres-metadata:26257?options=--search_path%3Dconsensus",
                f"BLOB_URI={minio_blob_uri()}",
                "KAFKA_BROKER=redpanda:9092",
                "REPLICA_SIZES=scale=1,workers=1;scale=1,workers=2",
                "ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/assertions.jsonl",
                # A table added to a running Postgres source is ahead of the
                # source's other tables for a moment, a known bug.
                "SKIP_RULES=pg_export",
                "DURATION",
            ],
        },
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--duration", type=int, default=60, help="seconds of workload")
    args = parser.parse_args()
    c.up("postgres", "redpanda", "materialized")
    c.run("driver", env_extra={"DURATION": str(args.duration)})
