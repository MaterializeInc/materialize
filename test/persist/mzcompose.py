# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Maelstrom test against the Persist subsystem.
"""

import argparse

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.postgres import PostgresMetadata

SERVICES = [
    Cockroach(setup_materialize=True, in_memory=True),
    PostgresMetadata(),
    Service(
        "maelstrom-persist",
        {"mzbuild": "maelstrom-persist", "volumes": ["./maelstrom:/store"]},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run maelstrom against persist"""

    # Please see `docker run materialize/maelstrom-persist:mzbuild-... --help` for
    # the meaning of the various arguments
    parser.add_argument(
        "--time-limit",
        type=int,
        default=60,
    )

    parser.add_argument("--node-count", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--max-txn-length", type=int, default=4)
    parser.add_argument("--unreliability", type=float, default=None)
    parser.add_argument(
        "--consensus",
        type=str,
        choices=["mem", "cockroach", "maelstrom", "postgres"],
        default="maelstrom",
    )
    parser.add_argument(
        "--blob", type=str, choices=["mem", "maelstrom"], default="maelstrom"
    )
    parser.add_argument(
        "--txn-wal", type=bool, default=False, action=argparse.BooleanOptionalAction
    )

    args = parser.parse_args()

    if args.consensus == "mem":
        consensus_uri = "mem://consensus"
    elif args.consensus == "cockroach":
        consensus_uri = (
            "postgres://root@cockroach:26257?options=--search_path=consensus"
        )
        c.up("cockroach")
    elif args.consensus == "postgres":
        consensus_uri = (
            "postgres://root@postgres-metadata:26257?options=--search_path=consensus"
        )
        c.up("postgres-metadata")
    else:
        # empty consensus uri defaults to Maelstrom consensus implementation
        consensus_uri = ""

    if args.blob == "mem":
        blob_uri = "mem://blob"
    else:
        # empty blob uri defaults to Maelstrom blob implementation
        blob_uri = ""

    maelstrom_cmd = "maelstrom"
    if args.txn_wal:
        maelstrom_cmd = "maelstrom-txn"

    c.run(
        "maelstrom-persist",
        f"--time-limit={args.time_limit}",
        f"--node-count={args.node_count}",
        f"--concurrency={args.concurrency}",
        f"--rate={args.rate}",
        "--",
        maelstrom_cmd,
        *([f"--blob-uri={blob_uri}"] if blob_uri else []),
        *([f"--consensus-uri={consensus_uri}"] if consensus_uri else []),
        *([f"--unreliability={args.unreliability}"] if args.unreliability else []),
    )

    # TODO: Reenable this when we un-break MaelstromConsensus
    # c.run("maelstrom-persist", "--time-limit=5", "--", "maelstrom")
