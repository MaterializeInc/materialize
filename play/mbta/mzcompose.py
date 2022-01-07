# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.mzcompose import Composition, WorkflowArgumentParser


def workflow_start_live_data(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--config-file",
        default="/workdir/examples/all-frequent-routes-config-weekend.csv",
    )
    parser.add_argument("--archive-at-shutdown", action="store_true")
    parser.add_argument("api_key", metavar="API-KEY")
    args = parser.parse_args()

    start_everything(c)
    c.run(
        "mbta-demo",
        "start_docker",
        args.config_file,
        "kafka:9092",
        args.api_key,
        "1" if args.archive_at_shutdown else "0",
        detach=True,
    )


def workflow_replay(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--config-file",
        default="/workdir/examples/all-frequent-routes-config-weekend.csv",
    )
    parser.add_argument("archive_path", metavar="ARCHIVE-PATH")
    args = parser.parse_args()
    start_everything(c)
    c.run(
        "mbta-demo",
        "replay",
        args.config_file,
        args.archive_path,
        "kakfa:9092",
        detach=True,
    )


def start_everything(c: Composition) -> None:
    c.up("kafka", "materialized")
    c.wait_for_tcp(host="kafka", port=9092)
    c.wait_for_materialized()
