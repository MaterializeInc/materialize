# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.mzcompose import Workflow, WorkflowArgumentParser


def workflow_start_live_data(w: Workflow, args: List[str]):
    parser = WorkflowArgumentParser(w)
    parser.add_argument(
        "--config-file",
        default="/workdir/examples/all-frequent-routes-config-weekend.csv",
    )
    parser.add_argument("--archive-at-shutdown", action="store_true")
    parser.add_argument("api_key", metavar="API-KEY")
    args = parser.parse_args(args)

    start_everything(w)
    w.run_service(
        service="mbta-demo",
        daemon=True,
        command=f"start_docker {args.config_file} kafka:9092 {args.api_key} {int(args.archive_at_shutdown)}",
    )


def workflow_replay(w: Workflow, args: List[str]):
    parser = WorkflowArgumentParser(w)
    parser.add_argument(
        "--config-file",
        default="/workdir/examples/all-frequent-routes-config-weekend.csv",
    )
    parser.add_argument("archive_path", metavar="ARCHIVE-PATH")
    args = parser.parse_args(args)
    start_everything(w)
    w.run_service(
        service="mbta-demo",
        daemon=True,
        command=f"replay {args.config_file} {args.archive_path} kakfa:9092",
    )


def start_everything(w: Workflow):
    w.start_services(services=["kafka", "materialized"])
    w.wait_for_tcp(host="kafka", port=9092)
    w.wait_for_mz()
