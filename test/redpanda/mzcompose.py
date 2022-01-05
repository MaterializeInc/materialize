# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Redpanda, Testdrive

SERVICES = [
    Redpanda(),
    Materialized(),
    Testdrive(
        shell_eval=True,
        volume_workdir="../testdrive:/workdir",
        depends_on=["redpanda", "materialized"],
    ),
]


def workflow_testdrive(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive tests against Redpanda."""
    parser.add_argument(
        "filter", nargs="*", help="limit to only the files matching filter"
    )
    args = parser.parse_args()

    if args.filter:
        # NOTE(benesch): this is so goofy, but at least it prevents the weird
        # semantics of shell_eval from reaching users. We should figure out how
        # to get rid of shell_eval.
        command = "echo " + " ".join(args.filter)
    else:
        # Features currently not supported by Redpanda:
        # - `kafka-time-offset.td` (https://github.com/vectorizedio/redpanda/issues/2397)

        # Due to interactions between docker-compose, entrypoint, command, and bash,
        # it is not possible to have a more complex filtering expression in
        # 'command' . So we basically run the entire testdrive suite here except
        # tests that contain features known to be not supported by Redpanda. So the
        # run includes testdrive tests that do not touch Kafka at all.
        #
        # TODO(benesch): fix this.
        command = "grep -L -E kafka_time_offset *.td"

    c.run("testdrive-svc", command)
