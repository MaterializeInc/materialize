# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Materialized, Redpanda, Testdrive, Workflow

services = [
    Redpanda(),
    Materialized(),
    Testdrive(shell_eval=True, volume_workdir="../testdrive:/workdir"),
]


def workflow_redpanda_testdrive(w: Workflow):
    w.start_and_wait_for_tcp(services=["redpanda", "materialized"])

    # Features currently not supported by Redpanda:
    # - `kafka-time-offset.td` (https://github.com/vectorizedio/redpanda/issues/2397)

    # Due to interactions between docker-compose, entrypoint, command, and bash, it is not possible to have
    # a more complex filtering expression in 'command' . So we basically run the entire testdrive suite here
    # except tests that contain features known to be not supported by Redpanda. So the run includes testdrive
    # tests that do not touch Kafka at all.

    w.run_service(
        service="testdrive-svc",
        command="grep -L -E 'kafka_time_offset' *.td",
    )
