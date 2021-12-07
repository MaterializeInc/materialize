# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from os import environ
from unittest.mock import patch

from materialize.mzcompose import (
    Localstack,
    Materialized,
    Testdrive,
    Toxiproxy,
    Workflow,
)

daemons = [
    Localstack(),
    Materialized(environment=["MZ_LOG_FILTER=dataflow::source::s3=trace,info"]),
    Toxiproxy(),
]

services = [*daemons, Testdrive(default_timeout="600s")]

#
# Test the S3 resumption logic by first instructing Toxiproxy to drop a connection
# after toxiproxy_bytes_allowed have been transfered over it. Then unblock the
# network and expect full recovery if the interruption has been
# shorter than the timeout.
#
def workflow_s3_resumption(w: Workflow):
    w.start_and_wait_for_tcp(services=daemons)
    w.wait_for_mz()

    # For different values of bytes_allowed, the following happens:
    # 0 - Connection is dropped immediately
    # 1K - SQS queue and bucket listing are both prevented
    # 2K - SQS and key fetching are both prevented
    # 10K - only key fetching is prevented

    for toxiproxy_bytes_allowed in [
        0,
        256,
        512,
        768,
        1024,
        1536,
        2 * 1024,
        3 * 1024,
        5 * 1024,
        10 * 1024,
        20 * 1024,
    ]:
        # For small values of toxiproxy_bytes_allowed, we need to allow for CREATE SOURCE to go undisturbed first, otherwise it fails immediately
        toxiproxy_setup = (
            ["configure-materialize.td", "toxiproxy-close-connection.td"]
            if toxiproxy_bytes_allowed < 1024
            else ["toxiproxy-close-connection.td", "configure-materialize.td"]
        )

        with patch.dict(
            environ, {"TOXIPROXY_BYTES_ALLOWED": str(toxiproxy_bytes_allowed)}
        ):
            w.run_service(
                service="testdrive-svc",
                command=" ".join(
                    [
                        f"--no-reset --max-errors=1 --seed {toxiproxy_bytes_allowed} --aws-endpoint=http://toxiproxy:4566",
                        "configure-toxiproxy.td",
                        "s3-create.td s3-insert-long.td s3-insert-long-gzip.td",
                        #
                        # Confirm that short network interruptions are tolerated
                        #
                        *toxiproxy_setup,
                        "short-sleep.td toxiproxy-restore-connection.td materialize-verify-success.td",
                        #
                        # Confirm that long network interruptions cause source error
                        # Disabled due to https://github.com/MaterializeInc/materialize/issues/7009
                        # "s3-insert-long.td s3-insert-long-gzip.td toxiproxy-close-connection.td materialize-verify-failure.td",
                        #
                        # Cleanup
                        #
                        "materialize-drop-source.td toxiproxy-remove.td",
                    ]
                ),
            )
