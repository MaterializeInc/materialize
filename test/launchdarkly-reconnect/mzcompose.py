# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Regression test for incident-984: the LaunchDarkly data source must reconnect
after its streaming connection dies with a mid-body read timeout, so that flag
updates keep syncing.

A mock LaunchDarkly server (mock_ld.py) serves an initial flag value on the
first two streaming connections and then goes silent while holding each
connection open. Two, because environmentd creates a short-lived bootstrap LD
client at boot before the long-lived sync client, and the stall must hit the
sync client. The SDK's transport read timeout (shortened to seconds via
MZ_LAUNCHDARKLY_READ_TIMEOUT) then fires mid-body, reproducing the exact
incident error class: `hyper::Error(Body, Kind(TimedOut))` on a silently-dead
connection. Note that a TCP RST or clean FIN would not reproduce it, those are
retried inside the eventsource client on every SDK version.

Every reconnecting client receives an updated value. environmentd is pointed
at the mock via MZ_LAUNCHDARKLY_BASE_URI, so the updated value can only reach
it if the data source reconnected after the timeout. A regressed SDK gets
stuck on the initial value and the assertion below times out.

Unlike test/launchdarkly, this needs no real LaunchDarkly credentials.
"""

from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.service import Service as DockerService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

FLAG_KEY = "reconnect-test"
MOCK_HOST = "mock-launchdarkly"
MOCK_PORT = 8080

SERVICES = [
    DockerService(
        name=MOCK_HOST,
        config={
            "image": "python:3.12-slim",
            "volumes": ["./mock_ld.py:/app/mock_ld.py"],
            "command": ["python3", "-u", "/app/mock_ld.py"],
            "ports": [MOCK_PORT],
            "healthcheck": {
                "test": [
                    "CMD",
                    "python3",
                    "-c",
                    "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')",
                ],
                "interval": "1s",
                "start_period": "30s",
            },
        },
    ),
    Materialized(
        environment_extra=[
            "MZ_LAUNCHDARKLY_SDK_KEY=sdk-mock-key",
            f"MZ_LAUNCHDARKLY_BASE_URI=http://{MOCK_HOST}:{MOCK_PORT}",
            f"MZ_LAUNCHDARKLY_KEY_MAP=max_result_size={FLAG_KEY}",
            "MZ_CONFIG_SYNC_LOOP_INTERVAL=1s",
            # Shorten the streaming read timeout (default 300s) so the mock's
            # stalled connection times out in seconds. Must be well above the
            # mock's 1s heartbeat interval so healthy streams never trip it.
            "MZ_LAUNCHDARKLY_READ_TIMEOUT=5s",
        ],
        additional_system_parameter_defaults={
            "log_filter": "mz_adapter::config=debug,launchdarkly_server_sdk=debug,info",
        },
        external_metadata_store=True,
    ),
    # The reconnect (eventsource backoff) plus the 1s sync loop means the
    # updated value can take several seconds to land; give it ample room.
    Testdrive(no_reset=True, seed=1, default_timeout="120s"),
]


def workflow_default(c: Composition) -> None:
    c.up(MOCK_HOST, "materialized", Service("testdrive", idle=True))

    # The mock serves 2 GiB on the first two streaming connections and stalls
    # each until the SDK's read timeout fires, then serves 3 GiB to every
    # later (reconnecting) client. Reaching 3 GiB therefore proves the sync
    # client's data source reconnected after the mid-body timeout; a regressed
    # SDK stays stuck at 2 GiB and this assertion times out. We don't assert
    # the transient 2 GiB value, as the timeout can race startup.
    c.testdrive("\n".join(["> SHOW max_result_size", "3GB"]))
