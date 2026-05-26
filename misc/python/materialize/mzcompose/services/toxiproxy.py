# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import random
import threading
import time
from typing import TYPE_CHECKING

import requests

from materialize.mzcompose.service import (
    Service,
)

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition


# Proxy names. Stable so callers can target the same proxy across
# multiple helper invocations.
CONSENSUS_PROXY_NAME = "consensus"
MINIO_PROXY_NAME = "minio"
AZURITE_PROXY_NAME = "azurite"

# One latency toxic per proxy, scoped by name.
_LATENCY_TOXIC = "latency"


class Toxiproxy(Service):
    def __init__(
        self,
        name: str = "toxiproxy",
        image: str = "jauderho/toxiproxy:v2.8.0",
        port: int = 8474,
        seed: int = random.randrange(2**63),
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "command": ["-host=0.0.0.0", f"-seed={seed}"],
                "ports": [port],
                "healthcheck": {
                    "test": ["CMD", "nc", "-z", "localhost", "8474"],
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )


def _api_port(c: "Composition", toxiproxy_service: str) -> int:
    return c.default_port(toxiproxy_service)


def _create_proxy(
    c: "Composition",
    name: str,
    listen_port: int,
    upstream_host: str,
    upstream_port: int,
    latency_ms: int,
    jitter_ms: int,
    toxiproxy_service: str,
) -> None:
    """Create or replace a toxiproxy proxy and install a latency toxic.

    Idempotent: any pre-existing proxy with this name is dropped first
    so callers can re-invoke this helper without first tearing down
    toxiproxy.
    """
    port = _api_port(c, toxiproxy_service)
    # DELETE returns 204 on success, 404 if absent; either is fine.
    requests.delete(f"http://localhost:{port}/proxies/{name}")

    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": name,
            "listen": f"0.0.0.0:{listen_port}",
            "upstream": f"{upstream_host}:{upstream_port}",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r.text

    r = requests.post(
        f"http://localhost:{port}/proxies/{name}/toxics",
        json={
            "name": _LATENCY_TOXIC,
            "type": "latency",
            "attributes": {"latency": latency_ms, "jitter": jitter_ms},
        },
    )
    assert r.status_code == 200, r.text


def setup_consensus_toxiproxy(
    c: "Composition",
    metadata_store: str = "postgres-metadata",
    latency_ms: int = 0,
    jitter_ms: int = 0,
    upstream_port: int = 26257,
    listen_port: int = 26257,
    toxiproxy_service: str = "toxiproxy",
) -> str:
    """Bring up toxiproxy + the metadata store and install a proxy in
    front of the consensus connection.

    Returns the service name (``"toxiproxy"``) to pass as
    ``external_metadata_store`` to :class:`Materialized`.
    """
    c.up(toxiproxy_service, metadata_store)
    _create_proxy(
        c,
        name=CONSENSUS_PROXY_NAME,
        listen_port=listen_port,
        upstream_host=metadata_store,
        upstream_port=upstream_port,
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
        toxiproxy_service=toxiproxy_service,
    )
    return toxiproxy_service


def setup_blob_toxiproxy(
    c: "Composition",
    latency_ms: int = 0,
    jitter_ms: int = 0,
    minio_upstream: str = "minio",
    minio_port: int = 9000,
    azurite_upstream: str = "azurite",
    azurite_port: int = 10000,
    toxiproxy_service: str = "toxiproxy",
) -> None:
    """Create toxiproxy proxies in front of minio and azurite.

    Both proxies get their own latency toxic. (Prior ad-hoc setups in
    test/parallel-workload mistakenly stacked two toxics on the minio
    proxy and left azurite unproxied; this helper avoids that.)
    """
    _create_proxy(
        c,
        name=MINIO_PROXY_NAME,
        listen_port=minio_port,
        upstream_host=minio_upstream,
        upstream_port=minio_port,
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
        toxiproxy_service=toxiproxy_service,
    )
    _create_proxy(
        c,
        name=AZURITE_PROXY_NAME,
        listen_port=azurite_port,
        upstream_host=azurite_upstream,
        upstream_port=azurite_port,
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
        toxiproxy_service=toxiproxy_service,
    )


def _update_latency(
    c: "Composition",
    proxy: str,
    latency_ms: int,
    jitter_ms: int,
    toxiproxy_service: str,
) -> None:
    port = _api_port(c, toxiproxy_service)
    r = requests.post(
        f"http://localhost:{port}/proxies/{proxy}/toxics/{_LATENCY_TOXIC}",
        json={"attributes": {"latency": latency_ms, "jitter": jitter_ms}},
    )
    assert r.status_code == 200, r.text


def set_consensus_latency(
    c: "Composition",
    latency_ms: int,
    jitter_ms: int,
    toxiproxy_service: str = "toxiproxy",
) -> None:
    """Update the consensus proxy's latency toxic in place."""
    _update_latency(
        c, CONSENSUS_PROXY_NAME, latency_ms, jitter_ms, toxiproxy_service
    )


def set_blob_latency(
    c: "Composition",
    latency_ms: int,
    jitter_ms: int,
    toxiproxy_service: str = "toxiproxy",
) -> None:
    """Update both blob proxies' latency toxics in place."""
    _update_latency(
        c, MINIO_PROXY_NAME, latency_ms, jitter_ms, toxiproxy_service
    )
    _update_latency(
        c, AZURITE_PROXY_NAME, latency_ms, jitter_ms, toxiproxy_service
    )


def set_proxy_enabled(
    c: "Composition",
    proxy: str,
    enabled: bool,
    toxiproxy_service: str = "toxiproxy",
) -> None:
    """Enable or disable a proxy. Disabling drops all in-flight
    connections and refuses new ones until re-enabled.

    This is the core primitive for "consensus blackout"-style chaos:
    flipping enabled=False for a few seconds creates a hard outage
    window, which is the most direct way to expose lost-lease races
    (the lease can't be refreshed -> it expires -> compactor GCs parts
    the reader still holds a stale view of).
    """
    port = _api_port(c, toxiproxy_service)
    r = requests.post(
        f"http://localhost:{port}/proxies/{proxy}",
        json={"enabled": enabled},
    )
    assert r.status_code == 200, r.text


def start_disruption_daemon(
    c: "Composition",
    rng: random.Random,
    proxy: str,
    interval_range: tuple[float, float] = (30.0, 90.0),
    duration_range: tuple[float, float] = (12.0, 30.0),
    disruption_probability: float = 0.5,
    toxiproxy_service: str = "toxiproxy",
) -> threading.Thread:
    """Start a daemon thread that periodically drops a single proxy
    for a few seconds, then re-enables it.

    The default 12-30 second outage window is deliberately chosen to
    be long enough to outlast a persist reader lease when that lease
    is shortened to ~10 seconds (see ``persist_reader_lease_duration``
    in the test's system parameter overrides), but short enough that
    normal statement timeouts (60-300s in testdrive) are not exceeded.

    Run multiple instances (one per proxy) to disrupt several
    independently — useful when you want, say, a high-frequency blob
    outage daemon overlapping a separate consensus blackout daemon to
    bias the failure mode toward fetch-time races.

    The thread is daemon=True; it dies with the main process when the
    test ends.

    :param proxy: Name of the proxy to disrupt.
    :param interval_range: (min, max) seconds between disruption
                           opportunities.
    :param duration_range: (min, max) seconds each disruption lasts.
    :param disruption_probability: Probability of actually disrupting
                                   at each tick.
    """

    def loop() -> None:
        while True:
            try:
                time.sleep(rng.uniform(*interval_range))
                if rng.random() >= disruption_probability:
                    continue
                duration = rng.uniform(*duration_range)
                print(
                    f"--- chaos: disabling {proxy} proxy for "
                    f"{duration:.1f}s"
                )
                set_proxy_enabled(c, proxy, False, toxiproxy_service)
                time.sleep(duration)
                print(f"--- chaos: re-enabling {proxy} proxy")
                set_proxy_enabled(c, proxy, True, toxiproxy_service)
            except Exception as e:
                # Don't let chaos crash the test run; retry after a
                # brief pause.
                print(f"--- chaos: error, will retry: {e}")
                time.sleep(5.0)

    t = threading.Thread(target=loop, daemon=True, name=f"chaos-{proxy}")
    t.start()
    return t


def start_slowdown_daemon(
    c: "Composition",
    rng: random.Random,
    proxy: str,
    baseline_latency_ms: int,
    baseline_jitter_ms: int,
    slow_latency_ms: int,
    slow_jitter_ms: int,
    interval_range: tuple[float, float] = (20.0, 60.0),
    duration_range: tuple[float, float] = (20.0, 45.0),
    slowdown_probability: float = 0.7,
    toxiproxy_service: str = "toxiproxy",
) -> threading.Thread:
    """Start a daemon thread that periodically cranks a proxy's
    latency toxic to a much higher value, then restores it.

    Unlike :func:`start_disruption_daemon` (which disables the proxy
    entirely), this leaves the connection *up* but very slow. That
    matters when the goal is to keep readers stuck in active blob
    fetches without preventing the compactor from actually deleting
    GC'd parts - which is the timing recipe for the
    BatchFetcher "lost lease?" panic (vs. the Listen-path variant
    that fires when the proxy is fully blacked out).
    """

    def loop() -> None:
        while True:
            try:
                time.sleep(rng.uniform(*interval_range))
                if rng.random() >= slowdown_probability:
                    continue
                duration = rng.uniform(*duration_range)
                print(
                    f"--- chaos: slowing {proxy} to "
                    f"{slow_latency_ms}/{slow_jitter_ms}ms for "
                    f"{duration:.1f}s"
                )
                _update_latency(
                    c,
                    proxy,
                    slow_latency_ms,
                    slow_jitter_ms,
                    toxiproxy_service,
                )
                time.sleep(duration)
                print(
                    f"--- chaos: restoring {proxy} to baseline "
                    f"{baseline_latency_ms}/{baseline_jitter_ms}ms"
                )
                _update_latency(
                    c,
                    proxy,
                    baseline_latency_ms,
                    baseline_jitter_ms,
                    toxiproxy_service,
                )
            except Exception as e:
                print(f"--- chaos: error, will retry: {e}")
                time.sleep(5.0)

    t = threading.Thread(target=loop, daemon=True, name=f"slowdown-{proxy}")
    t.start()
    return t
