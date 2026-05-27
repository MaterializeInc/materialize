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
from collections.abc import Callable
from contextlib import contextmanager
from typing import TYPE_CHECKING

import requests

from materialize.mzcompose.service import (
    Service,
)

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition


CONSENSUS_PROXY_NAME = "consensus"
"""Name used for the toxiproxy proxy that fronts the consensus connection."""

CONSENSUS_LATENCY_TOXIC = "latency"
"""Name used for the latency toxic applied to the consensus proxy."""


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


def setup_consensus_toxiproxy(
    c: "Composition",
    metadata_store: str = "postgres-metadata",
    latency_ms: int = 0,
    jitter_ms: int = 0,
    upstream_port: int = 26257,
    listen_port: int = 26257,
    toxiproxy_service: str = "toxiproxy",
) -> str:
    """Bring up toxiproxy + the metadata store and install a pass-through proxy
    in front of the consensus connection.

    A latency toxic is always installed (even when ``latency_ms`` and
    ``jitter_ms`` are 0) so callers can later bump the delay at runtime with
    :func:`set_consensus_latency` without having to know whether the toxic
    already exists.

    Pass ``latency_ms=0, jitter_ms=0`` (the default) during bootstrap so
    Materialize starts up quickly; bump the values later with
    :func:`set_consensus_latency`.

    Returns the service name to pass as ``external_metadata_store`` to
    :class:`Materialized` / :class:`Testdrive` (``"toxiproxy"`` by default).
    """
    c.up(toxiproxy_service, metadata_store)
    api_port = c.default_port(toxiproxy_service)

    # Idempotent: drop any pre-existing proxy of this name so callers can
    # re-invoke this helper without first tearing down toxiproxy
    # (e.g. feature-benchmark runs the same workflow twice — once per
    # this/other tag — without restarting the toxiproxy container).
    # DELETE returns 204 on success, 404 if absent; both are fine.
    requests.delete(f"http://localhost:{api_port}/proxies/{CONSENSUS_PROXY_NAME}")

    r = requests.post(
        f"http://localhost:{api_port}/proxies",
        json={
            "name": CONSENSUS_PROXY_NAME,
            "listen": f"0.0.0.0:{listen_port}",
            "upstream": f"{metadata_store}:{upstream_port}",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r.text

    # DELETE'ing the proxy above also drops its toxics, so create fresh.
    r = requests.post(
        f"http://localhost:{api_port}/proxies/{CONSENSUS_PROXY_NAME}/toxics",
        json={
            "name": CONSENSUS_LATENCY_TOXIC,
            "type": "latency",
            "attributes": {"latency": latency_ms, "jitter": jitter_ms},
        },
    )
    assert r.status_code == 200, r.text

    return toxiproxy_service


def set_consensus_latency(
    c: "Composition",
    latency_ms: int,
    jitter_ms: int,
    toxiproxy_service: str = "toxiproxy",
) -> None:
    """Update the latency toxic on the consensus proxy in place.

    Cheap to call; safe to call repeatedly. Requires that
    :func:`setup_consensus_toxiproxy` has already run.
    """
    api_port = c.default_port(toxiproxy_service)
    r = requests.post(
        f"http://localhost:{api_port}/proxies/{CONSENSUS_PROXY_NAME}/toxics/{CONSENSUS_LATENCY_TOXIC}",
        json={"attributes": {"latency": latency_ms, "jitter": jitter_ms}},
    )
    assert r.status_code == 200, r.text


@contextmanager
def consensus_latency(
    c: "Composition",
    latency_ms: int,
    jitter_ms: int,
    toxiproxy_service: str = "toxiproxy",
):
    """Bump the consensus latency toxic on entry, restore it to 0 on exit.

    Use this to keep setup (object creation, data population) running at
    full speed while injecting realistic latency only during the
    interesting phase of the test::

        setup_consensus_toxiproxy(c, metadata_store=c.metadata_store())
        c.up("materialized")
        # ... setup at 0 latency ...
        with consensus_latency(c, 5, 5):
            # ... the actual test ...
    """
    set_consensus_latency(c, latency_ms, jitter_ms, toxiproxy_service)
    try:
        yield
    finally:
        set_consensus_latency(c, 0, 0, toxiproxy_service)


def start_latency_chaos_daemon(
    name: str,
    set_latency: Callable[[int, int], None],
    rng: random.Random,
    baseline_latency_ms: int,
    baseline_jitter_ms: int,
    spike_latency_range: tuple[int, int] = (5000, 30000),
    spike_jitter_range: tuple[int, int] = (1000, 5000),
    interval_range: tuple[float, float] = (30.0, 120.0),
    duration_range: tuple[float, float] = (20.0, 60.0),
    spike_probability: float = 0.6,
) -> threading.Thread:
    """Start a background daemon that periodically spikes a latency
    toxic to multi-second values and then returns it to baseline.

    Steady latency exposes only races that depend on a constantly slow
    consensus connection. Persist races like the compute_import "lost
    lease?" panic instead require a *sudden* propagation delay so that
    the writer's view of a freshly registered lease lags long enough for
    a compactor to act on stale state. This daemon simulates that with
    occasional multi-second spikes between the baseline.

    The thread is marked daemon=True; it dies automatically when the
    main process exits at end of test.

    :param name: Label used in the chaos log output.
    :param set_latency: Callback that applies a (latency_ms, jitter_ms)
                        pair to the toxic of interest (consensus, blob,
                        etc.).
    :param rng: Random source. Seeding it from the test seed gives
                reproducible chaos schedules.
    :param baseline_latency_ms: Latency to restore between spikes.
    :param baseline_jitter_ms: Jitter to restore between spikes.
    :param spike_latency_range: Inclusive (lo, hi) sampling range for
                                each spike's latency, in milliseconds.
    :param spike_jitter_range: Inclusive (lo, hi) sampling range for
                               each spike's jitter, in milliseconds.
    :param interval_range: (min, max) seconds to wait between potential
                           spike opportunities.
    :param duration_range: (min, max) seconds each spike lasts.
    :param spike_probability: Probability of spiking at each opportunity.
    """

    def loop() -> None:
        while True:
            try:
                time.sleep(rng.uniform(*interval_range))
                if rng.random() >= spike_probability:
                    continue
                lat = rng.randint(*spike_latency_range)
                jit = rng.randint(*spike_jitter_range)
                dur = rng.uniform(*duration_range)
                print(
                    f"--- chaos[{name}]: spike latency={lat}ms "
                    f"jitter={jit}ms for {dur:.0f}s"
                )
                set_latency(lat, jit)
                time.sleep(dur)
                print(
                    f"--- chaos[{name}]: revert to baseline "
                    f"latency={baseline_latency_ms}ms "
                    f"jitter={baseline_jitter_ms}ms"
                )
                set_latency(baseline_latency_ms, baseline_jitter_ms)
            except Exception as e:
                # Don't let chaos crash the test run; just retry.
                print(f"--- chaos[{name}]: error, will retry: {e}")
                time.sleep(5.0)

    t = threading.Thread(target=loop, daemon=True, name=f"chaos-{name}")
    t.start()
    return t


def start_consensus_chaos_daemon(
    c: "Composition",
    rng: random.Random,
    baseline_latency_ms: int,
    baseline_jitter_ms: int,
    toxiproxy_service: str = "toxiproxy",
    **kwargs,
) -> threading.Thread:
    """Convenience wrapper that perturbs the consensus latency toxic.
    Forwards kwargs to :func:`start_latency_chaos_daemon`.
    """
    return start_latency_chaos_daemon(
        name="consensus",
        set_latency=lambda lat, jit: set_consensus_latency(
            c, lat, jit, toxiproxy_service
        ),
        rng=rng,
        baseline_latency_ms=baseline_latency_ms,
        baseline_jitter_ms=baseline_jitter_ms,
        **kwargs,
    )
