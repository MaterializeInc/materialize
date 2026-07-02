# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Toxiproxy administration and the disruptor thread.

A `Leg` is one logical connection of the system under test (e.g. the
envd<->clusterd gRPC pair, or the source's Postgres connection), backed by
one or more toxiproxy proxies that are always disrupted and healed together.
Every disruption is paired with its heal, and stopping the disruptor heals
everything, so the converge phase always starts from a clean network.
"""

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import requests

from materialize.invariants.framework import EventLog, TransientError

DISRUPTION_KINDS = ["disable", "latency", "timeout", "limit_data", "bandwidth"]

# Kinds that cut the connection entirely, subject to Leg.max_outage.
FULL_OUTAGE_KINDS = {"disable", "timeout"}


@dataclass(frozen=True)
class Proxy:
    name: str
    listen_port: int
    upstream: str


@dataclass(frozen=True)
class Leg:
    name: str
    proxies: tuple[Proxy, ...]
    # Cap on full-outage disruptions. The metadata leg fronts persist
    # consensus, whose reader leases expire after 15 minutes and make
    # clusterd halt, so its outages must stay well below that.
    max_outage: float | None = None
    # Allowed disruption kinds, None means all. High-volume legs (persist
    # blob) exclude the buffering toxics: latency and bandwidth hold the
    # leg's entire in-flight traffic in toxiproxy's memory.
    kinds: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ProcessTarget:
    """A process the disruptor may SIGKILL or SIGSTOP, with paired heals.

    `heal` must be idempotent and block until the process serves again (for
    containers with a restart policy it can be a no-op up()). Pauses are
    capped like full outages: a paused clusterd stops renewing its persist
    leases, which expire after 15 minutes.
    """

    name: str
    kill: Callable[[], None]
    heal: Callable[[], None]
    pause: Callable[[], None]
    unpause: Callable[[], None]
    max_outage: float = 120.0


class ToxiproxyApi:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()

    def create(self, proxy: Proxy) -> None:
        r = self.session.post(
            f"{self.base_url}/proxies",
            json={
                "name": proxy.name,
                "listen": f"0.0.0.0:{proxy.listen_port}",
                "upstream": proxy.upstream,
                "enabled": True,
            },
            timeout=30,
        )
        assert r.status_code == 201, f"creating proxy {proxy.name}: {r} {r.text}"

    def set_enabled(self, proxy_name: str, enabled: bool) -> None:
        r = self.session.post(
            f"{self.base_url}/proxies/{proxy_name}",
            json={"enabled": enabled},
            timeout=30,
        )
        assert r.status_code == 200, f"toggling proxy {proxy_name}: {r} {r.text}"

    def add_toxic(
        self,
        proxy_name: str,
        name: str,
        type_: str,
        attributes: dict,
        stream: str = "downstream",
    ) -> None:
        r = self.session.post(
            f"{self.base_url}/proxies/{proxy_name}/toxics",
            json={
                "name": name,
                "type": type_,
                "stream": stream,
                "attributes": attributes,
            },
            timeout=30,
        )
        assert (
            r.status_code == 200
        ), f"adding toxic {name} to {proxy_name}: {r} {r.text}"

    def delete_toxic(self, proxy_name: str, name: str) -> None:
        r = self.session.delete(
            f"{self.base_url}/proxies/{proxy_name}/toxics/{name}", timeout=30
        )
        assert r.status_code in (200, 204), f"deleting toxic {name}: {r} {r.text}"

    def reset(self) -> None:
        """Re-enable all proxies and remove all toxics."""
        r = self.session.post(f"{self.base_url}/reset", timeout=30)
        assert r.status_code == 204, f"toxiproxy reset: {r} {r.text}"

    def proxies(self) -> dict:
        r = self.session.get(f"{self.base_url}/proxies", timeout=30)
        assert r.status_code == 200, f"listing proxies: {r} {r.text}"
        return r.json()

    def assert_healed(self) -> None:
        for name, proxy in self.proxies().items():
            assert proxy["enabled"], f"proxy {name} still disabled"
            assert not proxy[
                "toxics"
            ], f"proxy {name} still has toxics: {proxy['toxics']}"


class Disruptor(threading.Thread):
    """Applies random disruption/heal cycles to the scenario's legs."""

    def __init__(
        self,
        api: ToxiproxyApi,
        legs: list[Leg],
        rng: random.Random,
        log: EventLog,
        interval: tuple[float, float],
        duration: tuple[float, float],
        concurrent: int,
        on_error: Callable[[Exception], None],
        processes: list[ProcessTarget] | None = None,
    ) -> None:
        super().__init__(name="disruptor")
        self.api = api
        self.legs = legs
        self.rng = rng
        self.log = log
        self.interval = interval
        self.duration = duration
        self.concurrent = concurrent
        self.on_error = on_error
        self.processes = processes or []
        self.stop_event = threading.Event()
        self.cycles = 0
        # Written only by this thread, read by others after join or for
        # diagnostics (appends are atomic enough for that purpose).
        self.history: list[str] = []

    def _record(self, message: str) -> None:
        self.history.append(f"{time.strftime('%H:%M:%S')} {message}")
        self.log.log("disrupt", message)

    def run(self) -> None:
        try:
            while not self.stop_event.wait(self.rng.uniform(*self.interval)):
                # Occasionally a storm: several short back-to-back
                # disruptions followed by a longer calm window, verifying
                # that the system recovers repeatedly, not just once at the
                # end of the run.
                if self.rng.random() < 0.2:
                    self._record("storm starting")
                    for _ in range(self.rng.randint(2, 4)):
                        self._one_cycle(duration_scale=0.4)
                        if self.stop_event.wait(self.rng.uniform(1.0, 5.0)):
                            return
                    self._record("storm over, calm window")
                    if self.stop_event.wait(self.rng.uniform(*self.interval)):
                        return
                else:
                    self._one_cycle()
        except Exception as e:
            self.on_error(e)
        finally:
            self._heal_all_with_retries()

    def _one_cycle(self, duration_scale: float = 1.0) -> None:
        try:
            if self.processes and self.rng.random() < 0.25:
                self._process_cycle(duration_scale)
            else:
                self._leg_cycle(duration_scale)
        except requests.RequestException as e:
            # The toxiproxy admin API can stall while the host is overloaded
            # (e.g. right after an envd restart). A lost cycle is not a
            # failure, but nothing may stay disrupted.
            self._record(f"cycle failed ({e}), healing everything")
            self._heal_all_with_retries()
        self.cycles += 1

    def _leg_cycle(self, duration_scale: float) -> None:
        count = min(len(self.legs), self.rng.randint(1, self.concurrent))
        targets = self.rng.sample(self.legs, count)
        duration = self.rng.uniform(*self.duration) * duration_scale
        applied: list[tuple[Leg, str]] = []
        for leg in targets:
            kind = self.rng.choice(list(leg.kinds or DISRUPTION_KINDS))
            if kind in FULL_OUTAGE_KINDS and leg.max_outage is not None:
                duration = min(duration, leg.max_outage)
            self._apply(leg, kind)
            applied.append((leg, kind))
        self._record(
            "applied "
            + ", ".join(f"{kind} on {leg.name}" for leg, kind in applied)
            + f" for {duration:.1f}s"
        )
        self.stop_event.wait(duration)
        for leg, kind in applied:
            self._heal(leg, kind)
        self._record(
            "healed " + ", ".join(f"{kind} on {leg.name}" for leg, kind in applied)
        )

    def _process_cycle(self, duration_scale: float) -> None:
        target = self.rng.choice(self.processes)
        kind = self.rng.choice(["kill", "pause"])
        duration = min(
            self.rng.uniform(*self.duration) * duration_scale, target.max_outage
        )
        self._record(f"applied {kind} on process {target.name} for {duration:.1f}s")
        if kind == "kill":
            target.kill()
            self.stop_event.wait(duration)
            target.heal()
        else:
            target.pause()
            self.stop_event.wait(duration)
            target.unpause()
        self._record(f"healed {kind} on process {target.name}")

    def _apply(self, leg: Leg, kind: str) -> None:
        # Toxics attach to one direction only, so half of the disruptions
        # are asymmetric: one side of the connection keeps hearing the other.
        stream = self.rng.choice(["downstream", "upstream"])
        for proxy in leg.proxies:
            if kind == "disable":
                self.api.set_enabled(proxy.name, False)
            elif kind == "latency":
                self.api.add_toxic(
                    proxy.name,
                    kind,
                    "latency",
                    {
                        "latency": self.rng.randint(100, 3000),
                        "jitter": self.rng.randint(0, 1000),
                    },
                    stream=stream,
                )
            elif kind == "timeout":
                # timeout=0 holds the connection open and drops all data.
                self.api.add_toxic(
                    proxy.name, kind, "timeout", {"timeout": 0}, stream=stream
                )
            elif kind == "limit_data":
                self.api.add_toxic(
                    proxy.name,
                    kind,
                    "limit_data",
                    {"bytes": self.rng.randint(128, 65536)},
                    stream=stream,
                )
            elif kind == "bandwidth":
                self.api.add_toxic(
                    proxy.name,
                    kind,
                    "bandwidth",
                    {"rate": self.rng.randint(1, 64)},
                    stream=stream,
                )
            else:
                raise ValueError(f"unknown disruption kind {kind}")

    def _heal(self, leg: Leg, kind: str) -> None:
        for proxy in leg.proxies:
            if kind == "disable":
                self.api.set_enabled(proxy.name, True)
            else:
                self.api.delete_toxic(proxy.name, kind)

    def _heal_all_with_retries(self) -> None:
        for target in self.processes:
            try:
                target.unpause()
            except Exception:
                pass
            try:
                target.heal()
            except Exception as e:
                self.on_error(TransientError(f"failed to heal {target.name}: {e}"))
        deadline = time.monotonic() + 60
        while True:
            try:
                self.api.reset()
                # A crashed and restarted toxiproxy comes back empty. The
                # legs' proxies must exist again before anything reconnects.
                existing = self.api.proxies()
                for leg in self.legs:
                    for proxy in leg.proxies:
                        if proxy.name not in existing:
                            self._record(f"re-creating lost proxy {proxy.name}")
                            self.api.create(proxy)
                self.api.assert_healed()
                self._record("all legs healed")
                return
            except Exception as e:
                if time.monotonic() > deadline:
                    self.on_error(
                        TransientError(f"failed to heal toxiproxy state: {e}")
                    )
                    return
                time.sleep(1)

    def stop_and_heal(self) -> None:
        self.stop_event.set()
        self.join(timeout=60)
        if self.is_alive():
            self.log.log("disrupt", "disruptor thread failed to stop in time")
        else:
            # run() already healed in its finally block, but verify.
            try:
                self.api.assert_healed()
            except Exception:
                self._heal_all_with_retries()
