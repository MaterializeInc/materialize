# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Thin confluent-kafka producer wrapper for Antithesis drivers.

Tracks the highest delivered offset per topic so drivers can poll Materialize
statistics for catchup. Retries delivery failures on partition; surfaces
permanent errors.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

LOG = logging.getLogger("antithesis.helper_kafka")

BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")

# Per-RPC and per-delivery timeouts for librdkafka. Default
# `request.timeout.ms` is 30s, which can fail entirely inside a single
# faults-ON window (MAX_ON defaults to 40s in the global fault-
# orchestrator). Bumping it gives one request a real chance of spanning
# the transition into the next quiet window before failing. librdkafka
# also requires `delivery.timeout.ms` to be >= `request.timeout.ms +
# linger.ms`; we pin both explicitly so the relationship is reviewable
# here rather than implicit. `delivery.timeout.ms` is the wall-clock
# budget the broker side of the producer has to either deliver or fail
# the message; idempotent retries happen under this umbrella, so the
# value needs to span at least one full ON+OFF cycle (~80s) plus
# margin.
_REQUEST_TIMEOUT_MS = 60_000
_DELIVERY_TIMEOUT_MS = 180_000

# Wall-clock budget for synchronous admin / flush waits. The orchestrator's
# longest faults-ON window is MAX_ON (40s default); 90s comfortably spans
# one such window plus catchup overhead.
ADMIN_TIMEOUT_S = 90

# Wall-clock budget for `producer.flush(timeout=...)` in drivers. Tuned to
# absorb at least one MAX_ON window so a produce burst that landed mid-
# fault still has time to drain after the orchestrator opens its next
# quiet window. Shorter than `_DELIVERY_TIMEOUT_MS` so a flush that
# returns with `pending > 0` is a strong signal the producer is still
# struggling, not that we just ran out of patience.
FLUSH_TIMEOUT_S = 90


@dataclass
class DeliveryTracker:
    """Records highest delivered offset per (topic, partition) and any error."""

    max_offset: dict[tuple[str, int], int] = field(default_factory=dict)
    last_error: KafkaException | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def callback(self, err, msg):
        if err is not None:
            with self._lock:
                self.last_error = KafkaException(err)
            LOG.warning("kafka delivery error: %s", err)
            return
        key = (msg.topic(), msg.partition())
        with self._lock:
            existing = self.max_offset.get(key, -1)
            if msg.offset() > existing:
                self.max_offset[key] = msg.offset()

    def topic_max_offset(self, topic: str) -> int:
        with self._lock:
            offsets = [o for (t, _), o in self.max_offset.items() if t == topic]
        return max(offsets) if offsets else -1


def make_producer(client_id: str | None = None) -> tuple[Producer, DeliveryTracker]:
    """Construct a Producer with a fresh DeliveryTracker."""
    config: dict[str, object] = {
        "bootstrap.servers": BROKER,
        "linger.ms": 5,
        "enable.idempotence": True,
        "acks": "all",
        # See module-level _REQUEST_TIMEOUT_MS / _DELIVERY_TIMEOUT_MS for
        # the fault-orchestrator-aware rationale on these values.
        "request.timeout.ms": _REQUEST_TIMEOUT_MS,
        "delivery.timeout.ms": _DELIVERY_TIMEOUT_MS,
    }
    if client_id:
        config["client.id"] = client_id
    LOG.info(
        "kafka producer: building (broker=%s client_id=%s request_timeout=%dms delivery_timeout=%dms)",
        BROKER,
        client_id or "<auto>",
        _REQUEST_TIMEOUT_MS,
        _DELIVERY_TIMEOUT_MS,
    )
    return Producer(config), DeliveryTracker()


# Retry budget for the whole `ensure_topic` block — the broker may be
# paused for the full fault-orchestrator MAX_ON window (~40s) so a
# single-shot admin call (`list_topics`/`create_topics`) inside one
# `ADMIN_TIMEOUT_S` ceiling can be smaller than the fault window and
# hard-fail.  Mirror the pgwire retry budget so admin-path retries
# span at least one full fault-ON + fault-OFF cycle plus margin.
_ENSURE_TOPIC_BUDGET_S = 180
_ENSURE_TOPIC_BACKOFF_INITIAL_S = 0.5
_ENSURE_TOPIC_BACKOFF_MAX_S = 4.0


def ensure_topic(topic: str, num_partitions: int = 1) -> None:
    """Create the topic if it doesn't already exist.

    Retries the whole `list → create` block against `_ENSURE_TOPIC_BUDGET_S`
    so a faults-ON window that exceeds `ADMIN_TIMEOUT_S` (single-shot
    admin timeout) doesn't propagate a hard failure all the way to the
    driver.  No-op on race with auto-create (TOPIC_ALREADY_EXISTS = 36
    is treated as success).  The retry loop reconstructs `AdminClient`
    from scratch each iteration so a torn-down librdkafka session
    doesn't bleed state across attempts.
    """
    LOG.info("kafka admin: probing topic %s (broker=%s)", topic, BROKER)
    deadline = time.monotonic() + _ENSURE_TOPIC_BUDGET_S
    backoff = _ENSURE_TOPIC_BACKOFF_INITIAL_S
    attempt = 0
    while True:
        attempt += 1
        try:
            _ensure_topic_once(topic, num_partitions)
            return
        except Exception as exc:  # noqa: BLE001
            if time.monotonic() > deadline:
                LOG.warning(
                    "kafka admin: ensure_topic giving up after %d attempts "
                    "(budget=%ds): %s",
                    attempt,
                    _ENSURE_TOPIC_BUDGET_S,
                    exc,
                )
                raise
            LOG.info(
                "kafka admin: ensure_topic attempt %d failed (%s); retrying in %.1fs",
                attempt,
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, _ENSURE_TOPIC_BACKOFF_MAX_S)


def _ensure_topic_once(topic: str, num_partitions: int) -> None:
    """Single-attempt `list → create` cycle.  See `ensure_topic`."""
    admin = AdminClient({"bootstrap.servers": BROKER})
    list_start = time.monotonic()
    existing = admin.list_topics(timeout=ADMIN_TIMEOUT_S).topics
    LOG.info(
        "kafka admin: list_topics returned %d topics in %.2fs",
        len(existing),
        time.monotonic() - list_start,
    )
    if topic in existing:
        LOG.info("kafka admin: topic %s already present; skipping create", topic)
        return
    LOG.info(
        "kafka admin: creating topic %s with %d partition(s)", topic, num_partitions
    )
    create_start = time.monotonic()
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=num_partitions, replication_factor=1)]
    )
    for t, fut in futures.items():
        try:
            fut.result(timeout=ADMIN_TIMEOUT_S)
            LOG.info(
                "kafka admin: topic %s created in %.2fs",
                t,
                time.monotonic() - create_start,
            )
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS = 36
            err = exc.args[0] if exc.args else None
            if err is not None and getattr(err, "code", lambda: None)() == 36:
                LOG.info("kafka admin: topic %s raced with auto-create; continuing", t)
                continue
            LOG.warning(
                "kafka admin: topic %s create failed in %.2fs: %s",
                t,
                time.monotonic() - create_start,
                exc,
            )
            raise
