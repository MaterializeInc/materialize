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
from dataclasses import dataclass, field

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

LOG = logging.getLogger("antithesis.helper_kafka")

BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")


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
    }
    if client_id:
        config["client.id"] = client_id
    return Producer(config), DeliveryTracker()


def ensure_topic(topic: str, num_partitions: int = 1) -> None:
    """Create the topic if it doesn't already exist. No-op on race with auto-create."""
    admin = AdminClient({"bootstrap.servers": BROKER})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        return
    LOG.info("creating kafka topic %s with %d partition(s)", topic, num_partitions)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=num_partitions, replication_factor=1)]
    )
    for t, fut in futures.items():
        try:
            fut.result(timeout=30)
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS = 36
            err = exc.args[0] if exc.args else None
            if err is not None and getattr(err, "code", lambda: None)() == 36:
                LOG.info("kafka topic %s raced with auto-create; continuing", t)
                continue
            raise
