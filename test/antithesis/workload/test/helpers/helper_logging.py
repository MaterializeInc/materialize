# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Logging setup with per-invocation correlation IDs for Antithesis drivers.

Antithesis launches each Test Composer command as a fresh process, and
many drivers run concurrently in the same timeline. Correlating log
records back to a specific invocation — "which of the eight in-flight
parallel_driver_upsert_latest_value processes was the one that stalled
on flush?" — requires a stable per-invocation token that appears in
every line.

Use:

    import helper_logging
    LOG = helper_logging.setup_logging("driver.my_driver")
    LOG.info("starting; prefix=%s", prefix)
    # 2026-... INFO driver.my_driver [inv=a3f9b1c2] starting; prefix=p...

The same INVOCATION_ID is exported as a module attribute so drivers can
include it in assertion-detail dicts, subprocess invocations, Kafka
client.ids, and anywhere else a correlation token is useful.
"""

from __future__ import annotations

import logging
import os
import secrets

# Short hex string minted once per process at import time. 32 bits is
# enough to make collisions vanishingly rare across concurrent drivers
# in one timeline; staying short keeps it readable inline in every line.
INVOCATION_ID = secrets.token_hex(4)


def setup_logging(name: str | None = None) -> logging.Logger:
    """Install a root handler with the invocation-ID-stamped formatter and
    return a named logger.

    Idempotent: if the root already has a handler, the configuration is
    left alone and only the named logger is returned. This means the
    first driver/helper to call `setup_logging` in a process wins the
    format, which is fine because every caller passes the same format
    template.

    Log level defaults to INFO; override via `LOG_LEVEL` env var (e.g.
    `LOG_LEVEL=DEBUG` to see per-attempt retry logs the helpers emit).
    """
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                fmt=(
                    f"%(asctime)s %(levelname)s %(name)s "
                    f"[inv={INVOCATION_ID}] %(message)s"
                )
            )
        )
        root.addHandler(handler)
        root.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    return logging.getLogger(name)
