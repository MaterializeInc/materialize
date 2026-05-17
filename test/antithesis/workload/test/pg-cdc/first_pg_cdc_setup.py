#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first_ command: configure the upstream PG and create the
Materialize Postgres CDC source.

Runs once per Antithesis timeline before any parallel/singleton drivers.
Steps:
  1. Wait for the upstream PG to accept connections.
  2. CREATE TABLE public.cdc_test, set REPLICA IDENTITY FULL (so DELETEs
     surface every column, not just the PK).
  3. CREATE PUBLICATION antithesis_pub FOR TABLE public.cdc_test.
  4. Create the Materialize-side secret/connection/source/table via
     helper_pg_source.ensure_pg_cdc_source.

REPLICA IDENTITY FULL is what the parallel driver's `_check_rows` semantic
relies on — without it, Materialize sees the new row image on inserts but
only the PK on deletes, which is fine for the count assertion but limits
what we can validate downstream.
"""

from __future__ import annotations

import sys

import helper_logging
import helper_pg_upstream
from helper_pg_source import (
    UPSTREAM_PUBLICATION,
    UPSTREAM_SCHEMA,
    UPSTREAM_TABLE,
    ensure_pg_cdc_source,
)

from antithesis.assertions import reachable

LOG = helper_logging.setup_logging("first.pg_cdc_setup")


def setup_upstream() -> None:
    """Create the cdc_test table and publication on the upstream PG."""
    LOG.info("creating %s.%s on upstream PG", UPSTREAM_SCHEMA, UPSTREAM_TABLE)
    # The data-loss workload owns its own schema (not `public`) so the
    # testdrive-runner drivers can own `public` exclusively.
    helper_pg_upstream.execute(f"CREATE SCHEMA IF NOT EXISTS {UPSTREAM_SCHEMA}")
    helper_pg_upstream.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {UPSTREAM_SCHEMA}.{UPSTREAM_TABLE} (
            id          TEXT PRIMARY KEY,
            batch_id    TEXT NOT NULL,
            value       TEXT NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # REPLICA IDENTITY FULL: send the entire old row in every UPDATE/DELETE
    # record. Without it, DELETE only carries the PK, which is enough for
    # CDC correctness but means the upstream row image and the
    # Materialize row image only line up on INSERT — making it harder to
    # write per-row assertions that survive the full mutation cycle.
    helper_pg_upstream.execute(
        f"ALTER TABLE {UPSTREAM_SCHEMA}.{UPSTREAM_TABLE} REPLICA IDENTITY FULL"
    )

    # CREATE PUBLICATION isn't IF-NOT-EXISTS friendly in PG, so we
    # idempotency-guard via the catalog.
    rows = helper_pg_upstream.query(
        "SELECT 1 FROM pg_publication WHERE pubname = %s",
        (UPSTREAM_PUBLICATION,),
    )
    if not rows:
        helper_pg_upstream.execute(
            f"CREATE PUBLICATION {UPSTREAM_PUBLICATION} "
            f"FOR TABLE {UPSTREAM_SCHEMA}.{UPSTREAM_TABLE}"
        )
        LOG.info("publication %s created", UPSTREAM_PUBLICATION)
    else:
        LOG.info(
            "publication %s already present; skipping create", UPSTREAM_PUBLICATION
        )


def main() -> int:
    LOG.info("waiting for upstream PG (%s)...", helper_pg_upstream.PG_HOST)
    helper_pg_upstream.wait_until_ready()

    setup_upstream()
    ensure_pg_cdc_source()

    reachable(
        "pg: first-run setup complete — upstream PG seeded, Materialize source created",
        {
            "upstream": helper_pg_upstream.PG_HOST,
            "publication": UPSTREAM_PUBLICATION,
        },
    )
    LOG.info("Postgres CDC setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
