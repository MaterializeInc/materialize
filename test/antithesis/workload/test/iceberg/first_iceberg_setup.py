#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first command for the `iceberg` group.

Antithesis selects exactly one `first_*` per execution history.  The
iceberg group has two logical setup steps that every driver depends on:

  1. Polaris + minio bucket bootstrap (shared with parallel-workload;
     `helper_polaris_setup`).
  2. The MZ-side secret / aws_conn / polaris_conn / source table /
     sink, via `helper_iceberg.ensure_iceberg_objects`.

Chaining them under a single `first_*` command means every execution
history gets both, instead of half the histories running each one
alone.  Each step is idempotent on re-entry.
"""

from __future__ import annotations

import sys

import helper_iceberg
import helper_logging
import helper_polaris_setup
from antithesis.assertions import reachable

LOG = helper_logging.setup_logging("first.iceberg_setup")


def main() -> int:
    LOG.info("iceberg group first command: polaris -> mz objects")
    helper_polaris_setup.main()
    helper_iceberg.ensure_iceberg_objects()
    reachable(
        "iceberg: first-run setup complete — polaris ready, MZ sink created",
        {
            "catalog": helper_iceberg.CATALOG_NAME,
            "namespace": helper_iceberg.NAMESPACE_NAME,
            "sink": helper_iceberg.SINK_NAME,
            "iceberg_table": helper_iceberg.ICEBERG_TABLE,
        },
    )
    LOG.info("iceberg setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
