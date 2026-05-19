# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.helpers.iceberg`. See package __init__.py."""

from __future__ import annotations

from typing import Any


def setup_polaris_for_iceberg(c: Any, *args: Any, **kwargs: Any) -> tuple[str, str]:
    # `parallel_workload.database` imports this at module load time, so the
    # stub has to exist even though Polaris IS now in our topology (added
    # for the parallel-workload + combined groups so CreateIcebergSinkAction
    # no longer silent-fails on `unknown catalog item polaris_conn`).
    #
    # Upstream's `setup_polaris_for_iceberg` provisions a Polaris principal
    # via the admin API and returns its (access_key, secret_key) for
    # `Database.create` to build a per-principal `aws_conn` against minio.
    # Our driver doesn't do that dance — `_create_database_for_antithesis`
    # creates `polaris_conn` directly with the bootstrap `root:root`
    # credentials and `aws_conn` with the minio root creds, skipping the
    # principal layer.  We never call upstream `Database.create` (we use the
    # stand-in instead), so this function is unreachable in practice.
    # Raise loudly if anything ever does call it — that's a regression.
    raise RuntimeError(
        "setup_polaris_for_iceberg() stub: the Antithesis driver provisions "
        "polaris_conn and aws_conn directly via _create_database_for_antithesis "
        "and does not use upstream's principal-creation flow."
    )
