# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Persist consensus backend configuration for mzcompose.

Controls which consensus backend persist uses, independently of the metadata
store (which also provides the timestamp oracle and adapter stash).

Set ``PERSIST_CONSENSUS_BACKEND=shared-log`` to swap in the persist shared log
gRPC service as the consensus backend. All other values (or unset) leave the
default behavior: persist consensus comes from the metadata store.
"""

from __future__ import annotations

import os

from materialize.mzcompose.service import Service
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    is_external_metadata_store,
)
from materialize.mzcompose.services.persist_shared_log import PersistSharedLog


def persist_consensus_backend() -> str | None:
    """Returns the configured persist consensus backend, or None for the default."""
    backend = os.getenv("PERSIST_CONSENSUS_BACKEND")
    if backend == "shared-log":
        return backend
    return None


def persist_consensus_url() -> str | None:
    """Returns the persist consensus URL override, or None for the default."""
    if persist_consensus_backend() == "shared-log":
        return f"rpc://persist-shared-log:{PersistSharedLog.DEFAULT_PORT}"
    return None


PERSIST_CONSENSUS_BACKEND: str | None = persist_consensus_backend()
"""The configured persist consensus backend, or None for the default."""

PERSIST_CONSENSUS_URL: str | None = persist_consensus_url()
"""The persist consensus URL override, or None to use the metadata store."""


def persist_consensus_services() -> list[Service]:
    """Returns the service(s) needed for the configured persist consensus backend.

    When ``PERSIST_CONSENSUS_BACKEND=shared-log``, returns a
    ``[PersistSharedLog()]`` configured to use the metadata store as its own
    persist backing storage. Otherwise returns an empty list.

    Add ``*persist_consensus_services()`` to your composition's ``SERVICES``
    list alongside ``*metadata_store_services()``.
    """
    if PERSIST_CONSENSUS_BACKEND != "shared-log":
        return []

    # The shared log service needs its own persist backend. Use the metadata
    # store (Postgres/CRDB/AlloyDB) for consensus, and local file storage for
    # blob. This ensures the shared log's state survives container restarts.
    if is_external_metadata_store(METADATA_STORE):
        consensus_url = f"postgres://root@{METADATA_STORE}:26257?options=--search_path=shared_log_consensus"
        blob_url = "file:///mzdata/persist/blob"
        depends_on = {METADATA_STORE: {"condition": "service_healthy"}}
    else:
        # Internal metadata store: fall back to in-memory (tests only).
        consensus_url = None
        blob_url = None
        depends_on = None

    return [
        PersistSharedLog(
            blob_url=blob_url,
            consensus_url=consensus_url,
            depends_on=depends_on,
        )
    ]
