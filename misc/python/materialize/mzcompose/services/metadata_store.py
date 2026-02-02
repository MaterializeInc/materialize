# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Metadata store configuration for mzcompose.

This module provides utilities for selecting and configuring the metadata
store backend (CockroachDB, FoundationDB, or PostgreSQL) used by Materialize.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from materialize.mzcompose.service import Service
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.foundationdb import (
    FDB_NUM_NODES,
    FoundationDB,
    fdb_coordinator_addresses,
    foundationdb_services,
)

if TYPE_CHECKING:
    from materialize.mzcompose.services.postgres import PostgresMetadata


def metadata_store_name() -> str:
    """
    Determines the metadata store to use.

    Defaults to 'postgres-metadata' unless the BUILDKITE_TAG environment variable is set,
    in which case 'cockroach' is used, or the value of the EXTERNAL_METADATA_STORE environment variable if set.
    """
    if os.getenv("BUILDKITE_TAG", "") != "":
        return "cockroach"
    return os.getenv("EXTERNAL_METADATA_STORE", "postgres-metadata")


def metadata_store_service(
    metadata_store: str,
) -> type[Cockroach | FoundationDB | PostgresMetadata] | None:
    """
    Returns the service class corresponding to the specified metadata store, or None if an internal store is used.
    :param metadata_store: The name of the metadata store.
    """
    # Import here to avoid circular imports
    from materialize.mzcompose.services.postgres import PostgresMetadata

    match metadata_store:
        case "cockroach":
            return Cockroach
        case "foundationdb":
            return FoundationDB
        case "postgres-metadata":
            return PostgresMetadata
        case "postgres-internal":
            return None
        case _:
            raise ValueError(f"Unknown METADATA_STORE: {metadata_store}")


def is_external_metadata_store(metadata_store: str) -> bool:
    """
    Determines if the specified metadata store is external, i.e., it runs in a different container than
    `environmentd`.
    :param metadata_store: The name of the metadata store.
    """
    match metadata_store:
        case "cockroach" | "foundationdb" | "postgres-metadata":
            return True
        case "postgres-internal":
            return False
        case _:
            raise ValueError(f"Unknown METADATA_STORE: {metadata_store}")


METADATA_STORE: str = metadata_store_name()
""" Global metadata store configuration. """

METADATA_STORE_SERVICE = metadata_store_service(METADATA_STORE)
""" Global metadata store service class. None if an internal store is used. """


def external_metadata_store() -> str | bool:
    """
    Returns the appropriate external_metadata_store value for Materialized/Testdrive.

    - For multi-node FoundationDB: returns coordinator addresses string
    - For other external stores: returns True
    - For internal stores: returns False
    """
    # We'd like this to be `is_external_metadata_store(METADATA_STORE)`, but
    # can't as `METADATA_STORE` is always set.
    name = os.getenv("EXTERNAL_METADATA_STORE", "postgres-internal")
    if not is_external_metadata_store(name):
        return False
    # Map the metadata store to the `external_metadata_store: str | bool` argument.
    # Metadata stores where the name matches the address (or specific metadata) return `True`,
    # others, like FoundationDB, return a string that is more instructive.
    match METADATA_STORE:
        case "cockroach":
            return True
        case "foundationdb":
            return fdb_coordinator_addresses()
        case "postgres-metadata":
            return True
        case _:
            raise ValueError(f"Unknown METADATA_STORE: {METADATA_STORE}")


EXTERNAL_METADATA_STORE_ADDRESS: str | bool = external_metadata_store()
""" The external_metadata_store value to use for Materialized/Testdrive. """


def metadata_store_services(*args, **kwargs) -> list[Service]:
    """
    Returns a list containing the metadata store service instance if an external metadata store is used,
    otherwise returns an empty list. Useful to construct a `SERVICES` list in a composition.
    :param args: Passed through to the metadata store service constructor.
    :param kwargs: Passed through to the metadata store service constructor.

    For FoundationDB, returns multiple services (server nodes + cluster service).
    """
    if not METADATA_STORE_SERVICE:
        return []
    if METADATA_STORE == "foundationdb":
        return foundationdb_services(num_nodes=FDB_NUM_NODES, **kwargs)
    return [METADATA_STORE_SERVICE(*args, **kwargs)]


# Legacy switch to select between CockroachDB and Postgres for metadata storage.
def _get_cockroach_or_postgres_metadata() -> type[Cockroach | PostgresMetadata]:
    from materialize.mzcompose.services.postgres import PostgresMetadata

    return Cockroach if os.getenv("BUILDKITE_TAG", "") != "" else PostgresMetadata


CockroachOrPostgresMetadata = _get_cockroach_or_postgres_metadata()
