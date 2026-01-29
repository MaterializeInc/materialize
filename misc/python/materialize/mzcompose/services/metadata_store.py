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

from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.foundationdb import FoundationDB

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

# We'd like this to be `is_external_metadata_store(METADATA_STORE)`, but
# can't as `METADATA_STORE` is always set.
REQUIRES_EXTERNAL_METADATA_STORE: bool = is_external_metadata_store(
    os.getenv("EXTERNAL_METADATA_STORE", "postgres-internal")
)
""" Global flag indicating if an external metadata store is used. """


def metadata_store_service_list(
    *args, **kwargs
) -> list[Cockroach | FoundationDB | PostgresMetadata]:
    """
    Returns a list containing the metadata store service instance if an external metadata store is used,
    otherwise returns an empty list. Useful to construct a `SERVICES` list in a composition.
    :param args: Passed through to the metadata store service constructor.
    :param kwargs: Passed through to the metadata store service constructor.
    """
    return [METADATA_STORE_SERVICE(*args, **kwargs)] if METADATA_STORE_SERVICE else []


# Legacy switch to select between CockroachDB and Postgres for metadata storage.
def _get_cockroach_or_postgres_metadata() -> type[Cockroach | PostgresMetadata]:
    from materialize.mzcompose.services.postgres import PostgresMetadata

    return Cockroach if os.getenv("BUILDKITE_TAG", "") != "" else PostgresMetadata


CockroachOrPostgresMetadata = _get_cockroach_or_postgres_metadata()
