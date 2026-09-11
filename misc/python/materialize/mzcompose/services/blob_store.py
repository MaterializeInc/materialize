# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The blob stores persist can run against in a composition.

Each is a service of the same name that `Materialized` and `Testdrive` accept
as `external_blob_store`.
"""

from materialize.mzcompose.services.azurite import azure_blob_uri
from materialize.mzcompose.services.garage import garage_blob_uri
from materialize.mzcompose.services.minio import minio_blob_uri
from materialize.mzcompose.services.rustfs import rustfs_blob_uri

BLOB_STORES = ["minio", "azurite", "garage", "rustfs"]


def blob_store_uri(blob_store: str) -> str:
    """The persist blob URL for the named blob store service."""
    match blob_store:
        case "minio":
            return minio_blob_uri()
        case "azurite":
            return azure_blob_uri()
        case "garage":
            return garage_blob_uri()
        case "rustfs":
            return rustfs_blob_uri()
    raise ValueError(f"unknown blob store {blob_store!r}, expected one of {BLOB_STORES}")
