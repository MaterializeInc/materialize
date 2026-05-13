# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.services.minio`. See package __init__.py."""

from __future__ import annotations


def minio_blob_uri() -> str:
    # Only referenced from BackupRestoreAction, which the Antithesis driver
    # never schedules.
    raise RuntimeError(
        "minio_blob_uri() stub: BackupRestore scenario is not supported "
        "inside the Antithesis workload container."
    )
