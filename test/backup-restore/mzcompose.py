# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic Backup & Restore test with a table (CockroachDB metadata store)
"""

from materialize.backup_restore import workflow_default  # noqa: F401
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    Mc(),
    Materialized(
        external_blob_store=True,
        external_metadata_store=True,
        sanity_restart=False,
        metadata_store="cockroach",
    ),
    Testdrive(no_reset=True, metadata_store="cockroach"),
    Persistcli(),
]
