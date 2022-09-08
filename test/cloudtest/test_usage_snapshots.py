# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def test_usage_snapshots(mz: MaterializeApplication) -> None:
    mz.environmentd.sql(f"CREATE CLUSTER sized1 REPLICAS (sized_replica1 (SIZE '2-1'))")

    time.sleep(1)

    mz.environmentd.sql(f"CREATE CLUSTER sized2 REPLICAS (sized_replica2 (SIZE '1-1'))")

    time.sleep(1)

    # TODO: confirm the `mz_audit_events` get created
    # TODO: list blobs in Minio
    # TODO: read blobs from Minio

    raise NotImplementedError()
