# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.cloudtest.app.materialize_application import MaterializeApplication


def test_egress_ips(mz: MaterializeApplication) -> None:
    egress_ips = mz.environmentd.sql_query(
        "SELECT egress_ip FROM mz_catalog.mz_egress_ips"
    )
    assert egress_ips == (["1.2.3.4"], ["88.77.66.55"])
