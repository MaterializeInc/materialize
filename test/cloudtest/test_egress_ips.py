# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.mz_version import MzVersion


def test_egress_ips(mz: MaterializeApplication) -> None:
    egress_ips = mz.environmentd.sql_query("SELECT * FROM mz_catalog.mz_egress_ips")

    if MzVersion.parse_cargo() >= MzVersion.parse_mz("v0.118.0-dev"):
        assert egress_ips == (
            ["1.2.3.4", 32, "1.2.3.4/32"],
            ["2001:db8::", 60, "2001:db8::/60"],
            ["88.77.66.0", 28, "88.77.66.0/28"],
        )
    else:
        assert egress_ips == (["1.2.3.4"], ["88.77.66.55"])
