# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import requests

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
]


def workflow_default(c: Composition) -> None:
    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/test/http-auth/listener_config_normal_only.json"
        )
    ):
        c.up("materialized")
        base = f"http://localhost:{c.port('materialized', 6876)}"

        with c.test_case("session_login_rejects_disallowed_role"):
            s = requests.Session()
            r = s.post(
                f"{base}/api/login",
                json={"username": "mz_system", "password": "password"},
            )
            assert r.status_code == 401, (
                f"expected 401, got {r.status_code}: {r.text}"
            )

        with c.test_case("session_cookie_cannot_bypass_allowed_roles"):
            s = requests.Session()
            s.post(
                f"{base}/api/login",
                json={"username": "mz_system", "password": "password"},
            )
            r = s.post(
                f"{base}/api/sql",
                json={"query": "SELECT current_user"},
            )
            assert r.status_code == 401, (
                f"expected 401, got {r.status_code}: {r.text}"
            )
