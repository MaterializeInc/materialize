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

        # Regression test for database-issues#11340. With `allowed_roles:
        # Normal`, header-based Basic auth correctly rejects `mz_system`, but
        # `/api/login` previously did not run the same role check — letting an
        # internal role obtain a session cookie and bypass the policy on
        # subsequent requests. Make sure `/api/login` enforces the listener's
        # role policy directly and never mints a session for a disallowed role.
        with c.test_case("session_login_rejects_disallowed_role"):
            s = requests.Session()
            r = s.post(
                f"{base}/api/login",
                json={"username": "mz_system", "password": "password"},
            )
            assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"
            assert (
                "mz_session" not in s.cookies
            ), f"login rejection must not set a session cookie: {s.cookies}"
