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

    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/test/http-auth/listener_config_unauth_normal.json"
        )
    ):
        c.up("materialized")
        ext = f"http://localhost:{c.port('materialized', 6876)}/api/sql"
        internal = f"http://localhost:{c.port('materialized', 6878)}/api/sql"

        def sql_post(url, query, user=None):
            headers = {}
            if user:
                headers["x-materialize-user"] = user
            return requests.post(url, headers=headers, json={"query": query})

        def sql_user(url, query, user=None):
            r = sql_post(url, query, user)
            assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
            return r.json()["results"][0]["rows"][0][0]

        with c.test_case("anonymous_external_request"):
            assert sql_user(ext, "SELECT current_user;") == "anonymous_http_user"

        with c.test_case("internal_mz_system_allowed"):
            assert (
                sql_user(internal, "SELECT current_user;", "mz_system") == "mz_system"
            )

        # Regression test for database-issues#11346. With
        # `authenticator_kind=None` and `allowed_roles=Normal`, the
        # `x-materialize-user` header previously injected an authenticated
        # user without consulting `allowed_roles`, so external callers could
        # assert `mz_system` or `mz_support` and bypass the policy.
        with c.test_case("external_rejects_mz_system"):
            r = sql_post(ext, "SELECT current_user;", "mz_system")
            assert r.status_code in (
                401,
                403,
            ), f"expected 401/403, got {r.status_code}: {r.text}"

        with c.test_case("external_rejects_mz_support"):
            r = sql_post(ext, "SELECT current_user;", "mz_support")
            assert r.status_code in (
                401,
                403,
            ), f"expected 401/403, got {r.status_code}: {r.text}"

        with c.test_case("anonymous_cannot_alter_system"):
            r = sql_post(ext, "ALTER SYSTEM SET max_tables = 100;")
            assert r.status_code == 200
            assert (
                "permission denied"
                in r.json()["results"][0]["error"]["message"].lower()
            )
