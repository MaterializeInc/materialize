# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

import requests

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

LISTENERS_CONFIG = os.path.join(os.path.dirname(__file__), "listeners.json")

SERVICES = [
    Materialized(listeners_config_path=LISTENERS_CONFIG),
]


def sql_post(url, query, user=None):
    headers = {}
    if user:
        headers["x-materialize-user"] = user
    return requests.post(url, headers=headers, json={"query": query})


def sql_user(url, query, user=None):
    r = sql_post(url, query, user)
    assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
    return r.json()["results"][0]["rows"][0][0]


def workflow_default(c: Composition) -> None:
    c.up("materialized")

    ext = f"http://localhost:{c.port('materialized', 6876)}/api/sql"
    internal = f"http://localhost:{c.port('materialized', 6878)}/api/sql"

    with c.test_case("anonymous_external_request"):
        assert sql_user(ext, "SELECT current_user;") == "anonymous_http_user"

    with c.test_case("internal_mz_system_allowed"):
        assert sql_user(internal, "SELECT current_user;", "mz_system") == "mz_system"

    with c.test_case("external_rejects_mz_system"):
        r = sql_post(ext, "SELECT current_user;", "mz_system")
        assert r.status_code in (401, 403), (
            f"expected 401/403, got {r.status_code}: {r.text}"
        )

    with c.test_case("external_rejects_mz_support"):
        r = sql_post(ext, "SELECT current_user;", "mz_support")
        assert r.status_code in (401, 403), (
            f"expected 401/403, got {r.status_code}: {r.text}"
        )

    with c.test_case("anonymous_cannot_alter_system"):
        r = sql_post(ext, "ALTER SYSTEM SET max_tables = 100;")
        assert r.status_code == 200
        assert "permission denied" in r.json()["results"][0]["error"]["message"].lower()
