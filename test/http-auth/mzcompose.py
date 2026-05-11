# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import base64
import json
import time

import jwt
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

OIDC_PORT = 8443
OIDC_USER = "alice@example.com"
OIDC_KID = "test-key-1"
OIDC_ISSUER = f"http://oidc-mock:{OIDC_PORT}"
OIDC_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwks():
    pub = OIDC_KEY.public_key().public_numbers()
    n = pub.n.to_bytes((pub.n.bit_length() + 7) // 8, "big")
    e = pub.e.to_bytes((pub.e.bit_length() + 7) // 8, "big")
    enc = base64.urlsafe_b64encode
    return json.dumps(
        {
            "keys": [
                {
                    "kty": "RSA",
                    "kid": OIDC_KID,
                    "use": "sig",
                    "alg": "RS256",
                    "n": enc(n).rstrip(b"=").decode(),
                    "e": enc(e).rstrip(b"=").decode(),
                }
            ]
        }
    )


def _make_jwt(groups):
    pem = OIDC_KEY.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    now = int(time.time())
    return jwt.encode(
        {
            "iss": OIDC_ISSUER,
            "sub": OIDC_USER,
            "groups": groups,
            "exp": now + 3600,
            "iat": now,
        },
        pem,
        algorithm="RS256",
        headers={"kid": OIDC_KID, "alg": "RS256"},
    )


def _http_sql(port, token, query):
    r = requests.post(
        f"http://localhost:{port}/api/sql",
        headers={"Authorization": f"Bearer {token}"},
        json={"query": query},
    )
    assert r.status_code == 200, f"HTTP SQL failed: {r.status_code} {r.text}"
    return r.json()["results"][0]


class OidcMock(Service):
    def __init__(self):
        super().__init__(
            name="oidc-mock",
            config={
                "image": "python:3.12-slim",
                "command": [
                    "python3",
                    "/mock/oidc_mock_server.py",
                    OIDC_ISSUER,
                    _jwks(),
                    str(OIDC_PORT),
                ],
                "ports": [OIDC_PORT],
                "volumes": [
                    f"{MZ_ROOT}/test/http-auth/oidc_mock_server.py:/mock/oidc_mock_server.py:ro"
                ],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "python3",
                        "-c",
                        f"import urllib.request; urllib.request.urlopen('http://localhost:{OIDC_PORT}/.well-known/openid-configuration')",
                    ],
                    "interval": "1s",
                    "start_period": "10s",
                },
            },
        )


SERVICES = [
    OidcMock(),
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

    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_oidc_group_sync_case_sensitive(c: Composition) -> None:
    """Regression test for SQL-276: OIDC group sync must use exact (case-sensitive)
    role name matching. A JWT group "Admin" must NOT grant access to a role named
    "admin" — only to a role named exactly "Admin"."""
    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/test/http-auth/listener_config_oidc.json",
            additional_system_parameter_defaults={
                "oidc_issuer": OIDC_ISSUER,
                "oidc_authentication_claim": "sub",
                "oidc_group_role_sync_enabled": "true",
                "oidc_group_claim": "groups",
            },
            depends_on=["oidc-mock"],
            sanity_restart=False,
        )
    ):
        c.up("oidc-mock")
        c.up("materialized")
        admin = c.sql_cursor(service="materialized", port=6877, user="mz_system")
        http_port = c.port("materialized", 6876)

        admin.execute("CREATE ROLE admin")
        admin.execute("CREATE TABLE secrets (data TEXT)")
        admin.execute("INSERT INTO secrets VALUES ('top-secret')")
        admin.execute("GRANT SELECT ON TABLE secrets TO admin")
        admin.execute('CREATE ROLE "Admin"')

        with c.test_case("case_collision_no_privilege_escalation"):
            # JWT group "Admin" must match only the "Admin" role (no privileges),
            # not the lowercase "admin" role (which has SELECT on secrets).
            token = _make_jwt(["Admin"])
            result = _http_sql(http_port, token, "SELECT data FROM secrets")
            assert (
                "error" in result
            ), f"attacker should NOT be able to read secrets via case collision, but got: {result}"

        with c.test_case("exact_match_grants_correct_role"):
            # JWT group "admin" (exact match) must grant the lowercase "admin" role.
            token = _make_jwt(["admin"])
            result = _http_sql(http_port, token, "SELECT data FROM secrets")
            assert "rows" in result and result["rows"] == [
                ["top-secret"]
            ], f"user with group 'admin' should be able to read secrets, but got: {result}"
