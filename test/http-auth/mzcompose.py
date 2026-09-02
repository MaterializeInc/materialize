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

        # Regression test for database-issues#11340. `/api/login` does not run
        # the listener's role check itself, so a disallowed role (here
        # `mz_system` on an `allowed_roles: Normal` listener) can authenticate
        # and mint a session cookie. Authorization runs per request in the
        # `http_authz` middleware instead, so that cookie is rejected on every
        # protected route and the policy still can't be bypassed.
        with c.test_case("session_cookie_for_disallowed_role_is_rejected"):
            s = requests.Session()
            r = s.post(
                f"{base}/api/login",
                json={"username": "mz_system", "password": "password"},
            )
            assert (
                r.status_code == 200
            ), f"expected login to succeed, got {r.status_code}: {r.text}"
            assert (
                "mz_session" in s.cookies
            ), f"login should mint a session cookie: {s.cookies}"

            # Reusing that session cookie on a protected route is rejected by
            # the authorization middleware.
            r = s.post(f"{base}/api/sql", json={"query": "SELECT 1"})
            assert (
                r.status_code == 401
            ), f"expected 401 reusing disallowed-role session, got {r.status_code}: {r.text}"

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
        check_internal_endpoints_require_authorization(c, internal)

    check_livez_coordinator_coupling(c)


def check_livez_coordinator_coupling(c: Composition) -> None:
    """Regression test for SQL-343: previously having group_claim_for above
    the authenticator match in http_auth, so unauthenticated internal
    endpoints (/api/livez, /api/readyz, /metrics) began round-tripping
    the coordinator (`Command::GetSystemVars`) on every request for a value
    only the Frontegg arm uses.
    """
    c.up("materialized")
    base = f"http://localhost:{c.port('materialized', 6878)}"

    # Coordinator-side count of GetSystemVars commands, scraped from /metrics.
    def get_system_vars_count() -> int:
        for line in requests.get(f"{base}/metrics").text.splitlines():
            if (
                line.startswith("mz_slow_message_handling_count{")
                and "get_system_vars" in line
            ):
                return int(float(line.split()[-1]))
        return 0

    with c.test_case("livez_does_not_round_trip_coordinator"):
        before = get_system_vars_count()
        for _ in range(100):
            assert requests.get(f"{base}/api/livez").status_code == 200
        delta = get_system_vars_count() - before
        assert delta < 50, f"100 liveness probes caused {delta} coordinator round-trips"


def check_internal_endpoints_require_authorization(
    c: Composition, internal: str
) -> None:
    """The internal catalog/coordinator HTTP endpoints in
    `src/environmentd/src/http/catalog.rs` only authenticate the caller, never
    authorize them: they take an `AuthedClient` and call adapter APIs doc'd "No
    authorization is performed... limit to internal servers or superusers." So
    any normal user on a listener with `routes.internal=true` +
    `allowed_roles=NormalAndInternal` (the orchestrator's internal HTTP
    listener, and the *external* one under password/SASL/OIDC auth) reaches
    them. Here that caller is the non-superuser `anonymous_http_user` (the
    `anonymous_cannot_alter_system` case above proves it lacks superuser).

    These cases assert the secure behavior (401/403), so they fail RED until
    the handlers gain an internal-or-superuser check.
    """
    # `internal` is the .../api/sql URL on the 6878 listener; derive its root.
    # No auth header => the session is the normal `anonymous_http_user`.
    base = internal[: -len("/api/sql")]

    for path in (
        "catalog/dump",
        "catalog/check",
        "coordinator/check",
        "coordinator/dump",
    ):
        with c.test_case(f"internal_{path.replace('/', '_')}_requires_authorization"):
            r = requests.get(f"{base}/api/{path}")
            assert r.status_code in (401, 403), (
                f"normal user (anonymous_http_user) reached /api/{path}: "
                f"status={r.status_code}, {len(r.text)} bytes returned"
            )
    with c.test_case("internal_inject_audit_events_requires_authorization"):
        # A normal user must not be able to forge audit-log entries.
        forged = [
            {
                "event_type": "create",
                "object_type": "table",
                "details": {"IdNameV1": {"id": "u1", "name": "forged_by_normal_user"}},
                "user": "mz_system",  # caller picks the attributed user
            }
        ]
        r = requests.post(f"{base}/api/catalog/inject-audit-events", json=forged)
        # Proof the forgery took effect: count its rows in the audit log.
        audit = requests.post(
            internal,
            headers={"x-materialize-user": "mz_system"},
            json={
                "query": "SELECT count(*) FROM mz_audit_events "
                "WHERE details->>'name' = 'forged_by_normal_user'"
            },
        )
        forged_rows = audit.json()["results"][0]["rows"][0][0]
        assert r.status_code in (401, 403), (
            "normal user (anonymous_http_user) forged an audit event via "
            f"/api/catalog/inject-audit-events: status={r.status_code}, "
            f"audit rows={forged_rows}"
        )
