# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from dataclasses import dataclass

import psycopg
import requests

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
]

# The canonical no-auth CI listener config enables the `endpoint_api` route
# (`/metrics/custom`) on the external HTTP listener (6876), like CI/orchestratord.
NO_AUTH_LISTENERS = f"{MZ_ROOT}/src/materialized/ci/listener_configs/no_auth.json"


@dataclass
class MetricTrigger:
    case: str  # test-case suffix + unique object-name seed
    view_body: str  # the view's non-value columns become the metric's labels
    metric_ident: str  # exposed as the Prometheus metric name, with an `mz_custom_` prefix
    help_text: str  # the `HELP '...'` literal
    why: str


# `CREATE METRIC` rejects each of these at plan time now, because the resulting
# Prometheus `Desc` would be invalid and `MetricsRegistry::register`'s
# `.expect("defining a gauge vec")` (src/ore/src/metrics.rs) would otherwise
# panic on the next scrape. All views expose `count`.
METRIC_TRIGGERS = [
    MetricTrigger(
        "invalid_metric_name",
        "SELECT 1::int8 AS count",
        '"http-requests"',
        "h",
        "a hyphen in the metric name",
    ),
    MetricTrigger(
        "invalid_label_name",
        "SELECT 1::int8 AS count, 'ok'::text AS \"2xx\"",
        "responses",
        "h",
        "a label name starting with a digit (2xx)",
    ),
    MetricTrigger(
        "empty_help",
        "SELECT 1::int8 AS count",
        "leads",
        "",
        "empty HELP text",
    ),
]


def environmentd_alive(c: Composition, timeout: float = 20.0) -> bool:
    """Whether environmentd answers `SELECT 1`. On the buggy path it aborted and,
    with no restart policy, stays down — so this exhausts `timeout`."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            if c.sql_query("SELECT 1", reuse_connection=False)[0][0] == 1:
                return True
        except Exception:
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(1)


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

    # `handle_metrics_custom` registers a Prometheus gauge vector per metric via
    # `MetricsRegistry::register`, which panics — rather than 500s — on an invalid
    # `Desc` (see METRIC_TRIGGERS). environmentd's enhanced panic handler turns
    # that into `process::abort()`, so one scrape takes the whole process down,
    # and re-aborts on every later scrape since the bad metric lives in the
    # catalog. The `no_auth` listener exposes `endpoint_api` like CI/orchestratord.
    # Invariant (fix-agnostic): such a scrape must not abort environmentd — a
    # plan-time rejection or a graceful HTTP error are both fine.
    with c.override(Materialized(listeners_config_path=NO_AUTH_LISTENERS)):
        for trig in METRIC_TRIGGERS:
            with c.test_case(f"metrics_custom_{trig.case}"):
                # Boot, or recover from the previous case's abort (env boots fine
                # with a bad metric in the catalog; it only crashes on scrape).
                c.up("materialized")
                assert environmentd_alive(c), f"environmentd down before {trig.case}"

                # RBAC is irrelevant to the panic; disabling it lets the anonymous
                # HTTP user reach registration without grants. Re-applied (with
                # idempotent drops) so each case is independent of a crash+restart.
                c.sql(
                    "ALTER SYSTEM SET enable_prometheus_metrics_api = true;"
                    " ALTER SYSTEM SET enable_rbac_checks = false;",
                    user="mz_system",
                    port=6877,
                    print_statement=False,
                )
                view, api = f"v_{trig.case}", f"api_{trig.case}"
                c.sql(
                    f"DROP API IF EXISTS materialize.public.{api} CASCADE;"
                    f" DROP VIEW IF EXISTS materialize.public.{view} CASCADE;"
                    f" CREATE VIEW materialize.public.{view} AS {trig.view_body};"
                    f" CREATE API materialize.public.{api} FORMAT PROMETHEUS;",
                    print_statement=False,
                )
                try:
                    c.sql(
                        f"CREATE METRIC materialize.public.{trig.metric_ident}"
                        f" IN API materialize.public.{api} AS (TYPE 'gauge',"
                        f" HELP '{trig.help_text}',"
                        f" VALUES FROM materialize.public.{view},"
                        f" VALUE COLUMN 'count')",
                        print_statement=False,
                    )
                except psycopg.Error as e:
                    # Rejected at plan time — the dangerous object never exists.
                    print(f"[{trig.case}] rejected at CREATE METRIC ({trig.why}): {e}")
                    continue

                # On the buggy path the abort resets the connection mid-request.
                url = (
                    f"http://localhost:{c.port('materialized', 6876)}"
                    f"/metrics/custom/materialize/public/{api}"
                )
                try:
                    requests.get(url, timeout=30)
                except requests.exceptions.RequestException:
                    pass
                assert environmentd_alive(c), (
                    f"environmentd aborted after scraping a metric with {trig.why}"
                    " — handle_metrics_custom panicked in MetricsRegistry::register"
                )
