# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""End-to-end tests for the MCP (Model Context Protocol) HTTP endpoints."""

import json
import re
import time

import requests
from psycopg import Cursor

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(
        listeners_config_path=f"{MZ_ROOT}/src/materialized/ci/listener_configs/v26_32_0/no_auth.json",
    ),
]


# -- helpers ------------------------------------------------------------------


def mcp_url(c: Composition, endpoint: str) -> str:
    port = c.port("materialized", 6876)
    return f"http://localhost:{port}/api/mcp/{endpoint}"


def jsonrpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    msg: dict = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        msg["params"] = params
    return msg


def post_mcp(c: Composition, endpoint: str, body: dict) -> requests.Response:
    return requests.post(mcp_url(c, endpoint), json=body)


# -- tests --------------------------------------------------------------------


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_endpoints(c: Composition) -> None:
    c.up("materialized")

    # MCP feature flags default to true; no explicit enable needed.

    with c.test_case("agent_initialize"):
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "initialize",
                {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1.0"},
                },
            ),
        )
        assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
        body = r.json()
        assert "result" in body, f"missing result: {body}"
        result = body["result"]
        assert result["protocolVersion"] == "2025-11-25"
        assert "serverInfo" in result
        assert result["serverInfo"]["name"] == "materialize-mcp-agent"

    with c.test_case("agent_tools_list"):
        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 200
        body = r.json()
        tools = body["result"]["tools"]
        tool_names = {t["name"] for t in tools}
        assert (
            "get_data_products" in tool_names
        ), f"missing get_data_products: {tool_names}"
        assert (
            "get_data_product_details" in tool_names
        ), f"missing get_data_product_details: {tool_names}"
        assert (
            "query" in tool_names
        ), f"query should be present by default: {tool_names}"

    with c.test_case("agent_get_data_products"):
        r = post_mcp(
            c,
            "agent",
            jsonrpc("tools/call", {"name": "get_data_products", "arguments": {}}),
        )
        assert r.status_code == 200
        body = r.json()
        content = body["result"]["content"]
        assert len(content) > 0
        assert content[0]["type"] == "text"

    with c.test_case("agent_unknown_tool"):
        # Unknown tool name fails serde deserialization → Axum returns 422.
        r = post_mcp(
            c,
            "agent",
            jsonrpc("tools/call", {"name": "no_such_tool", "arguments": {}}),
        )
        assert r.status_code == 422, f"expected 422, got {r.status_code}: {r.text}"

    with c.test_case("agent_invalid_jsonrpc"):
        r = requests.post(
            mcp_url(c, "agent"),
            json={"jsonrpc": "1.0", "id": 1, "method": "tools/list"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body

    with c.test_case("developer_initialize"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "initialize",
                {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1.0"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        result = body["result"]
        assert result["protocolVersion"] == "2025-11-25"
        assert result["serverInfo"]["name"] == "materialize-mcp-developer"

    with c.test_case("developer_tools_list"):
        r = post_mcp(c, "developer", jsonrpc("tools/list"))
        assert r.status_code == 200
        body = r.json()
        tools = body["result"]["tools"]
        tool_names = {t["name"] for t in tools}
        assert "query_system_catalog" in tool_names

    with c.test_case("developer_query"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {"sql_query": "SELECT name FROM mz_clusters"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        content = body["result"]["content"]
        assert len(content) > 0
        assert content[0]["type"] == "text"
        assert "quickstart" in content[0]["text"]

    with c.test_case("developer_reject_non_select"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {"sql_query": "CREATE TABLE evil (id int)"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body

    # -- developer: pg_catalog and information_schema -------------------------

    with c.test_case("developer_pg_catalog"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {
                        "sql_query": "SELECT typname FROM pg_catalog.pg_type WHERE typname = 'bool'"
                    },
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "result" in body, f"expected result: {body}"
        assert "bool" in body["result"]["content"][0]["text"]

    with c.test_case("developer_information_schema"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {
                        "sql_query": "SELECT table_schema FROM information_schema.tables WHERE table_name = 'mz_databases' AND table_schema = 'mz_catalog' LIMIT 1"
                    },
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "result" in body, f"expected result: {body}"
        assert "mz_catalog" in body["result"]["content"][0]["text"]

    # -- developer: rejection cases ------------------------------------------

    with c.test_case("developer_reject_user_table"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {"sql_query": "SELECT * FROM user_table"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body
        assert "non-system tables" in body["error"]["message"]

    with c.test_case("developer_reject_multi_statement"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {"sql_query": "SELECT 1; SELECT * FROM mz_tables"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body
        assert "Only one query" in body["error"]["message"]

    with c.test_case("developer_reject_schema_squatting"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {"sql_query": "SELECT * FROM mz_catalogg.fake_table"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body
        assert "non-system tables" in body["error"]["message"]

    with c.test_case("developer_reject_mixed_tables"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {
                        "sql_query": "SELECT * FROM mz_tables t JOIN public.user_data u ON t.id = u.table_id"
                    },
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body
        assert "non-system tables" in body["error"]["message"]

    with c.test_case("developer_reject_empty_query"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {
                    "name": "query_system_catalog",
                    "arguments": {"sql_query": ""},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body
        assert "Empty query" in body["error"]["message"]

    # -- developer: wrong endpoint -------------------------------------------

    with c.test_case("developer_reject_agent_tool"):
        r = post_mcp(
            c,
            "developer",
            jsonrpc(
                "tools/call",
                {"name": "get_data_products", "arguments": {}},
            ),
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body
        assert "not available on developer" in body["error"]["message"]

    # -- developer: disable/enable via flag ----------------------------------

    with c.test_case("developer_disable_via_flag"):
        # Confirm it works first.
        r = post_mcp(c, "developer", jsonrpc("tools/list"))
        assert r.status_code == 200

        # Disable via system parameter.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_developer = false",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "developer", jsonrpc("tools/list"))
        assert r.status_code == 503

        # Re-enable.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_developer = true",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "developer", jsonrpc("tools/list"))
        assert r.status_code == 200

    # -- DEX-27: read_data_product auto-routes to the catalog cluster ---------
    #
    # Verifies that when an MV lives on a cluster other than the session's
    # default, `read_data_product` (called without a `cluster` argument)
    # transparently issues `SET CLUSTER` to the data product's home cluster
    # so the read actually hits the index/MV's dataflow. We confirm this by
    # checking `mz_internal.mz_recent_activity_log` for the SELECT we
    # issued and asserting it ran on the off-default cluster.

    with c.test_case("agent_read_data_product_auto_routes_cluster"):
        # Provision the off-default cluster + MV + grants for the HTTP user.
        c.sql(
            """
            DROP MATERIALIZED VIEW IF EXISTS public.dex27_routed_mv;
            DROP CLUSTER IF EXISTS dex27_other CASCADE;
            CREATE CLUSTER dex27_other REPLICAS (r1 (SIZE 'scale=1,workers=1'));
            CREATE MATERIALIZED VIEW public.dex27_routed_mv IN CLUSTER dex27_other
                AS SELECT 7::int AS id, 'routed'::text AS name;
            GRANT USAGE ON CLUSTER dex27_other TO anonymous_http_user;
            GRANT SELECT ON public.dex27_routed_mv TO anonymous_http_user;
            -- Make sure statement logging fires for our read so we can
            -- inspect mz_recent_activity_log without flakiness.
            ALTER SYSTEM SET statement_logging_default_sample_rate = 1.0;
            ALTER SYSTEM SET statement_logging_max_sample_rate = 1.0;
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        # First touch the agent endpoint so `anonymous_http_user` is
        # auto-provisioned, then call `read_data_product` with NO cluster
        # argument — the server should route to `dex27_other` based on the
        # catalog row, not the session-default `quickstart`.
        post_mcp(
            c,
            "agent",
            jsonrpc(
                "initialize",
                {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "dex27", "version": "0.1.0"},
                },
                req_id=2700,
            ),
        )
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "tools/call",
                {
                    "name": "read_data_product",
                    "arguments": {
                        "name": '"materialize"."public"."dex27_routed_mv"',
                        "limit": 5,
                    },
                },
                req_id=2701,
            ),
        )
        assert r.status_code == 200, f"unexpected status: {r.status_code} {r.text}"
        body = r.json()
        assert "error" not in body, f"read_data_product errored: {body}"
        rows = json.loads(body["result"]["content"][0]["text"])
        assert rows == [["7", "routed"]], f"unexpected rows: {rows}"

        # Now confirm via the activity log that the read ran on
        # `dex27_other`, not the session default. The log is emitted
        # asynchronously, so poll briefly.
        deadline = time.monotonic() + 30
        observed_cluster: str | None = None
        while time.monotonic() < deadline:
            rows = c.sql_query(
                """
                SELECT cluster_name
                FROM mz_internal.mz_recent_activity_log
                WHERE application_name = 'mz_mcp_agents'
                  AND sql ILIKE '%dex27_routed_mv%'
                  AND finished_status = 'success'
                ORDER BY began_at DESC
                LIMIT 1
                """,
                user="mz_system",
                port=6877,
            )
            if rows:
                observed_cluster = rows[0][0]
                break
            time.sleep(0.5)
        assert observed_cluster == "dex27_other", (
            "no-override read should auto-route to the data product's cluster "
            f"(dex27_other), but activity log shows cluster_name = {observed_cluster!r}"
        )

    # -- read_data_product fails loud when role lacks USAGE on home cluster ---
    #
    # `mz_mcp_data_products` filters by SELECT on the object but not by
    # cluster privileges, so a role may see a data product hosted on a
    # cluster it can't use. Auto-routing without a USAGE check would
    # emit `SET CLUSTER = <home>; SELECT ...` and the SELECT would fail
    # with `permission denied for CLUSTER`. Silently falling back to the
    # session default would hide the missing privilege as "slow reads
    # forever," so we instead surface a clear `ClusterPrivilegeMissing`
    # error and let the caller decide: grant USAGE, or pass an explicit
    # `cluster` override to read from a cluster they can use.

    with c.test_case("agent_read_data_product_fails_when_lacking_cluster_usage"):
        # Provision a "compute" cluster that hosts the MV's dataflow, and
        # a "serving" cluster the HTTP user has USAGE on. Grant SELECT on
        # the MV but withhold USAGE on the compute cluster.
        c.sql(
            """
            DROP MATERIALIZED VIEW IF EXISTS public.restricted_mv;
            DROP CLUSTER IF EXISTS restricted_compute CASCADE;
            DROP CLUSTER IF EXISTS restricted_serving CASCADE;
            CREATE CLUSTER restricted_compute REPLICAS (r1 (SIZE 'scale=1,workers=1'));
            CREATE CLUSTER restricted_serving REPLICAS (r1 (SIZE 'scale=1,workers=1'));
            CREATE MATERIALIZED VIEW public.restricted_mv IN CLUSTER restricted_compute
                AS SELECT 9::int AS id, 'override'::text AS name;
            GRANT SELECT ON public.restricted_mv TO anonymous_http_user;
            GRANT USAGE ON CLUSTER restricted_serving TO anonymous_http_user;
            REVOKE USAGE ON CLUSTER restricted_compute FROM anonymous_http_user;
            ALTER ROLE anonymous_http_user SET cluster = 'restricted_serving';
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        # Touch the agent endpoint so `anonymous_http_user` exists.
        post_mcp(
            c,
            "agent",
            jsonrpc(
                "initialize",
                {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "restricted", "version": "0.1.0"},
                },
                req_id=2800,
            ),
        )

        # No-override read: must fail with ClusterPrivilegeMissing and an
        # actionable message naming the missing cluster.
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "tools/call",
                {
                    "name": "read_data_product",
                    "arguments": {
                        "name": '"materialize"."public"."restricted_mv"',
                        "limit": 5,
                    },
                },
                req_id=2801,
            ),
        )
        assert r.status_code == 200, f"unexpected status: {r.status_code} {r.text}"
        body = r.json()
        err = body.get("error")
        assert err is not None, (
            "no-override read should fail loud when the role lacks USAGE on "
            f"the home cluster, but got: {body}"
        )
        assert (
            err["data"]["error_type"] == "ClusterPrivilegeMissing"
        ), f"unexpected error_type: {err}"
        assert (
            "restricted_compute" in err["message"]
        ), f"error message should name the missing cluster: {err['message']!r}"

        # With an explicit `cluster` override to a usable cluster, the
        # read succeeds. Confirms the documented recovery path.
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "tools/call",
                {
                    "name": "read_data_product",
                    "arguments": {
                        "name": '"materialize"."public"."restricted_mv"',
                        "cluster": "restricted_serving",
                        "limit": 5,
                    },
                },
                req_id=2802,
            ),
        )
        body = r.json()
        assert (
            "error" not in body
        ), f"override to a usable cluster should succeed, got: {body}"
        rows = json.loads(body["result"]["content"][0]["text"])
        assert rows == [["9", "override"]], f"unexpected rows: {rows}"

        # Sanity: explicit override to the un-usable home cluster still
        # fails (now at SQL execution time, not in the auto-route check).
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "tools/call",
                {
                    "name": "read_data_product",
                    "arguments": {
                        "name": '"materialize"."public"."restricted_mv"',
                        "cluster": "restricted_compute",
                        "limit": 5,
                    },
                },
                req_id=2803,
            ),
        )
        body = r.json()
        assert (
            "error" in body
        ), "explicit override to a cluster without USAGE should still fail loudly"

        # Tidy up the role default so it does not leak into later cases.
        c.sql(
            "ALTER ROLE anonymous_http_user RESET cluster",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

    # -- QAR-136: developer query pins reads to a named cluster replica -------
    #
    # On a cluster with more than one replica, introspection reads (including
    # EXPLAIN ANALYZE) must target a specific replica. The developer `query`
    # tool accepts an optional `cluster_replica` for this. Verify the three
    # observable behaviors: untargeted EXPLAIN ANALYZE fails and names the
    # remedy, a pinned one succeeds, and a nonexistent replica surfaces a
    # clean ExecutionError.

    with c.test_case("developer_query_cluster_replica_pinning"):
        c.sql(
            """
            DROP CLUSTER IF EXISTS qar136_two_replicas CASCADE;
            CREATE CLUSTER qar136_two_replicas REPLICAS (
                r1 (SIZE 'scale=1,workers=1'),
                r2 (SIZE 'scale=1,workers=1')
            );
            GRANT USAGE ON CLUSTER qar136_two_replicas TO anonymous_http_user;
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        def qar136_query(req_id: int, arguments: dict) -> dict:
            r = post_mcp(
                c,
                "developer",
                jsonrpc(
                    "tools/call",
                    {"name": "query", "arguments": arguments},
                    req_id=req_id,
                ),
            )
            assert r.status_code == 200, f"unexpected status: {r.status_code} {r.text}"
            return r.json()

        # Without a replica pin, the introspection read cannot choose among
        # the two replicas. The error should point at the remedy.
        body = qar136_query(
            2900,
            {
                "cluster": "qar136_two_replicas",
                "sql_query": "EXPLAIN ANALYZE CLUSTER MEMORY",
            },
        )
        err = body.get("error")
        assert err is not None, (
            "untargeted EXPLAIN ANALYZE on a 2-replica cluster should fail, "
            f"but got: {body}"
        )
        assert (
            "log source reads must target a replica" in err["message"].lower()
        ), f"error should mention replica targeting: {err['message']!r}"

        # Pinned to r1, the same read succeeds.
        body = qar136_query(
            2901,
            {
                "cluster": "qar136_two_replicas",
                "cluster_replica": "r1",
                "sql_query": "EXPLAIN ANALYZE CLUSTER MEMORY",
            },
        )
        assert "error" not in body, f"pinned EXPLAIN ANALYZE errored: {body}"
        content = body["result"]["content"]
        assert (
            content and content[0]["type"] == "text"
        ), f"unexpected content: {content}"

        # A nonexistent replica surfaces the SQL layer's error as a clean
        # ExecutionError rather than anything worse.
        body = qar136_query(
            2902,
            {
                "cluster": "qar136_two_replicas",
                "cluster_replica": "no_such_replica",
                "sql_query": "EXPLAIN ANALYZE CLUSTER MEMORY",
            },
        )
        err = body.get("error")
        assert err is not None, f"nonexistent replica should fail, but got: {body}"
        assert (
            err["data"]["error_type"] == "ExecutionError"
        ), f"unexpected error_type: {err}"

        # Replica pinning is not part of the agent surface. The agent `query`
        # dispatch arm drops `cluster_replica`, so supplying one there is
        # silently accepted-and-ignored rather than honored or rejected. If it
        # were honored, this nonexistent replica would fail with an
        # ExecutionError. Because it is dropped, the plain read succeeds. A plain
        # SELECT does not need replica targeting, so success here proves the pin
        # was never applied.
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "tools/call",
                {
                    "name": "query",
                    "arguments": {
                        "cluster": "qar136_two_replicas",
                        "cluster_replica": "no_such_replica",
                        "sql_query": "SELECT 1",
                    },
                },
                req_id=2903,
            ),
        )
        assert r.status_code == 200, f"unexpected status: {r.status_code} {r.text}"
        body = r.json()
        assert "error" not in body, (
            "agent endpoint should ignore cluster_replica, so the read should "
            f"succeed rather than fail on the bogus replica: {body}"
        )
        rows = json.loads(body["result"]["content"][0]["text"])
        assert rows == [["1"]], f"unexpected rows: {rows}"

        c.sql(
            "DROP CLUSTER IF EXISTS qar136_two_replicas CASCADE",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

    # -- agent: query tool disable/enable via flag --------------------------------

    with c.test_case("agent_query_tool_disable_via_flag"):
        # Query tool is enabled by default; confirm it appears in tools/list.
        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 200
        tool_names = {t["name"] for t in r.json()["result"]["tools"]}
        assert (
            "query" in tool_names
        ), f"query should be enabled by default: {tool_names}"

        # Disable via system parameter.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agent_query_tool = false",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 200
        tool_names = {t["name"] for t in r.json()["result"]["tools"]}
        assert (
            "query" not in tool_names
        ), f"query should be hidden after disabling: {tool_names}"

        # Re-enable.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agent_query_tool = true",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 200
        tool_names = {t["name"] for t in r.json()["result"]["tools"]}
        assert "query" in tool_names, f"query should be re-enabled: {tool_names}"

    # -- OAuth Protected Resource Metadata (RFC 9728) -------------------------
    #
    # End-to-end coverage of the discovery endpoint that lets MCP-aware
    # clients (Claude Desktop, ChatGPT remote MCP) negotiate OAuth.
    # Three scenarios:
    #
    #   1. On the no-auth listener the endpoint MUST 404 — there is no
    #      OAuth flow to advertise when the listener doesn't validate
    #      tokens. This is the security canary: if the discovery endpoint
    #      ever starts publishing a document on a no-auth listener,
    #      something is wrong.
    #   2. With `oidc_issuer` unset the endpoint MUST 404 even when the
    #      listener does validate tokens, because RFC 9728 requires at
    #      least one authorization server.
    #   3. The 401 on `/api/mcp/*` does NOT emit a Bearer challenge on
    #      this no-auth listener — same reason: nothing to advertise.

    discovery_url = (
        f"http://localhost:{c.port('materialized', 6876)}"
        "/.well-known/oauth-protected-resource"
    )

    with c.test_case("oauth_metadata_404_on_no_auth_listener"):
        r = requests.get(discovery_url)
        assert r.status_code == 404, (
            "discovery endpoint must 404 on a None-authenticator listener; "
            f"got {r.status_code}: {r.text}"
        )

    with c.test_case("oauth_metadata_no_bearer_challenge_on_no_auth_listener"):
        # MCP 401 path: with no auth configured the listener auto-provisions
        # `anonymous_http_user` instead of returning 401, so we can't
        # observe the challenge headers directly here. The unit/integration
        # tests in src/environmentd/tests/server.rs cover the
        # authenticated-listener case. This case asserts only that the
        # MCP route still responds (so we know it is wired) and that the
        # discovery endpoint stays a 404.
        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert (
            r.status_code == 200
        ), f"MCP route should serve anon users on no-auth listener: {r.status_code}"

    # -- agent: disable/enable via flag ----------------------------------------

    with c.test_case("agent_disable_via_flag"):
        # Confirm it works first.
        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 200

        # Disable via system parameter.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agent = false",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 503

        # Re-enable.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agent = true",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "agent", jsonrpc("tools/list"))
        assert r.status_code == 200

    # -- hydration: end-to-end coverage for DEX-30 ----------------------------
    #
    # Two scenarios:
    #   1. MV on `quickstart` (1 replica) → hydrated=true, 1/1 replicas.
    #   2. MV on a cluster with zero replicas → hydrated=false, 0/0.
    #
    # The HTTP user is `anonymous_http_user` (auto-provisioned on the first
    # MCP request); we touch the endpoint eagerly with `initialize`, then
    # grant the necessary privileges as `mz_system`.

    # First touch the agent endpoint so `anonymous_http_user` exists as a role
    # and we can GRANT to it.
    post_mcp(
        c,
        "agent",
        jsonrpc(
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "setup", "version": "0.1.0"},
            },
            req_id=999,
        ),
    )

    c.sql(
        """
        GRANT USAGE ON CLUSTER quickstart TO anonymous_http_user;
        GRANT USAGE ON DATABASE materialize TO anonymous_http_user;
        GRANT USAGE, CREATE ON SCHEMA materialize.public TO anonymous_http_user;
        """,
        user="mz_system",
        port=6877,
        print_statement=False,
    )

    def hydration_for(object_name: str) -> dict:
        """Calls `get_data_product_details` and returns the parsed hydration
        object from the first row. Asserts the row has the documented 5-cell
        shape and that the hydration cell carries the three required keys."""
        r = post_mcp(
            c,
            "agent",
            jsonrpc(
                "tools/call",
                {
                    "name": "get_data_product_details",
                    "arguments": {"name": object_name},
                },
            ),
        )
        assert r.status_code == 200, f"unexpected status: {r.status_code} {r.text}"
        body = r.json()
        assert "error" not in body, f"unexpected error response: {body}"
        rows = json.loads(body["result"]["content"][0]["text"])
        assert rows, f"expected at least one details row, got: {rows}"
        row = rows[0]
        assert (
            len(row) == 5
        ), f"details row should have 5 cells (object_name, cluster, description, schema, hydration), got: {row}"
        hydration = row[4]
        assert isinstance(hydration, dict), f"hydration should be a dict: {hydration}"
        for key in ("hydrated", "replica_count", "hydrated_replica_count"):
            assert key in hydration, f"hydration missing `{key}`: {hydration}"
        return hydration

    with c.test_case("agent_get_data_product_details_hydrated"):
        # Grant SELECT to both `anonymous_http_user` (the MCP server's
        # session user on this no-auth listener) and `materialize` (the
        # default user for `c.sql_query`, used by the SQL-level test
        # below). `mz_mcp_data_product_details` filters by
        # `mz_show_my_object_privileges`, which is per-user.
        c.sql(
            """
            DROP MATERIALIZED VIEW IF EXISTS public.test_hydration_mv;
            CREATE MATERIALIZED VIEW public.test_hydration_mv IN CLUSTER quickstart
                AS SELECT 1::int AS id, 'widget'::text AS name;
            GRANT SELECT ON public.test_hydration_mv TO anonymous_http_user;
            GRANT SELECT ON public.test_hydration_mv TO materialize;
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        # The MV runs on `quickstart`, which has a single ready replica, so
        # hydration should converge almost immediately. Poll briefly to
        # avoid a race with hydration-status updates.
        object_name = '"materialize"."public"."test_hydration_mv"'
        deadline = time.monotonic() + 30
        last: dict = {}
        while time.monotonic() < deadline:
            last = hydration_for(object_name)
            if last["hydrated"]:
                break
            time.sleep(0.5)
        assert (
            last.get("hydrated") is True
        ), f"expected hydrated=true within 30s, last: {last}"
        assert (
            last["replica_count"] == last["hydrated_replica_count"]
        ), f"counts should match when hydrated: {last}"
        assert last["replica_count"] >= 1, f"quickstart has a replica: {last}"

    with c.test_case("agent_get_data_product_details_zero_replicas"):
        # A cluster with no replicas can't hydrate anything, so `hydrated`
        # must be false with 0/0 counts. This is the canary case for the
        # `replica_count > 0` guard in the view's `hydrated` expression.
        c.sql(
            """
            DROP MATERIALIZED VIEW IF EXISTS public.test_hydration_empty_mv;
            DROP CLUSTER IF EXISTS test_hydration_empty;
            CREATE CLUSTER test_hydration_empty REPLICAS ();
            CREATE MATERIALIZED VIEW public.test_hydration_empty_mv
                IN CLUSTER test_hydration_empty
                AS SELECT 1::int AS id;
            GRANT USAGE ON CLUSTER test_hydration_empty TO anonymous_http_user;
            GRANT SELECT ON public.test_hydration_empty_mv TO anonymous_http_user;
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        hydration = hydration_for(
            '"materialize"."public"."test_hydration_empty_mv"',
        )
        assert hydration == {
            "hydrated": False,
            "replica_count": 0,
            "hydrated_replica_count": 0,
        }, f"expected zero-replica hydration, got: {hydration}"

    with c.test_case("agent_mcp_data_product_details_view_sql"):
        # The hydration column should also be queryable directly via SQL,
        # not just through MCP. This locks in the catalog surface so users
        # and dashboards can build on it independently of the MCP server.
        rows = c.sql_query(
            """
            SELECT object_name, hydration
            FROM mz_internal.mz_mcp_data_product_details
            WHERE object_name = '"materialize"."public"."test_hydration_mv"'
            """,
        )
        assert rows, f"expected at least one row, got: {rows}"
        _, hydration = rows[0]
        # psycopg decodes jsonb to a Python dict.
        assert isinstance(hydration, dict), f"hydration should be dict: {hydration!r}"
        assert hydration.get("hydrated") is True, hydration
        assert hydration["replica_count"] == hydration["hydrated_replica_count"]
        assert set(hydration.keys()) == {
            "hydrated",
            "replica_count",
            "hydrated_replica_count",
        }, f"unexpected keys in hydration object: {hydration}"


def workflow_restrict_to_user_objects_startup_append_bypass(c: Composition) -> None:
    """Regression test for SQL-383: a `restrict_to_user_objects` read deferred
    on the session's startup `mz_sessions` builtin-table append resumed with
    empty `resolved_ids`, silently skipping the system-catalog guard. Same
    bug class as the SHOW fix in #36543.

    `EXPLAIN TIMESTAMP` is the reliable vector — it reaches `sequence_plan`
    and its raw plan depends on `mz_sessions`. The deferral fires only on the
    *first* `mz_sessions` reference in a session, so the same statement is
    correctly blocked once the wait future is cleared; we use that for a
    differential check.
    """
    c.up("materialized")

    # `restrict_to_user_objects` is enforced independently of
    # `enable_rbac_checks`, and the no-auth external SQL listener (port 6875)
    # lets a LOGIN role connect without a password, so each new connection is
    # a fresh restricted session.
    c.sql(
        """
        DROP ROLE IF EXISTS restricted_agent;
        CREATE ROLE restricted_agent LOGIN;
        ALTER ROLE restricted_agent SET restrict_to_user_objects = true;
        """,
        user="mz_system",
        port=6877,
        print_statement=False,
    )

    RESTRICTED = "is restricted"
    # mz_sessions is the only REQUIRED_BUILTIN_TABLE.
    BYPASS = "EXPLAIN TIMESTAMP FOR SELECT * FROM mz_internal.mz_sessions"

    def run(cur: Cursor, sql: str) -> str:
        """Run `sql`; return ``"ok:<nrows>"`` or the error message string."""
        try:
            cur.execute(sql.encode())
        except Exception as e:  # noqa: BLE001 — the message is the assertion
            return str(e)
        if cur.description is None:
            return "ok:0"
        return f"ok:{len(cur.fetchall())}"

    def fresh_cursor() -> Cursor:
        # Every check needs a fresh session whose startup append is still pending.
        return c.sql_cursor(user="restricted_agent", port=6875, reuse_connection=False)

    with c.test_case("explain_timestamp_first_statement_blocked"):
        cur = fresh_cursor()
        # First reference: deferred for the startup append, then resumed.
        first = run(cur, BYPASS)
        # Second reference: wait future already cleared, no deferral.
        second = run(cur, BYPASS)

        # Control: holds on both builds; proves the restriction works and that
        # the statement's resolved_ids carry mz_sessions.
        assert RESTRICTED in second, (
            "non-deferred EXPLAIN TIMESTAMP over mz_sessions should be blocked, "
            f"got: {second!r}"
        )

        # Regression: fails on the buggy build, passes once `DeferredPlan`
        # carries `resolved_ids` into the resumed `check_plan`.
        assert RESTRICTED in first, (
            f"deferred EXPLAIN TIMESTAMP bypassed restrict_to_user_objects, "
            f"got: {first!r}"
        )

    with c.test_case("non_required_builtin_blocked_as_first_statement"):
        # mz_roles is not a REQUIRED_BUILTIN_TABLE, so no deferral fires —
        # blocked even as the first statement.
        outcome = run(
            fresh_cursor(),
            "EXPLAIN TIMESTAMP FOR SELECT * FROM mz_catalog.mz_roles",
        )
        assert RESTRICTED in outcome, (
            f"first-statement EXPLAIN TIMESTAMP over mz_roles should be blocked, "
            f"got: {outcome!r}"
        )

    with c.test_case("frontend_peek_path_not_vulnerable"):
        # SELECT uses the frontend peek path, which runs the RBAC check before
        # awaiting the startup appends. Pins that SELECT is not a bypass vector.
        outcome = run(fresh_cursor(), "SELECT * FROM mz_internal.mz_sessions")
        assert RESTRICTED in outcome, (
            f"first-statement SELECT over mz_sessions should be blocked, "
            f"got: {outcome!r}"
        )

    with c.test_case("restricted_agent_can_still_run_safe_queries"):
        # Sanity: the restriction is targeted, not a blanket failure.
        rows = c.sql_query(
            "SELECT current_user",
            user="restricted_agent",
            port=6875,
            reuse_connection=False,
        )
        assert rows[0][0] == "restricted_agent", rows

    with c.test_case("unrestricted_role_first_statement_succeeds"):
        # Positive control on the resume path: an unrestricted role running
        # the same deferred statement as its first statement must succeed,
        # not error. If `DeferredPlan` now carries `resolved_ids` correctly
        # the resumed `rbac::check_plan` returns Ok (no `restrict_to_user_objects`
        # to enforce), and the EXPLAIN TIMESTAMP plan flows through. A
        # regression here would mean we broke the non-restricted deferral path.
        cur = c.sql_cursor(user="materialize", port=6875, reuse_connection=False)
        outcome = run(cur, BYPASS)
        assert outcome.startswith("ok:"), (
            f"unrestricted EXPLAIN TIMESTAMP over mz_sessions should succeed "
            f"as first statement, got: {outcome!r}"
        )

    with c.test_case("restricted_agent_user_table_first_statement_succeeds"):
        # Positive control: a restricted role's first statement against a
        # user-owned object that does NOT depend on mz_sessions must succeed.
        # Confirms the fix only blocks the system-catalog refs and leaves
        # legitimate first-statement reads alone.
        c.sql(
            """
            DROP TABLE IF EXISTS restricted_smoke;
            CREATE TABLE restricted_smoke (a int);
            INSERT INTO restricted_smoke VALUES (1), (2), (3);
            GRANT SELECT ON restricted_smoke TO restricted_agent;
            GRANT USAGE ON SCHEMA public TO restricted_agent;
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )
        rows = c.sql_query(
            "SELECT count(*) FROM restricted_smoke",
            user="restricted_agent",
            port=6875,
            reuse_connection=False,
        )
        assert rows[0][0] == 3, rows
        c.sql(
            "DROP TABLE restricted_smoke;",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

    # test_case swallows assertion failures, so the cleanup always runs.
    # DROP OWNED BY first revokes the schema/object grants the positive-control
    # cases hand to the role, otherwise DROP ROLE fails with
    # `DependentObjectsStillExist`.
    c.sql(
        """
        DROP OWNED BY restricted_agent;
        DROP ROLE restricted_agent;
        """,
        user="mz_system",
        port=6877,
        print_statement=False,
    )


def workflow_oauth_metadata_host_injection(c: Composition) -> None:
    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/test/mcp/listener_config_oidc.json",
        )
    ):
        c.up("materialized")
        base = f"http://localhost:{c.port('materialized', 6876)}"
        c.sql(
            "ALTER SYSTEM SET oidc_issuer = 'https://issuer.example.com'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        attacker_host = 'attacker.example.net" foo=bar'
        attacker_prefix = "https://attacker.example.net"

        with c.test_case("vuln_www_authenticate_host_injection"):
            r = requests.post(
                f"{base}/api/mcp/agent",
                json=jsonrpc("tools/list"),
                headers={"X-Forwarded-Host": attacker_host},
            )
            assert r.status_code == 401, f"{r.status_code}: {r.text}"
            challenges = r.headers.get("WWW-Authenticate", "")
            # The Bearer challenge has both `scope` and `resource_metadata`
            # parameters; their relative order is part of the wire format
            # but we don't pin it here — what matters for this regression
            # guard is that resource_metadata is not pointing at the
            # attacker-controlled host.
            m = re.search(r'resource_metadata="([^"]*)"', challenges)
            assert m, challenges
            assert not m.group(1).startswith(attacker_prefix), m.group(1)

        with c.test_case("vuln_metadata_resource_host_injection"):
            r = requests.get(
                f"{base}/.well-known/oauth-protected-resource",
                headers={"X-Forwarded-Host": attacker_host},
            )
            assert r.status_code == 200, f"{r.status_code}: {r.text}"
            resource = r.json().get("resource", "")
            assert not resource.startswith(attacker_prefix), resource

        with c.test_case("vuln_metadata_cache_control_missing"):
            r = requests.get(f"{base}/.well-known/oauth-protected-resource")
            cache_control = r.headers.get("Cache-Control", "")
            assert "no-store" in cache_control or "private" in cache_control, repr(
                cache_control
            )


def workflow_oauth_metadata_extras(c: Composition) -> None:
    """Smoke tests for the production-ready additions to the discovery
    endpoint: `scopes_supported`, scope in WWW-Authenticate, path-suffixed
    well-known aliases, and invalid-issuer rejection."""
    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/test/mcp/listener_config_oidc.json",
        )
    ):
        c.up("materialized")
        base = f"http://localhost:{c.port('materialized', 6876)}"
        c.sql(
            "ALTER SYSTEM SET oidc_issuer = 'https://issuer.example.com'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        with c.test_case("metadata_advertises_scope"):
            r = requests.get(f"{base}/.well-known/oauth-protected-resource")
            assert r.status_code == 200, f"{r.status_code}: {r.text}"
            scopes = r.json().get("scopes_supported", [])
            assert scopes == ["mcp.read"], scopes

        with c.test_case("path_suffixed_aliases_serve_same_doc"):
            base_doc = requests.get(
                f"{base}/.well-known/oauth-protected-resource"
            ).json()
            for suffix in ("api/mcp/agent", "api/mcp/developer"):
                url = f"{base}/.well-known/oauth-protected-resource/{suffix}"
                r = requests.get(url)
                assert r.status_code == 200, f"{url}: {r.status_code}: {r.text}"
                assert r.json() == base_doc, url

        with c.test_case("www_authenticate_emits_scope"):
            r = requests.post(
                f"{base}/api/mcp/agent",
                json=jsonrpc("tools/list"),
            )
            assert r.status_code == 401, f"{r.status_code}: {r.text}"
            challenges = r.headers.get("WWW-Authenticate", "")
            assert 'scope="mcp.read"' in challenges, challenges
            assert "resource_metadata=" in challenges, challenges

        with c.test_case("invalid_issuer_returns_503"):
            c.sql(
                "ALTER SYSTEM SET oidc_issuer = 'not a url'",
                user="mz_system",
                port=6877,
                print_statement=False,
            )
            r = requests.get(f"{base}/.well-known/oauth-protected-resource")
            assert r.status_code == 503, f"{r.status_code}: {r.text}"
            # Restore so subsequent test cases (if any are added) start clean.
            c.sql(
                "ALTER SYSTEM SET oidc_issuer = 'https://issuer.example.com'",
                user="mz_system",
                port=6877,
                print_statement=False,
            )


# Error emitted by the 1 MB parser guard (MAX_STATEMENT_BATCH_SIZE in
# src/sql-parser/src/parser.rs) before any lexing happens.
SIZE_LIMIT_ERROR = "statement batch size cannot exceed"


def workflow_parser_statement_size_limit_bypass(c: Composition) -> None:
    """Regression test for DEX-64: the 1 MB parser guard lives in
    `parse_with_limit` / `parse_item_name_with_limit`. Before the fix the MCP
    handlers called the unbounded `parse()` and `parse_item_name()` directly,
    so they lexed and parsed multi-megabyte input the guard should reject
    (HTTP bodies go up to 5 MiB). These cases pin that MCP enforces the same
    limit; the "statement batch size cannot exceed ..." message is the
    fingerprint that the guard fired.
    """
    c.up("materialized")

    # A >1 MB batch of valid statements. The guard rejects it before lexing;
    # the unbounded parser instead parses all of them, so a "Found N
    # statements" rejection would be the fingerprint that the guard was
    # skipped.
    batch = "SELECT 1;" * (1_200_000 // len("SELECT 1;"))

    def tool_error(endpoint: str, tool: str, arguments: dict) -> str:
        r = post_mcp(
            c, endpoint, jsonrpc("tools/call", {"name": tool, "arguments": arguments})
        )
        assert r.status_code == 200, f"{r.status_code}: {r.text}"
        err = r.json().get("error")
        assert err is not None, f"oversized input should be rejected: {r.text[:500]}"
        return err["message"]

    with c.test_case("sql_http_endpoint_enforces_size_limit"):
        # Control: the reference SQL path rejects it with the guard, on any build.
        port = c.port("materialized", 6876)
        r = requests.post(f"http://localhost:{port}/api/sql", json={"query": batch})
        assert SIZE_LIMIT_ERROR in r.text, r.text[:500]

    # `parse()` bypass, reached through `validate_readonly_query` in both tools.
    for endpoint, tool, args in [
        ("agent", "query", {"cluster": "quickstart", "sql_query": batch}),
        ("developer", "query_system_catalog", {"sql_query": batch}),
    ]:
        with c.test_case(f"{endpoint}_{tool}_enforces_size_limit"):
            msg = tool_error(endpoint, tool, args)
            assert SIZE_LIMIT_ERROR in msg, f"{tool} parsed the oversized batch: {msg}"

    # `parse_item_name()` bypass: an oversized, non-parseable data product name.
    with c.test_case("agent_read_data_product_name_enforces_size_limit"):
        msg = tool_error("agent", "read_data_product", {"name": "(" * 1_200_000})
        assert (
            SIZE_LIMIT_ERROR in msg
        ), f"parse_item_name processed oversized name: {msg[:200]}"


def workflow_auth_failure_modes(c: Composition) -> None:
    """Auth rejection at the HTTP layer, against an OIDC listener.

    The existing OIDC workflows cover the discovery documents and the
    unauthenticated challenge. These cover what happens when a client does
    present credentials but they are unusable: a malformed Authorization
    header, or a bearer token that cannot be validated against the
    configured issuer. All must be rejected, never accepted or 500'd.
    """
    with c.override(
        Materialized(
            listeners_config_path=f"{MZ_ROOT}/test/mcp/listener_config_oidc.json",
        )
    ):
        c.up("materialized")
        base = f"http://localhost:{c.port('materialized', 6876)}"
        # A real-looking issuer with no reachable JWKS: no bearer token can
        # be validated against it, which is exactly what we want to assert.
        c.sql(
            "ALTER SYSTEM SET oidc_issuer = 'https://issuer.example.com'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        agent = f"{base}/api/mcp/agent"

        # A structurally valid JWT (header.payload.signature) that cannot be
        # verified against the configured issuer.
        unverifiable_jwt = (
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
            ".eyJzdWIiOiJhdHRhY2tlciIsImlzcyI6Imh0dHBzOi8vZXZpbC5leGFtcGxlLmNvbSJ9"
            ".c2lnbmF0dXJl"
        )

        cases = {
            "malformed_authorization_header": "Garbage",
            "bearer_without_token": "Bearer ",
            "bearer_non_jwt": "Bearer not-a-jwt",
            "bearer_unverifiable_jwt": f"Bearer {unverifiable_jwt}",
        }
        for name, header in cases.items():
            with c.test_case(f"auth_rejected_{name}"):
                r = requests.post(
                    agent,
                    json=jsonrpc("tools/list"),
                    headers={"Authorization": header},
                )
                # Rejected as unauthorized, never accepted and never a 5xx.
                assert r.status_code == 401, f"{name}: {r.status_code}: {r.text}"

        with c.test_case("get_returns_405_on_oidc_listener"):
            # 405 is decided by routing before auth, so it holds even on an
            # authenticated listener with no credentials presented.
            r = requests.get(agent)
            assert r.status_code == 405, f"{r.status_code}: {r.text}"
