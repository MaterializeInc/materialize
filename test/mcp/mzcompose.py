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
import time

import requests

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(
        listeners_config_path=f"{MZ_ROOT}/src/materialized/ci/listener_configs/no_auth.json",
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
            "query" not in tool_names
        ), f"query should be hidden by default: {tool_names}"

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

    # -- read_data_product fallback when role lacks USAGE on the home cluster -
    #
    # The catalog view `mz_mcp_data_products` filters by SELECT on the
    # object but not by cluster privileges, so a role may legitimately see
    # a data product whose home cluster it cannot use. Before the USAGE
    # check, the no-override path emitted `SET CLUSTER = <home>; SELECT`
    # and the SELECT hard-failed on `permission denied for CLUSTER`. We
    # now skip the `SET CLUSTER` in that case and run the read on the
    # session default — slower (no index access) but correct.

    with c.test_case("agent_read_data_product_falls_back_without_cluster_usage"):
        # Provision a "compute" cluster that hosts the MV's dataflow, and
        # a "serving" cluster that the HTTP user has USAGE on. Grant
        # SELECT on the MV but withhold USAGE on the compute cluster.
        c.sql(
            """
            DROP MATERIALIZED VIEW IF EXISTS public.restricted_mv;
            DROP CLUSTER IF EXISTS restricted_compute CASCADE;
            DROP CLUSTER IF EXISTS restricted_serving CASCADE;
            CREATE CLUSTER restricted_compute REPLICAS (r1 (SIZE 'scale=1,workers=1'));
            CREATE CLUSTER restricted_serving REPLICAS (r1 (SIZE 'scale=1,workers=1'));
            CREATE MATERIALIZED VIEW public.restricted_mv IN CLUSTER restricted_compute
                AS SELECT 9::int AS id, 'fallback'::text AS name;
            -- Agent gets SELECT on the MV and USAGE on the serving cluster
            -- only; nothing on `restricted_compute`.
            GRANT SELECT ON public.restricted_mv TO anonymous_http_user;
            GRANT USAGE ON CLUSTER restricted_serving TO anonymous_http_user;
            REVOKE USAGE ON CLUSTER restricted_compute FROM anonymous_http_user;
            ALTER ROLE anonymous_http_user SET cluster = 'restricted_serving';
            """,
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        # Touch the agent endpoint so `anonymous_http_user` exists, then
        # call `read_data_product` with no override. The server must
        # detect that the role lacks USAGE on `restricted_compute` and
        # run the read on the session default (`restricted_serving`).
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
        assert "error" not in body, (
            "no-override read should fall back to the session default when the "
            f"role lacks USAGE on the home cluster, but got error: {body}"
        )
        rows = json.loads(body["result"]["content"][0]["text"])
        assert rows == [["9", "fallback"]], f"unexpected rows: {rows}"

        # Confirm via the activity log that the SELECT ran on
        # `restricted_serving` (the session default), not the home
        # cluster the role can't use.
        deadline = time.monotonic() + 30
        observed_cluster: str | None = None
        while time.monotonic() < deadline:
            log_rows = c.sql_query(
                """
                SELECT cluster_name
                FROM mz_internal.mz_recent_activity_log
                WHERE application_name = 'mz_mcp_agents'
                  AND sql ILIKE '%restricted_mv%'
                  AND finished_status = 'success'
                ORDER BY began_at DESC
                LIMIT 1
                """,
                user="mz_system",
                port=6877,
            )
            if log_rows:
                observed_cluster = log_rows[0][0]
                break
            time.sleep(0.5)
        assert observed_cluster == "restricted_serving", (
            "read should have fallen back to the session default, but activity "
            f"log shows cluster_name = {observed_cluster!r}"
        )

        # Sanity check: an explicit override still wins over auto-routing
        # and over the fallback. Passing the home cluster fails as before
        # because the role lacks USAGE — explicit caller intent is honored.
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
                req_id=2802,
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
