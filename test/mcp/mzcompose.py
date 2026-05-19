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
    # Verifies that `mz_internal.mz_mcp_data_product_details` exposes a
    # `hydration` JSON column that the agent endpoint surfaces through
    # `get_data_product_details`, so agents can distinguish "still warming
    # up" from "genuinely empty." Two scenarios:
    #
    #   1. MV on `quickstart` (1 replica) → hydrated=true, 1/1 replicas.
    #   2. MV on a cluster with zero replicas → hydrated=false, 0/0.
    #
    # The HTTP user that the MCP server runs as on this no-auth listener is
    # `anonymous_http_user`; it gets auto-provisioned on the first MCP
    # request, so we provision it eagerly with an `initialize` call and then
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
        c.sql(
            """
            DROP MATERIALIZED VIEW IF EXISTS public.test_hydration_mv;
            CREATE MATERIALIZED VIEW public.test_hydration_mv IN CLUSTER quickstart
                AS SELECT 1::int AS id, 'widget'::text AS name;
            GRANT SELECT ON public.test_hydration_mv TO anonymous_http_user;
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
