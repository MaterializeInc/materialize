# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""End-to-end tests for the MCP (Model Context Protocol) HTTP endpoints."""

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

    with c.test_case("agents_initialize"):
        r = post_mcp(
            c,
            "agents",
            jsonrpc(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1.0"},
                },
            ),
        )
        assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
        body = r.json()
        assert "result" in body, f"missing result: {body}"
        result = body["result"]
        assert result["protocolVersion"] == "2024-11-05"
        assert "serverInfo" in result
        assert result["serverInfo"]["name"] == "materialize-mcp-agents"

    with c.test_case("agents_tools_list"):
        r = post_mcp(c, "agents", jsonrpc("tools/list"))
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

    with c.test_case("agents_get_data_products"):
        r = post_mcp(
            c,
            "agents",
            jsonrpc("tools/call", {"name": "get_data_products", "arguments": {}}),
        )
        assert r.status_code == 200
        body = r.json()
        content = body["result"]["content"]
        assert len(content) > 0
        assert content[0]["type"] == "text"

    with c.test_case("agents_unknown_tool"):
        # Unknown tool name fails serde deserialization → Axum returns 422.
        r = post_mcp(
            c,
            "agents",
            jsonrpc("tools/call", {"name": "no_such_tool", "arguments": {}}),
        )
        assert r.status_code == 422, f"expected 422, got {r.status_code}: {r.text}"

    with c.test_case("agents_invalid_jsonrpc"):
        r = requests.post(
            mcp_url(c, "agents"),
            json={"jsonrpc": "1.0", "id": 1, "method": "tools/list"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "error" in body

    with c.test_case("observatory_initialize"):
        r = post_mcp(
            c,
            "observatory",
            jsonrpc(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1.0"},
                },
            ),
        )
        assert r.status_code == 200
        body = r.json()
        result = body["result"]
        assert result["protocolVersion"] == "2024-11-05"
        assert result["serverInfo"]["name"] == "materialize-mcp-observatory"

    with c.test_case("observatory_tools_list"):
        r = post_mcp(c, "observatory", jsonrpc("tools/list"))
        assert r.status_code == 200
        body = r.json()
        tools = body["result"]["tools"]
        tool_names = {t["name"] for t in tools}
        assert "query_system_catalog" in tool_names

    with c.test_case("observatory_query"):
        r = post_mcp(
            c,
            "observatory",
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

    with c.test_case("observatory_reject_non_select"):
        r = post_mcp(
            c,
            "observatory",
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

    with c.test_case("agents_disable_via_flag"):
        # Confirm it works first.
        r = post_mcp(c, "agents", jsonrpc("tools/list"))
        assert r.status_code == 200

        # Disable via system parameter.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agents = false",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "agents", jsonrpc("tools/list"))
        assert r.status_code == 503

        # Re-enable.
        c.sql(
            "ALTER SYSTEM SET enable_mcp_agents = true",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        r = post_mcp(c, "agents", jsonrpc("tools/list"))
        assert r.status_code == 200
