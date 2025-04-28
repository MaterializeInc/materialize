# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
import subprocess
import time

import pytest
import pytest_asyncio
import requests
from docker.errors import DockerException
from mcp import Tool
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer

from materialize_mcp_server.mz_client import MzClient


def wait_for_readyz(host: str, port: int, timeout: int = 120, interval: int = 1):
    url = f"http://{host}:{port}/api/readyz"
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        print("Waiting for materialized to become ready...")
        time.sleep(interval)

    raise TimeoutError(f"Materialized did not become ready within {timeout} seconds.")


@pytest_asyncio.fixture(scope="function")
async def materialize_pool():
    try:
        context = subprocess.check_output(
            ["docker", "context", "show"], text=True
        ).strip()
        output = subprocess.check_output(
            ["docker", "context", "inspect", context], text=True
        )
        context_info = json.loads(output)[0]
        host = context_info["Endpoints"]["docker"]["Host"]

        os.environ["DOCKER_HOST"] = host
        print(f"Configured DOCKER_HOST = {host}")

    except Exception as e:
        pytest.skip(f"Failed to configure DOCKER_HOST: {e}")

    try:
        # Configure the container with explicit settings
        container = (
            DockerContainer("materialize/materialized:latest")
            .with_exposed_ports(6875, 6878)
            .with_env("MZ_LOG_FILTER", "info")
        )

        print("Starting Materialize container...")
        container.start()
        print("Materialize container started")

        # Get container info using the container object directly
        print(f"Container ID: {container._container.id}")
        print(f"Container status: {container._container.status}")

    except DockerException as e:
        pytest.skip(
            f"Failed to start Materialize container; skipping integration tests: {e}"
        )

    host = container.get_container_host_ip()
    sql_port = int(container.get_exposed_port(6875))
    http_port = int(container.get_exposed_port(6878))
    wait_for_readyz(host, http_port)
    print(f"Materialize running at {host}:{sql_port}")

    conn = f"postgres://materialize@{host}:{sql_port}/materialize"
    pool = AsyncConnectionPool(conninfo=conn, min_size=1, max_size=10, open=False)
    await pool.open()
    yield pool
    container.stop()


@pytest.mark.asyncio
async def test_basic_tool(materialize_pool):
    client = MzClient(pool=materialize_pool)
    tools = await client.list_tools()
    assert len(tools) == 0

    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                "CREATE VIEW my_tool AS SELECT 1 AS id, 'hello' AS result;"
            )
            await cur.execute("CREATE INDEX my_tool_id_idx ON my_tool (id);")
            await cur.execute("COMMENT ON VIEW my_tool IS 'Get result from id';")

            await cur.execute(
                "CREATE VIEW missing_comment AS SELECT 1 AS id, 'goodbye' AS result;"
            )
            await cur.execute(
                "CREATE INDEX missing_comment_id_idx ON missing_comment (id);"
            )

            await cur.execute(
                "CREATE VIEW missing_idx AS SELECT 1 AS id, 'not it' AS result;"
            )
            await cur.execute("COMMENT ON VIEW missing_idx IS 'Get result from id';")

    tools = await client.list_tools()
    assert len(tools) == 1
    assert tools[0] == Tool(
        name="my_tool_id_idx",
        description="Get result from id",
        inputSchema={
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "number"}},
        },
    )

    result = await client.call_tool("my_tool_id_idx", {"id": 1})
    assert len(result) == 1
    assert json.loads(result[0].text) == {"result": "hello"}


@pytest.mark.asyncio
async def test_exists_tool(materialize_pool):
    client = MzClient(pool=materialize_pool)
    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE VIEW my_tool AS SELECT 1 AS id;")
            await cur.execute("CREATE INDEX my_tool_id_idx ON my_tool (id);")
            await cur.execute("COMMENT ON VIEW my_tool IS 'Check if id exists';")

    result = await client.call_tool("my_tool_id_idx", {"id": 1})
    assert len(result) == 1
    assert json.loads(result[0].text) == {"exists": True}

    result = await client.call_tool("my_tool_id_idx", {"id": 2})
    assert len(result) == 1
    assert json.loads(result[0].text) == {"exists": False}


@pytest.mark.asyncio
async def test_type_handling_keys(materialize_pool):
    client = MzClient(pool=materialize_pool)
    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """
            CREATE VIEW all_types AS
            SELECT
                1::smallint                                         AS smallint_col,
                2::integer                                          AS integer_col,
                3::bigint                                           AS bigint_col,
                2::uint4                                            AS uint2_col,
                4::uint4                                            AS uint4_col,
                8::uint4                                            AS uint8_col,
                4.5::real                                           AS real_col,
                6.7::double precision                               AS double_col,
                1.23::numeric                                       AS numeric_col,
                true                                                AS boolean_col,
                'a'::char                                           AS char_col,
                'abc'::varchar                                      AS varchar_col,
                'abc'::text                                         AS text_col,
                '2024-01-01'::date                                  AS date_col,
                '12:34:56'::time                                    AS time_col,
                '2024-01-01 12:34:56'::timestamp                    AS timestamp_col,
                '2024-01-01 12:34:56+00'::timestamptz               AS timestamptz_col,
                decode('DEADBEEF', 'hex')::bytea                    AS bytea_col,
                '{"a": 1, "b": [1, 2, 3]}'::jsonb                   AS jsonb_col,
                '550e8400-e29b-41d4-a716-446655440000'::uuid        AS uuid_col;
                """
            )
            await cur.execute("CREATE DEFAULT INDEX all_types_idx ON all_types;")
            await cur.execute("COMMENT ON VIEW all_types IS 'All types';")

    tools = await client.list_tools()
    assert len(tools) == 1

    assert tools[0].name == "all_types_idx"
    assert tools[0].description == "All types"
    assert sorted(tools[0].inputSchema["required"]) == sorted(
        [
            "bigint_col",
            "boolean_col",
            "bytea_col",
            "char_col",
            "date_col",
            "double_col",
            "integer_col",
            "jsonb_col",
            "numeric_col",
            "real_col",
            "smallint_col",
            "text_col",
            "time_col",
            "timestamp_col",
            "timestamptz_col",
            "uint2_col",
            "uint4_col",
            "uint8_col",
            "uuid_col",
            "varchar_col",
        ]
    )

    assert tools[0].inputSchema["properties"] == {
        "bigint_col": {"type": "number"},
        "boolean_col": {"type": "boolean"},
        "bytea_col": {
            "type": "string",
            "contentEncoding": "base64",
            "contentMediaType": "application/octet-stream",
        },
        "char_col": {"type": "string"},
        "smallint_col": {"type": "number"},
        "double_col": {"type": "number"},
        "text_col": {"type": "string"},
        "integer_col": {"type": "number"},
        "uint2_col": {"type": "number"},
        "uint4_col": {"type": "number"},
        "uint8_col": {"type": "number"},
        "date_col": {"type": "string", "format": "date"},
        "time_col": {"type": "string", "format": "time"},
        "timestamp_col": {"type": "string", "format": "date-time"},
        "timestamptz_col": {"type": "string", "format": "date-time"},
        "jsonb_col": {"type": "object"},
        "numeric_col": {"type": "number"},
        "real_col": {"type": "number"},
        "varchar_col": {"type": "string"},
        "uuid_col": {"type": "string", "format": "uuid"},
    }


@pytest.mark.asyncio
async def test_type_handling_values(materialize_pool):
    client = MzClient(pool=materialize_pool)
    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """
            CREATE VIEW all_types AS
            SELECT
                1                                                   AS id,
                1::smallint                                         AS smallint_col,
                2::integer                                          AS integer_col,
                3::bigint                                           AS bigint_col,
                2::uint4                                            AS uint2_col,
                4::uint4                                            AS uint4_col,
                8::uint4                                            AS uint8_col,
                4.5::real                                           AS real_col,
                6.7::double precision                               AS double_col,
                1.23::numeric                                       AS numeric_col,
                true                                                AS boolean_col,
                'a'::char                                           AS char_col,
                'abc'::varchar                                      AS varchar_col,
                'abc'::text                                         AS text_col,
                '2024-01-01'::date                                  AS date_col,
                '12:34:56'::time                                    AS time_col,
                '2024-01-01 12:34:56'::timestamp                    AS timestamp_col,
                '2024-01-01 12:34:56+00'::timestamptz               AS timestamptz_col,
                decode('DEADBEEF', 'hex')::bytea                    AS bytea_col,
                '{"a": 1, "b": [1, 2, 3]}'::jsonb                   AS jsonb_col,
                '550e8400-e29b-41d4-a716-446655440000'::uuid        AS uuid_col;
                """
            )
            await cur.execute("CREATE INDEX all_types_idx ON all_types (id);")
            await cur.execute("COMMENT ON VIEW all_types IS 'All types';")

    results = await client.call_tool("all_types_idx", {"id": 1})
    assert len(results) == 1
