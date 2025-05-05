# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os

import pytest
import pytest_asyncio
from mcp import Tool
from mcp.types import ToolAnnotations
from psycopg_pool import AsyncConnectionPool

from mcp_materialize.mz_client import MzClient


@pytest_asyncio.fixture(scope="function")
async def materialize_pool():
    conn = os.getenv("MZ_DSN", "postgres://materialize@localhost:6875/materialize")
    async with AsyncConnectionPool(
        conninfo=conn, min_size=1, max_size=10, open=False
    ) as pool:
        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute("DROP SCHEMA IF EXISTS materialize.tools CASCADE;")
                await cur.execute("CREATE SCHEMA materialize.tools;")

        yield pool


@pytest.mark.asyncio
async def test_basic_tool(materialize_pool):
    async with MzClient(pool=materialize_pool) as client:
        tools = await client.list_tools()
        assert len(tools) == 0

    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """CREATE OR REPLACE VIEW tools.my_tool AS
                SELECT 1 AS id, 'hello' AS result;"""
            )
            await cur.execute("CREATE INDEX my_tool_id_idx ON tools.my_tool (id);")
            await cur.execute("COMMENT ON VIEW tools.my_tool IS 'Get result from id';")

            await cur.execute(
                """CREATE OR REPLACE VIEW tools.missing_comment AS
                SELECT 1 AS id, 'goodbye' AS result;"""
            )
            await cur.execute(
                "CREATE INDEX missing_comment_id_idx ON tools.missing_comment (id);"
            )

            await cur.execute(
                """CREATE OR REPLACE VIEW tools.missing_idx AS
                SELECT 1 AS id, 'not it' AS result;"""
            )
            await cur.execute(
                "COMMENT ON VIEW tools.missing_idx IS 'Get result from id';"
            )

    async with MzClient(pool=materialize_pool) as client:
        tools = await client.list_tools()
        assert len(tools) == 1
        assert tools[0] == Tool(
            name="materialize_tools_my_tool_id_idx",
            description="Get result from id",
            inputSchema={
                "type": "object",
                "required": ["id"],
                "properties": {"id": {"type": "number"}},
            },
            annotations=ToolAnnotations(readOnlyHint=True),
        )

        result = await client.call_tool("materialize_tools_my_tool_id_idx", {"id": 1})
        assert len(result) == 1
        assert json.loads(result[0].text) == {"result": "hello"}


@pytest.mark.asyncio
async def test_exists_tool(materialize_pool):
    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE OR REPLACE VIEW tools.my_tool AS SELECT 1 AS id;")
            await cur.execute("CREATE INDEX my_tool_id_idx ON tools.my_tool (id);")
            await cur.execute("COMMENT ON VIEW tools.my_tool IS 'Check if id exists';")

    async with MzClient(pool=materialize_pool) as client:
        result = await client.call_tool("materialize_tools_my_tool_id_idx", {"id": 1})
        assert len(result) == 1
        assert json.loads(result[0].text) == {"exists": True}

        result = await client.call_tool("materialize_tools_my_tool_id_idx", {"id": 2})
        assert len(result) == 1
        assert json.loads(result[0].text) == {"exists": False}


@pytest.mark.asyncio
async def test_type_handling_keys(materialize_pool):
    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """
            CREATE OR REPLACE VIEW tools.all_types AS
            SELECT
                1::smallint                                  AS smallint_col,
                2::integer                                   AS integer_col,
                3::bigint                                    AS bigint_col,
                2::uint4                                     AS uint2_col,
                4::uint4                                     AS uint4_col,
                8::uint4                                     AS uint8_col,
                4.5::real                                    AS real_col,
                6.7::double precision                        AS double_col,
                1.23::numeric                                AS numeric_col,
                true                                         AS boolean_col,
                'a'::char                                    AS char_col,
                'abc'::varchar                               AS varchar_col,
                'abc'::text                                  AS text_col,
                '2024-01-01'::date                           AS date_col,
                '12:34:56'::time                             AS time_col,
                '2024-01-01 12:34:56'::timestamp             AS timestamp_col,
                '2024-01-01 12:34:56+00'::timestamptz        AS timestamptz_col,
                decode('DEADBEEF', 'hex')::bytea             AS bytea_col,
                '{"a": 1, "b": [1, 2, 3]}'::jsonb            AS jsonb_col,
                '550e8400-e29b-41d4-a716-446655440000'::uuid AS uuid_col;
                """
            )
            await cur.execute("CREATE DEFAULT INDEX all_types_idx ON tools.all_types;")
            await cur.execute("COMMENT ON VIEW tools.all_types IS 'All types';")

    async with MzClient(pool=materialize_pool) as client:
        tools = await client.list_tools()
        assert len(tools) == 1

        assert tools[0].name == "materialize_tools_all_types_idx"
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
    async with materialize_pool.connection() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """
            CREATE OR REPLACE VIEW tools.all_types AS
            SELECT
                1                                            AS id,
                1::smallint                                  AS smallint_col,
                2::integer                                   AS integer_col,
                3::bigint                                    AS bigint_col,
                2::uint4                                     AS uint2_col,
                4::uint4                                     AS uint4_col,
                8::uint4                                     AS uint8_col,
                4.5::real                                    AS real_col,
                6.7::double precision                        AS double_col,
                1.23::numeric                                AS numeric_col,
                true                                         AS boolean_col,
                'a'::char                                    AS char_col,
                'abc'::varchar                               AS varchar_col,
                'abc'::text                                  AS text_col,
                '2024-01-01'::date                           AS date_col,
                '12:34:56'::time                             AS time_col,
                '2024-01-01 12:34:56'::timestamp             AS timestamp_col,
                '2024-01-01 12:34:56+00'::timestamptz        AS timestamptz_col,
                decode('DEADBEEF', 'hex')::bytea             AS bytea_col,
                '{"a": 1, "b": [1, 2, 3]}'::jsonb            AS jsonb_col,
                '550e8400-e29b-41d4-a716-446655440000'::uuid AS uuid_col;
                """
            )
            await cur.execute("CREATE INDEX all_types_idx ON tools.all_types (id);")
            await cur.execute("COMMENT ON VIEW tools.all_types IS 'All types';")

    async with MzClient(pool=materialize_pool) as client:
        results = await client.call_tool("materialize_tools_all_types_idx", {"id": 1})
        assert len(results) == 1
