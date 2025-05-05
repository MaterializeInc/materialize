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
import asyncio
import base64
import decimal
import json
import logging
from collections.abc import Sequence
from textwrap import dedent
from typing import Any
from uuid import UUID

import aiorwlock
from mcp import Tool
from mcp.types import EmbeddedResource, ImageContent, TextContent, ToolAnnotations
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger("mz_mcp_server")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

TOOL_QUERY = base = dedent(
    """
        WITH tools AS (
            SELECT
                op.database || '_' || op.schema || '_' || i.name AS name,
                op.database,
                op.schema,
                op.name AS object_name,
                c.name AS cluster,
                cts.comment AS description,
                jsonb_build_object(
                    'type', 'object',
                    'required', jsonb_agg(distinct ccol.name) FILTER (WHERE ccol.position = ic.on_position),
                    'properties', jsonb_strip_nulls(jsonb_object_agg(
                        ccol.name,
                        CASE
                            WHEN ccol.type IN (
                                'uint2', 'uint4','uint8', 'int', 'integer', 'smallint',
                                'double', 'double precision', 'bigint', 'float',
                                'numeric', 'real'
                            ) THEN jsonb_build_object(
                                'type', 'number',
                                'description', cts_col.comment
                            )
                            WHEN ccol.type = 'boolean' THEN jsonb_build_object(
                                'type', 'boolean',
                                'description', cts_col.comment
                            )
                            WHEN ccol.type = 'bytea' THEN jsonb_build_object(
                                'type', 'string',
                                'description', cts_col.comment,
                                'contentEncoding', 'base64',
                                'contentMediaType', 'application/octet-stream'
                            )
                            WHEN ccol.type = 'date' THEN jsonb_build_object(
                                'type', 'string',
                                'format', 'date',
                                'description', cts_col.comment
                            )
                            WHEN ccol.type = 'time' THEN jsonb_build_object(
                                'type', 'string',
                                'format', 'time',
                                'description', cts_col.comment
                            )
                            WHEN ccol.type ilike 'timestamp%%' THEN jsonb_build_object(
                                'type', 'string',
                                'format', 'date-time',
                                'description', cts_col.comment
                            )
                            WHEN ccol.type = 'jsonb' THEN jsonb_build_object(
                                'type', 'object',
                                'description', cts_col.comment
                            )
                            WHEN ccol.type = 'uuid' THEN jsonb_build_object(
                                'type', 'string',
                                'format', 'uuid',
                                'description', cts_col.comment
                            )
                            ELSE jsonb_build_object(
                                'type', 'string',
                                'description', cts_col.comment
                            )
                        END
                    ) FILTER (WHERE ccol.position = ic.on_position))
                ) AS input_schema,
                array_agg(distinct ccol.name) FILTER (WHERE ccol.position <> ic.on_position) AS output_columns
            FROM mz_internal.mz_show_my_object_privileges op
            JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
            JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
            JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
            JOIN mz_indexes i ON i.on_id = o.id
            JOIN mz_index_columns ic ON i.id = ic.index_id
            JOIN mz_columns ccol ON ccol.id = o.id
            JOIN mz_clusters c ON c.id = i.cluster_id
            JOIN mz_internal.mz_show_my_cluster_privileges cp ON cp.name = c.name
            JOIN mz_internal.mz_comments cts ON cts.id = o.id AND cts.object_sub_id IS NULL
            LEFT JOIN mz_internal.mz_comments cts_col ON cts_col.id = o.id AND cts_col.object_sub_id = ccol.position
            WHERE op.privilege_type = 'SELECT'
              AND cp.privilege_type = 'USAGE'
            GROUP BY 1,2,3,4,5,6
        )
        SELECT * FROM tools
        """  # noqa: E501
)


class MzTool:
    def __init__(
        self,
        name,
        database,
        schema,
        object_name,
        cluster,
        description,
        input_schema,
        output_columns,
    ):
        self.name = name
        self.database = database
        self.schema = schema
        self.object_name = object_name
        self.cluster = cluster
        self.description = description
        self.input_schema = input_schema
        self.output_columns = output_columns

    def as_tool(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema=self.input_schema,
            annotations=ToolAnnotations(readOnlyHint=True),
        )


class MissingTool(Exception):
    def __init__(self, message):
        super().__init__(message)


class MzClient:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool
        self.tools: dict[str, MzTool] = {}
        self._lock = aiorwlock.RWLock()
        self._bg_task: asyncio.Task | None = None

    async def __aenter__(self) -> "MzClient":
        await self._load_tools()
        self._bg_task = asyncio.create_task(self._subscribe())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._bg_task:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass

    async def _subscribe(self) -> None:
        """
        Watches materialize for new tools.
        We cannot subscribe to the `TOOL` query directly because it relies on
        non-materializable functions. Instead, we watch indexes on objects that
        have comments as a proxy and then execute the full query.
        """
        try:
            async with self.pool.connection() as conn:
                await conn.set_autocommit(True)
                async with conn.cursor(row_factory=dict_row) as cur:
                    logger.info("Starting background tool subscription")
                    await cur.execute("BEGIN")
                    await cur.execute(
                        dedent(
                            """
                        DECLARE c CURSOR FOR
                        SUBSCRIBE (
                            SELECT count(*) AS eligible_tools
                            FROM mz_objects o
                            JOIN mz_indexes i ON o.id = i.on_id
                            JOIN mz_internal.mz_comments cts ON cts.id = o.id
                        ) WITH (PROGRESS)
                    """
                        )
                    )
                    while True:
                        await cur.execute("FETCH ALL c")
                        reload = False
                        async for row in cur:
                            if not row["mz_progressed"]:
                                reload = True
                            elif reload:
                                logger.info("Reloading catalog of available tools")
                                await self._load_tools()
                                reload = False

        except asyncio.CancelledError:
            logger.info("Stopping background tool subscription")
            return

    async def _load_tools(self) -> None:
        """
        Load the catalog of available tools into self.tools under lock.
        """
        new_tools: dict[str, MzTool] = {}
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(TOOL_QUERY)
                async for row in cur:
                    tool = MzTool(
                        name=row["name"],
                        database=row["database"],
                        schema=row["schema"],
                        object_name=row["object_name"],
                        cluster=row["cluster"],
                        description=row["description"],
                        input_schema=row["input_schema"],
                        output_columns=row["output_columns"],
                    )
                    new_tools[tool.name] = tool

        # swap in the fresh catalog
        async with self._lock.writer_lock:
            self.tools = new_tools

    async def list_tools(self) -> list[Tool]:
        """
        Return the catalog of available tools.
        """
        async with self._lock.reader_lock:
            return [tool.as_tool() for tool in self.tools.values()]

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        pool = self.pool

        async with self._lock.reader_lock:
            tool = self.tools.get(name)

        if not tool:
            raise MissingTool(f"Tool not found: {name}")

        async with pool.connection() as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute(
                    sql.SQL("SET cluster TO {};").format(sql.Identifier(tool.cluster))
                )

                await cur.execute(
                    sql.SQL("SELECT {} FROM {} WHERE {};").format(
                        (
                            sql.SQL("count(*) > 0 AS exists")
                            if not tool.output_columns
                            else sql.SQL(",").join(
                                sql.Identifier(col) for col in tool.output_columns
                            )
                        ),
                        sql.Identifier(tool.database, tool.schema, tool.object_name),
                        sql.SQL(" AND ").join(
                            [
                                sql.SQL("{} = {}").format(
                                    sql.Identifier(k), sql.Placeholder()
                                )
                                for k in arguments.keys()
                            ]
                        ),
                    ),
                    list(arguments.values()),
                )
                rows = await cur.fetchall()
                columns = [desc.name for desc in cur.description]

                result = [
                    {k: v for k, v in dict(zip(columns, row)).items()} for row in rows
                ]

                match len(result):
                    case 0:
                        return []
                    case 1:
                        text = json.dumps(result[0], default=json_serial)
                    case _:
                        text = json.dumps(result, default=json_serial)

                return [TextContent(text=text, type="text")]


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    from datetime import date, datetime, time, timedelta

    if isinstance(obj, datetime | date | time):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return obj.total_seconds()
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("ascii")
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, UUID):
        return str(obj)
    else:
        raise TypeError(f"Type {type(obj)} not serializable. This is a bug.")
