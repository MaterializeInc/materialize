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
from importlib.resources import files
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

TOOL_QUERY = (files("mcp_materialize.sql") / "tools.sql").read_text()


class MzTool:
    def __init__(
        self,
        name,
        database,
        schema,
        object_name,
        cluster,
        title,
        description,
        input_schema,
        output_schema,
        output_columns,
    ):
        self.name = name
        self.database = database
        self.schema = schema
        self.object_name = object_name
        self.cluster = cluster
        self.title = title
        self.description = description
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.output_columns = output_columns

    def as_tool(self) -> Tool:
        return Tool(
            name=self.name,
            description=self.description,
            inputSchema=self.input_schema,
            outputSchema=self.output_schema,
            annotations=ToolAnnotations(title=self.title, readOnlyHint=True),
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
                        title=row["title"],
                        description=row["description"],
                        input_schema=row["input_schema"],
                        output_schema=row["output_schema"],
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
