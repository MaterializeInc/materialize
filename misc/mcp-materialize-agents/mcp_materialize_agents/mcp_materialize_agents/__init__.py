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
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.resources import files
from typing import Any
from uuid import UUID

from mcp.server import FastMCP
from mcp.server.fastmcp import Context
from mcp.types import ToolAnnotations
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

from .config import Config, load_config

logger = logging.getLogger("mcp_materialize_agents")


@asynccontextmanager
async def create_client(cfg: Config) -> AsyncIterator[AsyncConnectionPool]:
    logger.info(
        "Initializing connection pool with min_size=%s, max_size=%s",
        cfg.pool_min_size,
        cfg.pool_max_size,
    )

    async def configure(conn):
        await conn.set_autocommit(True)
        logger.debug("Configured new database connection")

    try:
        async with AsyncConnectionPool(
            conninfo=cfg.dsn,
            min_size=cfg.pool_min_size,
            max_size=cfg.pool_max_size,
            kwargs={"application_name": "mcp_materialize_agents"},
            configure=configure,
        ) as pool:
            try:
                logger.debug("Testing database connection...")
                async with pool.connection() as conn:
                    await conn.set_autocommit(True)
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT"
                            "   mz_environment_id() AS env,"
                            "   current_role AS role;"
                        )
                        meta = await cur.fetchone()
                        logger.info(
                            "Connected to Materialize environment %s as user %s",
                            meta["env"],
                            meta["role"],
                        )
                logger.debug("Connection pool initialized successfully")
                yield pool
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {str(e)}")
                raise
    except Exception as e:
        logger.error(f"Failed to create connection pool: {str(e)}")
        raise


TOOL_QUERY = (
    files("mcp_materialize_agents.mcp_materialize_agents") / "tools.sql"
).read_text()
INSTRUCTIONS = (
    files("mcp_materialize_agents.mcp_materialize_agents") / "system_prompt.md"
).read_text()


class DataProduct(BaseModel):
    name: str = Field(
        description=(
            "Fully qualified name of the data product with double quotes "
            '(e.g. \'"database"."schema"."view_name"\'). '
            "Use this exact name when querying."
        )
    )
    cluster: str = Field(
        description=(
            "Materialize compute cluster that hosts this data product. "
            "Required when executing queries - always use this exact "
            "cluster name."
        )
    )
    description: str = Field(
        description=(
            "Human-readable explanation of what business data this product "
            "contains and when to use it (e.g. 'Customer order history with "
            "shipping status for support queries')."
        )
    )


class FullDataProduct(BaseModel):
    name: str = Field(
        description=(
            "Fully qualified name with double quotes "
            '(e.g. \'"database"."schema"."view_name"\'). '
            "Use this exact name in queries."
        )
    )
    cluster: str = Field(
        description=(
            "Materialize compute cluster hosting this data. "
            "Always specify this cluster when querying this data product."
        )
    )
    description: str = Field(
        description=(
            "Detailed explanation of the business purpose and use cases "
            "for this data product."
        )
    )
    schema: dict[str, Any] = Field(
        description=(
            "Complete JSON schema showing all available fields, their data "
            "types, and descriptions. Use this to understand what data you "
            "can SELECT and what WHERE conditions you can use."
        )
    )


async def run():
    cfg = load_config()
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    async with create_client(cfg) as mz:
        server = FastMCP(
            "mcp_materialize", instructions=INSTRUCTIONS, host=cfg.host, port=cfg.port
        )

        @server.tool(
            description=(
                "Discover all available real-time data views (data products) "
                "that represent business entities like customers, orders, "
                "products, etc. Each data product provides fresh, queryable "
                "data with defined schemas. Use this first to see what data "
                "is available before querying specific information."
            ),
            annotations=ToolAnnotations(readOnlyHint=True),
        )
        async def get_data_products() -> list[DataProduct]:
            async with mz.connection() as conn:
                conn.set_autocommit(True)
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(TOOL_QUERY)
                    data_products = []
                    async for row in cur:
                        data_products.append(
                            DataProduct(
                                name=row["object_name"],
                                cluster=row["cluster"],
                                description=row["description"],
                            )
                        )
                    return data_products

        @server.tool(
            description=(
                "Get the complete schema and structure of a specific data "
                "product. This shows you exactly what fields are available, "
                "their types, and what data you can query. Use this after "
                "finding a data product from get_data_products() to "
                "understand how to query it."
            ),
            annotations=ToolAnnotations(readOnlyHint=True),
        )
        async def get_data_product_details(
            name: str = Field(
                description=(
                    "Exact name of the data product from " "get_data_products() list"
                )
            ),
        ) -> FullDataProduct:
            async with mz.connection() as conn:
                conn.set_autocommit(True)
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(TOOL_QUERY + " WHERE object_name = %s", [name])
                    async for row in cur:
                        return FullDataProduct(
                            name=row["object_name"],
                            description=row["description"],
                            cluster=row["cluster"],
                            schema=row["schema"],
                        )
                raise ValueError("Unknown data product name")

        @server.tool(
            description=(
                "Execute SQL queries against real-time data products to "
                "retrieve current business information. Use standard "
                "PostgreSQL syntax. You can JOIN multiple data products "
                "together, but ONLY if they are all hosted on the same "
                "cluster. Always specify the cluster parameter from the "
                "data product details. This provides fresh, up-to-date "
                "results from materialized views."
            ),
            annotations=ToolAnnotations(readOnlyHint=True),
        )
        async def query(
            ctx: Context,
            cluster: str = Field(
                description=(
                    "Exact cluster name from the data product details - "
                    "required for query execution"
                )
            ),
            sql_query: str = Field(
                description=(
                    "PostgreSQL-compatible SELECT statement to retrieve data. "
                    "Use the fully qualified data product name exactly as "
                    "provided (with double quotes). You can JOIN multiple data "
                    "products, but only those on the same cluster."
                )
            ),
        ):
            async with mz.connection() as conn:
                conn.set_autocommit(True)
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("START TRANSACTION READ ONLY;")
                    set_cluster = sql.SQL("SET CLUSTER TO {};").format(
                        sql.Identifier(cluster)
                    )
                    await cur.execute(set_cluster)
                    start = time.perf_counter()
                    await cur.execute(sql_query)
                    elapsed = (time.perf_counter() - start) * 1000
                    await ctx.debug(f"materialize query executed in {elapsed:.3f} ms")

                    rows = await cur.fetchall()
                    return serialize(rows)

        match cfg.transport:
            case "stdio":
                logger.info("Starting server in stdio mode...")
                await server.run_stdio_async()
            case "http":
                logger.info("Starting server in HTTP mode...")
                await server.run_streamable_http_async()
            case t:
                raise ValueError(f"Unknown transport: {t}")


def serialize(obj):
    """Serialize any Decimal/date/bytes/UUID into JSON-safe primitives."""
    # json.dumps will call json_serial for any non-standard type,
    # then json.loads turns it back into a Python dict/list of primitives.
    # Structured output types require the tool returns dict[str, Any]
    # but the json encoder used by the mcp library does not support all
    # standard postgres types
    return json.loads(json.dumps(obj, default=json_serial))


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
        return float(obj)
    elif isinstance(obj, UUID):
        return str(obj)
    else:
        raise TypeError(f"Type {type(obj)} not serializable. This is a bug.")


def main():
    """Synchronous wrapper for the async main function."""
    try:
        logger.info("Starting Materialize MCP Server...")
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down …")


if __name__ == "__main__":
    main()
