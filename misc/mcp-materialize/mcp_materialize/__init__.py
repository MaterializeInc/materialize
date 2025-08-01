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
"""
Materialize MCP Server

A  server that exposes Materialize indexes as "tools" over the Model Context
Protocol (MCP).  Each Materialize index that the connected role is allowed to
`SELECT` from (and whose cluster it can `USAGE`) is surfaced as a tool whose
inputs correspond to the indexed columns and whose output is the remaining
columns of the underlying view.

The server supports two transports:

* stdio – lines of JSON over stdin/stdout (handy for local CLIs)
* sse   – server‑sent events suitable for web browsers

---------------

1.  ``list_tools`` executes a catalog query to derive the list of exposable
    indexes; the result is translated into MCP ``Tool`` objects.
2.  ``call_tool`` validates the requested tool, switches the session to the
    appropriate cluster, executes a parameterised ``SELECT`` against the
    indexed view, and returns the first matching row (minus any columns whose
    values were supplied as inputs).
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.server import NotificationOptions, Server
from mcp.types import Tool
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from mcp_materialize.transports import http_transport, sse_transport, stdio_transport

from .config import Config, load_config
from .mz_client import MzClient

logger = logging.getLogger("mz_mcp_server")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@asynccontextmanager
async def create_client(cfg: Config) -> AsyncIterator[MzClient]:
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
            kwargs={"application_name": "mcp_materialize"},
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
                async with MzClient(pool=pool) as client:
                    yield client
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {str(e)}")
                raise
            finally:
                logger.info("Closing connection pool...")
                await pool.close()
    except Exception as e:
        logger.error(f"Failed to create connection pool: {str(e)}")
        raise


async def run():
    cfg = load_config()
    async with create_client(cfg) as mz:

        @asynccontextmanager
        async def lifespan(_server):
            yield mz

        server = Server("mcp_materialize", lifespan=lifespan)

        @server.list_tools()
        async def list_tools() -> list[Tool]:
            logger.debug("Listing available tools...")
            tools = await server.request_context.lifespan_context.list_tools()
            return tools

        @server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any]):
            logger.debug(f"Calling tool '{name}' with arguments: {arguments}")
            try:
                result = await server.request_context.lifespan_context.call_tool(
                    name, arguments
                )
                logger.debug(f"Tool '{name}' executed successfully")
                return result
            except Exception as e:
                logger.error(f"Error executing tool '{name}': {str(e)}")
                await server.request_context.session.send_tool_list_changed()
                raise

        options = server.create_initialization_options(
            notification_options=NotificationOptions(tools_changed=True)
        )
        match cfg.transport:
            case "stdio":
                logger.info("Starting server in stdio mode...")
                await stdio_transport(server, options)
            case "http":
                logger.info("Starting server in HTTP mode...")
                await http_transport(server, cfg)
            case "sse":
                logger.info(f"Starting SSE server on {cfg.host}:{cfg.port}...")
                await sse_transport(server, options, cfg)
            case t:
                raise ValueError(f"Unknown transport: {t}")


def main():
    """Synchronous wrapper for the async main function."""
    try:
        logger.info("Starting Materialize MCP Server...")
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down …")


if __name__ == "__main__":
    main()
