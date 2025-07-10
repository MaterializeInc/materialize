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
import contextlib
from collections.abc import AsyncIterator

from mcp.server import InitializationOptions, Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from mcp_materialize.config import Config


async def stdio_transport(server: Server, options: InitializationOptions):
    from mcp import stdio_server

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            options,
        )


async def sse_transport(server: Server, options: InitializationOptions, cfg: Config):
    import uvicorn
    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route
    from starlette.types import Receive, Scope, Send

    sse = SseServerTransport("/messages/")

    async def handle_sse(scope: Scope, receive: Receive, send: Send):
        async with sse.connect_sse(scope, receive, send) as (
            read_stream,
            write_stream,
        ):
            await server.run(read_stream, write_stream, options)

    starlette_app = Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse, methods=["GET"]),
            Mount("/messages/", app=sse.handle_post_message),
        ],
    )

    uv_server = uvicorn.Server(
        uvicorn.Config(
            starlette_app,
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level.lower(),
        )
    )
    await uv_server.serve()


async def http_transport(server: Server, cfg: Config):
    import uvicorn
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.types import Receive, Scope, Send

    session_manager = StreamableHTTPSessionManager(
        app=server,
        stateless=True,
    )

    async def handle_http(scope: Scope, receive: Receive, send: Send):
        await session_manager.handle_request(scope, receive, send)

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with session_manager.run():
            yield

    starlette_app = Starlette(
        routes=[
            Mount("/mcp", app=handle_http),
        ],
        lifespan=lifespan,
    )

    uv_server = uvicorn.Server(
        uvicorn.Config(
            starlette_app,
            host=cfg.host,
            port=cfg.port,
            log_level=cfg.log_level.lower(),
        )
    )
    await uv_server.serve()
