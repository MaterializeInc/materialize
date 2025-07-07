import contextlib
from typing import AsyncIterator

from mcp.server import Server, InitializationOptions
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
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route
    from starlette.types import Receive, Scope, Send
    from mcp.server.sse import SseServerTransport
    import uvicorn

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
    from starlette.applications import Starlette
    from starlette.routing import Mount, Route
    from starlette.types import Receive, Scope, Send
    import uvicorn

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
