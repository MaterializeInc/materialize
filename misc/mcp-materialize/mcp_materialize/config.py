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

import argparse
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    dsn: str
    transport: str
    host: str
    port: int
    pool_min_size: int
    pool_max_size: int
    log_level: str


def load_config() -> Config:
    parser = argparse.ArgumentParser(description="Run Materialize MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        help="Communication transport (default: stdio)",
    )

    parser.add_argument(
        "--mz-dsn",
        default=os.getenv(
            "MZ_DSN", "postgresql://materialize@localhost:6875/materialize"
        ),
        help="Materialize DSN (default: postgresql://materialize@localhost:6875/materialize)",
    )

    parser.add_argument(
        "--host",
        default=os.getenv("MCP_HOST", "0.0.0.0"),
        help="Server host (default: 0.0.0.0)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MCP_PORT", "3001")),
        help="Server port (default: 3001)",
    )

    parser.add_argument(
        "--pool-min-size",
        type=int,
        default=int(os.getenv("MCP_POOL_MIN_SIZE", "1")),
        help="Minimum connection pool size (default: 1)",
    )

    parser.add_argument(
        "--pool-max-size",
        type=int,
        default=int(os.getenv("MCP_POOL_MAX_SIZE", "10")),
        help="Maximum connection pool size (default: 10)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=os.getenv("MCP_LOG_LEVEL", "INFO"),
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()
    return Config(
        dsn=args.mz_dsn,
        transport=args.transport,
        host=args.host,
        port=args.port,
        pool_min_size=args.pool_min_size,
        pool_max_size=args.pool_max_size,
        log_level=args.log_level,
    )
