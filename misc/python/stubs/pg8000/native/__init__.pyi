# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

def identifier(ident: str) -> str: ...
def literal(value: str) -> str: ...

class Connection:
    parameter_statuses: dict[str, str]

    def __init__(
        self,
        host: str = ...,
        port: int = ...,
        user: str = ...,
        database: str | None = ...,
        password: str | None = ...,
        timeout: int | None = ...,
        application_name: str | None = ...,
        startup_params: dict[str, str] | None = ...,
    ):
        pass

    def run(self, sql: str) -> None: ...
