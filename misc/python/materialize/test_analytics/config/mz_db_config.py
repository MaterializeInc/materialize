# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass


@dataclass
class MzDbConfig:
    hostname: str
    username: str
    app_password: str | None
    application_name: str | None

    database: str
    search_path: str
    cluster: str

    port: int = 6875

    default_statement_timeout: str = "60s"

    enabled: bool = True

    def __post_init__(self):
        if self.enabled:
            assert self.app_password is not None, "password required"
