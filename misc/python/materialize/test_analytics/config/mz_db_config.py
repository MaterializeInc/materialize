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
    app_password: str

    database: str
    search_path: str

    port: int = 6875

    enabled: bool = True
