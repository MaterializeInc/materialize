# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Optional

from materialize.zippy.framework import Capability
from materialize.zippy.postgres_capabilities import PostgresTableExists
from materialize.zippy.watermarks import Watermarks


class PostgresCdcTableExists(Capability):
    """A Postgres CDC table exists in Materialize."""

    def __init__(
        self, name: str, postgres_table: Optional[PostgresTableExists] = None
    ) -> None:
        self.name = name
        self.postgres_table = postgres_table

    def get_watermarks(self) -> Watermarks:
        assert self.postgres_table is not None
        return self.postgres_table.watermarks
