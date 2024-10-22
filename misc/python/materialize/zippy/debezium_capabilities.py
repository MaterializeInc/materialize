# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.zippy.framework import Capability
from materialize.zippy.postgres_capabilities import PostgresTableExists
from materialize.zippy.watermarked_object_capabilities import WatermarkedObjectExists
from materialize.zippy.watermarks import Watermarks


class DebeziumRunning(Capability):
    """Debezium is running in the environment."""

    pass


class DebeziumSourceExists(WatermarkedObjectExists):
    """A Debezium source exists in Materialize."""

    def __init__(
        self, name: str, postgres_table: PostgresTableExists | None = None
    ) -> None:
        self.name = name
        self.postgres_table = postgres_table

    def get_watermarks(self) -> Watermarks:
        assert self.postgres_table is not None
        return self.postgres_table.watermarks

    def get_name_for_query(self) -> str:
        return f"{self.name}_tbl"
