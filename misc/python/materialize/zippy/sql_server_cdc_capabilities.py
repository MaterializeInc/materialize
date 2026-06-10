# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.zippy.sql_server_capabilities import SqlServerTableExists
from materialize.zippy.watermarked_object_capabilities import WatermarkedObjectExists
from materialize.zippy.watermarks import Watermarks


class SqlServerCdcTableExists(WatermarkedObjectExists):
    """A SQL Server CDC table exists in Materialize."""

    def __init__(
        self, name: str, sql_server_table: SqlServerTableExists | None = None
    ) -> None:
        self.name = name
        self.sql_server_table = sql_server_table

    def get_watermarks(self) -> Watermarks:
        assert self.sql_server_table is not None
        return self.sql_server_table.watermarks

    def get_name_for_query(self) -> str:
        return self.name
