# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.zippy.framework import Capability
from materialize.zippy.mysql_capabilities import MySqlTableExists
from materialize.zippy.watermarks import Watermarks


class MySqlCdcTableExists(Capability):
    """A MySQL CDC table exists in Materialize."""

    def __init__(self, name: str, mysql_table: MySqlTableExists | None = None) -> None:
        self.name = name
        self.mysql_table = mysql_table

    def get_watermarks(self) -> Watermarks:
        assert self.mysql_table is not None
        return self.mysql_table.watermarks
