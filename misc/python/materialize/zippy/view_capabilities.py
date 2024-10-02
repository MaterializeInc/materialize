# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Union

from materialize.zippy.debezium_capabilities import DebeziumSourceExists
from materialize.zippy.mysql_cdc_capabilities import MySqlCdcTableExists
from materialize.zippy.pg_cdc_capabilities import PostgresCdcTableExists
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.table_capabilities import TableExists
from materialize.zippy.watermarked_object_capabilities import WatermarkedObjectExists
from materialize.zippy.watermarks import Watermarks

WatermarkedObjectExistss = list[
    Union[
        TableExists,
        SourceExists,
        "ViewExists",
        DebeziumSourceExists,
        PostgresCdcTableExists,
        MySqlCdcTableExists,
    ]
]


class ViewExists(WatermarkedObjectExists):
    """A view exists in Materialize."""

    @classmethod
    def format_str(cls) -> str:
        return "view_{}"

    def __init__(
        self,
        name: str,
        inputs: list[WatermarkedObjectExists],
        expensive_aggregates: bool | None = None,
        has_index: bool = False,
    ) -> None:
        self.name = name
        self.inputs = inputs
        self.expensive_aggregates = expensive_aggregates
        self.has_index = has_index

    def get_watermarks(self) -> Watermarks:
        """Calculate the intersection of the mins/maxs of the inputs. The result from the view should match the calculation."""

        return Watermarks(
            min_watermark=max([f.get_watermarks().min for f in self.inputs]),
            max_watermark=min([f.get_watermarks().max for f in self.inputs]),
        )
