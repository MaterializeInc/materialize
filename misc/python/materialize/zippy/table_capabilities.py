# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.zippy.watermarked_object_capabilities import WatermarkedObjectExists
from materialize.zippy.watermarks import Watermarks


class TableExists(WatermarkedObjectExists):
    """A Table exists in the Mz instance."""

    @classmethod
    def format_str(cls) -> str:
        return "table_{}"

    def __init__(self, name: str, has_index: bool, max_rows_per_action: int) -> None:
        self.name = name
        self.has_index = has_index
        self.max_rows_per_action = max_rows_per_action
        self.watermarks = Watermarks()

    def get_watermarks(self) -> Watermarks:
        return self.watermarks

    def get_name_for_query(self) -> str:
        return self.name
