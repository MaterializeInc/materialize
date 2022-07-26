# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional, Union

from materialize.zippy.framework import Capability
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.table_capabilities import TableExists
from materialize.zippy.watermarks import Watermarks

WatermarkedObjects = List[Union[TableExists, SourceExists, "ViewExists"]]


class ViewExists(Capability):
    """A view exists in Materialize."""

    def __init__(self, name: str, froms: Optional[WatermarkedObjects] = None) -> None:
        self.name = name
        self.froms = froms if froms is not None else []

    def get_watermarks(self) -> Watermarks:
        """Calculate the intersection of the mins/maxs of the inputs. The result from the view should match the calculation."""

        assert self.froms is not None

        return Watermarks(
            min_watermark=max([f.get_watermarks().min for f in self.froms]),
            max_watermark=min([f.get_watermarks().max for f in self.froms]),
        )
