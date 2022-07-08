# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Union

from materialize.zippy.framework import Capability
from materialize.zippy.source_capabilities import SourceExists
from materialize.zippy.table_capabilities import TableExists

WatermarkedObjects = List[Union[TableExists, SourceExists]]


class ViewExists(Capability):
    """A view exists in Materialize."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.froms: WatermarkedObjects = []
