# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.zippy.framework import Capability
from materialize.zippy.watermarks import Watermarks


class TableExists(Capability):
    """A Table exists in the Mz instance."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.watermarks = Watermarks()

    def get_watermarks(self) -> Watermarks:
        return self.watermarks
