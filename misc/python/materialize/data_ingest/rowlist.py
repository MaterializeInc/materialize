# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.data_ingest.row import Row


class RowList:
    rows: List[Row]
    # TODO: Implement generator_properties

    def __init__(self, rows: List[Row]):
        self.rows = rows

    def __repr__(self) -> str:
        return f"RowList({','.join([str(row) for row in self.rows])})"
