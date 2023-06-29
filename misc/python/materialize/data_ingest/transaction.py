# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.data_ingest.rowlist import RowList


class Transaction:
    row_lists: List[RowList]

    def __init__(self, row_lists: List[RowList]):
        self.row_lists = row_lists

    def __repr__(self) -> str:
        return (
            f"Transaction({','.join([str(row_list) for row_list in self.row_lists])})"
        )
