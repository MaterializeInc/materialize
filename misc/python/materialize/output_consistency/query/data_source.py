# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass


@dataclass(unsafe_hash=True)
class DataSource:
    table_index: int | None = None

    def alias(self) -> str:
        if self.table_index is None:
            return "s"

        return f"s{self.table_index}"
