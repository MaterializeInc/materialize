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
    custom_db_object_name: str | None = None

    def get_db_object_name(
        self,
        base_name: str,
    ) -> str:
        if self.custom_db_object_name is not None:
            return self.custom_db_object_name

        table_index_suffix = (
            f"_{self.table_index}" if self.table_index is not None else ""
        )
        return f"{base_name}{table_index_suffix}"

    def alias(self) -> str:
        if self.table_index is None:
            return "s"

        return f"s{self.table_index}"
