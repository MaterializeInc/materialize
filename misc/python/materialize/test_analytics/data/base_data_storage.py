# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from collections.abc import Sequence
from typing import Any

from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnector,
)


class BaseDataStorage:

    def __init__(self, database_connector: DatabaseConnector):
        self.database_connector = database_connector

    def query_data(self, query: str, verbose: bool = False) -> Sequence[Sequence[Any]]:
        cursor = self.database_connector.create_cursor(allow_reusing_connection=True)

        if verbose:
            print(f"Executing query: {query}")

        cursor.execute(query)
        result = cursor.fetchall()

        if verbose:
            print(f"Query returned {len(result)} rows")

        return result
