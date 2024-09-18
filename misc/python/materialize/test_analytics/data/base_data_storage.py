# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import time
from collections.abc import Sequence
from typing import Any

from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnector,
)


class BaseDataStorage:

    def __init__(self, database_connector: DatabaseConnector):
        self.database_connector = database_connector

    def query_data(
        self,
        query: str,
        verbose: bool = True,
        statement_timeout: str | None = None,
    ) -> Sequence[Sequence[Any]]:
        statement_timeout = (
            statement_timeout
            or self.database_connector.config.default_statement_timeout
        )

        cursor = self.database_connector.create_cursor(
            allow_reusing_connection=True, statement_timeout=statement_timeout
        )

        if verbose:
            print(
                f"Executing query: {self.database_connector.to_short_printable_sql(query)}"
            )

        start_time = time.time()
        cursor.execute(query.encode())
        result = cursor.fetchall()
        end_time = time.time()

        if verbose:
            duration_in_sec = round(end_time - start_time, 2)
            print(f"Query returned {len(result)} rows in {duration_in_sec}s")

        return result
