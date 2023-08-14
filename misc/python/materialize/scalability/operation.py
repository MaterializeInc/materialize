# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from psycopg import Cursor, ProgrammingError


class Operation:
    def execute(self, cursor: Cursor) -> None:
        try:
            cursor.execute(self.sql_statement().encode('utf8'))
            cursor.fetchall()
        except ProgrammingError as e:
            assert "the last operation didn't produce a result" in str(e)

    def sql_statement(self) -> str:
        raise NotImplementedError
