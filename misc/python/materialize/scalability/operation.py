# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from psycopg import Cursor, ProgrammingError

from materialize.scalability.operation_data import OperationData


class Operation:
    def required_keys(self) -> set[str]:
        """
        Keys in the data dictionary that are required.
        """
        return set()

    def produced_keys(self) -> set[str]:
        """
        Keys in the data dictionary that will be added or updated.
        """
        return set()

    def execute(self, data: OperationData) -> OperationData:
        data.validate_requirements(self.required_keys(), self.__class__, "requires")
        data = self._execute(data)
        data.validate_requirements(self.produced_keys(), self.__class__, "produces")
        return data

    def _execute(self, data: OperationData) -> OperationData:
        raise NotImplementedError


class SqlOperation(Operation):
    def required_keys(self) -> set[str]:
        return {"cursor"}

    def _execute(self, data: OperationData) -> OperationData:
        try:
            cursor: Cursor = data.cursor()
            cursor.execute(self.sql_statement().encode("utf8"))
            cursor.fetchall()
        except ProgrammingError as e:
            assert "the last operation didn't produce a result" in str(e)

        return data

    def sql_statement(self) -> str:
        raise NotImplementedError
