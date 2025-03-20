# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from psycopg import Cursor, ProgrammingError

from materialize.scalability.operation.operation_data import OperationData


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

    def __str__(self) -> str:
        return self.__class__.__name__


class SqlOperationWithInput(Operation):
    def required_keys(self) -> set[str]:
        return {"cursor"}.union(self.required_input_keys())

    def required_input_keys(self) -> set[str]:
        raise NotImplementedError

    def _execute(self, data: OperationData) -> OperationData:
        try:
            cursor: Cursor = data.cursor()
            cursor.execute(self.sql_statement_based_on_input(data).encode("utf8"))
            cursor.fetchall()
        except ProgrammingError as e:
            assert "the last operation didn't produce a result" in str(e)

        return data

    def sql_statement_based_on_input(self, input: OperationData) -> str:
        raise NotImplementedError


class SqlOperationWithSeed(SqlOperationWithInput):
    def __init__(self, seed_key: str):
        self.seed_key = seed_key

    def required_input_keys(self) -> set[str]:
        return {self.seed_key}

    def sql_statement_based_on_input(self, input: OperationData) -> str:
        return self.sql_statement(str(input.get(self.seed_key)))

    def sql_statement(self, seed: str) -> str:
        raise NotImplementedError


class SqlOperationWithTwoSeeds(SqlOperationWithInput):
    def __init__(self, seed_key1: str, seed_key2: str):
        self.seed_key1 = seed_key1
        self.seed_key2 = seed_key2

    def required_input_keys(self) -> set[str]:
        return {self.seed_key1, self.seed_key2}

    def sql_statement_based_on_input(self, input: OperationData) -> str:
        return self.sql_statement(
            str(input.get(self.seed_key1)), str(input.get(self.seed_key2))
        )

    def sql_statement(self, seed1: str, seed2: str) -> str:
        raise NotImplementedError


class SimpleSqlOperation(SqlOperationWithInput):
    def required_input_keys(self) -> set[str]:
        return set()

    def sql_statement_based_on_input(self, _input: OperationData) -> str:
        return self.sql_statement()

    def sql_statement(self) -> str:
        raise NotImplementedError


class OperationChainWithDataExchange(Operation):
    def __init__(self, operations: list[Operation]):
        assert len(operations) > 0, "requires at least one operation"
        self.ops = operations

    def required_keys(self) -> set[str]:
        return self.operations()[0].required_keys()

    def operations(self) -> list[Operation]:
        return self.ops

    def _execute(self, data: OperationData) -> OperationData:
        for operation in self.operations():
            data = operation.execute(data)

        return data

    def __str__(self) -> str:
        return f"{self.__class__.__name__} with {', '.join(str(op) for op in self.operations())}"
