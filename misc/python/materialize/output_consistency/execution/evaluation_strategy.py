# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.output_consistency.data_type.data_type import DataType


class EvaluationStrategy:
    def __init__(self, key: str, name: str):
        self.key = key
        self.db_object_name = key
        self.name = name

    def generate_source(self, data_types: list[DataType]) -> list[str]:
        raise RuntimeError("Not implemented")

    def __str__(self) -> str:
        return self.name


class DummyEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("*", "Dummy")

    def generate_source(self, data_types: list[DataType]) -> list[str]:
        return []


class DataFlowRenderingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("t_dfr", "Dataflow rendering")

    def generate_source(self, data_types: list[DataType]) -> list[str]:
        column_specs = create_column_specs(data_types, True)
        create_table_statement = (
            f"CREATE TABLE {self.db_object_name} ({', '.join(column_specs)});"
        )

        value_row = create_value_row(data_types)
        fill_table_statement = (
            f"INSERT INTO {self.db_object_name} VALUES ({value_row});"
        )

        return [create_table_statement, fill_table_statement]


class ConstantFoldingEvaluation(EvaluationStrategy):
    def __init__(self) -> None:
        super().__init__("v_ctf", "Constant folding")

    def generate_source(self, data_types: list[DataType]) -> list[str]:
        column_specs = create_column_specs(data_types, False)

        value_row = create_value_row(data_types)

        create_view_statement = f"CREATE VIEW {self.db_object_name} ({', '.join(column_specs)}) AS SELECT {value_row};"

        return [create_view_statement]


def create_column_specs(data_types: list[DataType], include_type: bool) -> list[str]:
    column_specs = []
    for data_type in data_types:
        for data_value in data_type.raw_values:
            column_specs.append(
                data_value.column_name
                + (f" {data_type.type_name}" if include_type else "")
            )

    return column_specs


def create_value_row(data_types: list[DataType]) -> str:
    row_values = []

    for data_type in data_types:
        for data_value in data_type.raw_values:
            row_values.append(data_value.to_sql_as_value())

    return f"{', '.join(row_values)}"
