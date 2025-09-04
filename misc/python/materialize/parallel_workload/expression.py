# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize.data_ingest.data_type import (
    DATA_TYPES,
    Boolean,
    Bytea,
    DataType,
    Date,
    Double,
    Float,
    Int,
    IntArray,
    Interval,
    IntList,
    Jsonb,
    Long,
    MzTimestamp,
    Numeric,
    Numeric383,
    RecordSize,
    Text,
    TextTextMap,
    Time,
    Timestamp,
    UInt2,
    UInt4,
    UInt8,
)
from materialize.parallel_workload.database import (
    Column,
    KafkaColumn,
    MySqlColumn,
    PostgresColumn,
    SqlServerColumn,
)


class FuncOp:
    def __init__(self, text: str, params: list, materializable: bool = True):
        self.text = text
        self.params = params
        self.materializable = materializable


FUNC_OPS: dict[type[DataType], list[FuncOp]] = {dt: [] for dt in DATA_TYPES}

for dt in DATA_TYPES:
    FUNC_OPS[Boolean] += [
        FuncOp("{} IS NULL", [dt]),
        FuncOp("{} IS NOT NULL", [dt]),
    ]

    if dt != Bytea:
        FUNC_OPS[Text] += [FuncOp("cast({} as text)", [dt])]

    if dt not in (IntList, IntArray, TextTextMap, Bytea, Jsonb, Text):
        FUNC_OPS[Text] += [
            FuncOp("{} || {}", [dt, Text]),
            FuncOp("{} || {}", [Text, dt]),
        ]

    if dt not in (IntList, IntArray, TextTextMap, Bytea, Jsonb):
        FUNC_OPS[Boolean] += [
            FuncOp("{} > {}", [dt, dt]),
            FuncOp("{} < {}", [dt, dt]),
            FuncOp("{} >= {}", [dt, dt]),
            FuncOp("{} <= {}", [dt, dt]),
            FuncOp("{} = {}", [dt, dt]),
            FuncOp("{} != {}", [dt, dt]),
        ]

INT_TYPES = [Int, Long]
UINT_TYPES = [UInt2, UInt4, UInt8]
FLOAT_TYPES = [Float, Double, Numeric, Numeric383]

for dt in INT_TYPES + UINT_TYPES + FLOAT_TYPES:
    FUNC_OPS[dt] += [
        FuncOp("{} + {}", [dt, dt]),
        FuncOp("{} - {}", [dt, dt]),
        FuncOp("{} * {}", [dt, dt]),
        FuncOp("{} / {}", [dt, dt]),
        FuncOp("abs{}", [dt]),
    ]

FUNC_OPS[Long] += [FuncOp(f"cast({{}} as {Long.name()})", [Int])]

for i, dt in enumerate(UINT_TYPES):
    for dt2 in UINT_TYPES[i + 1 :]:
        FUNC_OPS[dt2] += [FuncOp(f"cast({{}} as {dt2.name()})", [dt])]

for dt in FLOAT_TYPES:
    for dt2 in FLOAT_TYPES:
        if dt2 == dt:
            continue
        FUNC_OPS[dt2] += [FuncOp(f"cast({{}} as {dt2.name()})", [dt])]

FUNC_OPS[Boolean] += [
    FuncOp("{} AND {}", [Boolean, Boolean]),
    FuncOp("{} OR {}", [Boolean, Boolean]),
    FuncOp("NOT {}", [Boolean]),
    FuncOp("{} @> {}", [IntList, IntList]),
    FuncOp("{} <@ {}", [IntList, IntList]),
    FuncOp("{} @> {}", [TextTextMap, TextTextMap]),
    FuncOp("{} <@ {}", [TextTextMap, TextTextMap]),
]

FUNC_OPS[Text] += [
    FuncOp("lower{}", [Text]),
    FuncOp("upper{}", [Text]),
    FuncOp("md5{}", [Text]),
    FuncOp("{} || {}", [Text, Text]),
    FuncOp("reverse{}", [Text]),
    FuncOp("{} -> {}", [TextTextMap, Text]),
    FuncOp("mz_environment_id()", [], materializable=False),
    FuncOp("mz_version()", [], materializable=False),
    FuncOp("current_database()", [], materializable=False),
    FuncOp("current_catalog()", [], materializable=False),
    FuncOp("current_user()", [], materializable=False),
    FuncOp("current_role()", [], materializable=False),
    FuncOp("session_user()", [], materializable=False),
    FuncOp("current_schema()", [], materializable=False),
]

FUNC_OPS[Int] += [
    FuncOp("bit_count{}", [Bytea]),
    FuncOp("bit_length{}", [Bytea]),
    FuncOp("bit_length{}", [Text]),
    FuncOp("ascii{}", [Text]),
    FuncOp("position({} in {})", [Text, Text]),
    FuncOp("length{}", [Text]),
    FuncOp("mz_version_num()", [], materializable=False),
    FuncOp("{} - {}", [Date, Date]),
]

FUNC_OPS[Timestamp] += [
    FuncOp("{} + {}", [Date, Interval]),
    FuncOp("{} - {}", [Date, Interval]),
    FuncOp("{} + {}", [Date, Time]),
    FuncOp("{} + {}", [Timestamp, Interval]),
    FuncOp("{} - {}", [Timestamp, Interval]),
    FuncOp("now()", [], materializable=False),
    FuncOp("current_timestamp()", [], materializable=False),
    FuncOp("cast({} as Timestamp)", [MzTimestamp]),
]

FUNC_OPS[MzTimestamp] += [
    FuncOp("mz_now()", [], materializable=False),
]

FUNC_OPS[Interval] += [
    FuncOp("{} - {}", [Timestamp, Timestamp]),
    FuncOp("{} - {}", [Time, Time]),
]

FUNC_OPS[Time] += [
    FuncOp("{} + {}", [Time, Interval]),
    FuncOp("{} - {}", [Time, Interval]),
]

FUNC_OPS[IntList] += [FuncOp("{} || {}", [IntList, IntList])]


def expression(
    data_type: type[DataType],
    columns: list[Column] | (
        list[MySqlColumn]
        | (list[PostgresColumn] | (list[SqlServerColumn] | list[KafkaColumn]))
    ),
    rng: random.Random,
    level: int = 0,
) -> str:
    if level < 120:
        if FUNC_OPS[data_type] and rng.random() < 0.3:
            fnop = rng.choice(FUNC_OPS[data_type])
            exprs = [
                f"({expression(dt, columns, rng, level + 1)})" for dt in fnop.params
            ]
            return fnop.text.format(*exprs)

        if rng.random() < 0.9:
            for col in random.sample(columns, len(columns)):
                if col.data_type == data_type:
                    return str(col)

    record_size = rng.choice(
        [RecordSize.TINY, RecordSize.SMALL, RecordSize.MEDIUM, RecordSize.LARGE]
    )
    return str(data_type.random_value(rng, record_size=record_size, in_query=True))
