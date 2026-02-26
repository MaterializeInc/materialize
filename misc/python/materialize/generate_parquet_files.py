# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import datetime
import decimal
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from materialize import MZ_ROOT


def generate_parquet_files(dir = MZ_ROOT / "test" / "testdrive") -> None:
    record_col = pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
            pa.field("avg", pa.float64()),
        ]
    )
    # arrays for various types
    arrays = [
        # i8
        pa.array([-1, 2, 3], type=pa.int8()),
        # u8
        pa.array([10, 20, 30], type=pa.uint8()),
        # i16
        pa.array([-1000, 2000, 3000], type=pa.int16()),
        # u16
        pa.array([10000, 20000, 30000], type=pa.uint16()),
        # i32
        pa.array([-100000, 200000, 300000], type=pa.int32()),
        # u32
        pa.array([1000000, 2000000, 3000000], type=pa.uint32()),
        # i64
        pa.array([-1 * 10**9, 2 * 10**9, 3 * 10**9], type=pa.int64()),
        # u64
        pa.array([10**18, 2 * 10**18, 3 * 10**18], type=pa.uint64()),
        # f32
        pa.array([-1.0, 2.5, 3.7], type=pa.float32()),
        # f64
        pa.array([-1.0, 2.5, 3.7], type=pa.float64()),
        # boolean
        pa.array([True, False, True], type=pa.bool_()),
        # string
        pa.array(["apple", "banana", "cherry"], type=pa.string()),
        # uuid
        pa.array([b"raw1", b"raw2", b"raw3"], type=pa.binary()),
        # date32
        pa.array(
            [
                datetime.date(2025, 11, 1),
                datetime.date(2025, 11, 2),
                datetime.date(2025, 11, 3),
            ],
            type=pa.date32(),
        ),
        # timestamp (milliseconds)
        pa.array(
            [
                datetime.datetime(2025, 11, 1, 10, 0, 0),
                datetime.datetime(2025, 11, 1, 11, 30, 0),
                datetime.datetime(2025, 11, 1, 12, 0, 0),
            ],
            type=pa.timestamp("ms"),
        ),
        # timestampz (milliseconds) - not supported
        # pa.array(
        #     [
        #         datetime.datetime(2025, 11, 1, 10, 0, 0, tzinfo=datetime.timezone.utc),
        #         datetime.datetime(2025, 11, 1, 11, 30, 0, tzinfo=datetime.timezone.utc),
        #         datetime.datetime(2025, 11, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
        #     ],
        #     type=pa.timestamp("ms", "UTC"),
        # ),
        # time32
        pa.array(
            [
                datetime.time(9, 0, 0),
                datetime.time(10, 30, 15),
                datetime.time(11, 45, 30),
            ],
            type=pa.time32("s"),
        ),
        # list
        pa.array(
            [pa.array([-1, 2]), pa.array([3, 4, 5]), pa.array([])],
            type=pa.list_(pa.int64()),
        ),
        # decimal128
        pa.array(
            [decimal.Decimal("-54.321"), decimal.Decimal("123.45"), None],
            type=pa.decimal128(precision=10, scale=5),
        ),
        # json
        pa.array(
            ['{"a": 5, "b": { "c": 1.1 } }', '{ "d": "str", "e" : [1,2,3] }', "{}"],
            type=pa.string(),
        ),
        # record
        pa.array(
            [
                {"name": "Taco", "age": 3, "avg": 2.2},
                {"name": "Burger", "age": 2, "avg": 4.5},
                {"name": "SlimJim", "age": 1, "avg": 1.14},
            ],
            type=record_col,
        ),
        # uuid
        pa.array(
            [
                uuid.UUID("badc0deb-adc0-deba-dc0d-ebadc0debadc").bytes,
                uuid.UUID("deadbeef-dead-4eef-8eef-deaddeadbeef").bytes,
                uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
            ],
            type=pa.uuid(),
        ),
    ]

    field_names = [
        "int8_col",
        "uint8_col",
        "int16_col",
        "uint16_col",
        "int32_col",
        "uint32_col",
        "int64_col",
        "uint64_col",
        "float32_col",
        "float64_col",
        "bool_col",
        "string_col",
        "binary_col",
        "date32_col",
        "timestamp_ms_col",
        # "timestampz_ms_col", -- timestamp with timezone not supported
        "time32_col",
        "list_col",
        "decimal_col",
        "json_col",
        "record_col",
        "uuid_col",
    ]
    table = pa.Table.from_arrays(arrays, names=field_names)

    for compression in ["none", "snappy", "gzip", "brotli", "zstd", "lz4"]:
        suffix = "" if compression == "none" else f".{compression}"
        local_file = dir / f"types.parquet{suffix}"
        pq.write_table(table, local_file, compression=compression)  # type: ignore
