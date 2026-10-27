#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# prepare-corpus.sh populates the corpora of the `ProtoScalarType`-shaped proto
# round-trip targets with hand-encoded, structurally valid messages.
#
# A byte-wise mutator cannot assemble a nested length-delimited protobuf on its
# own: every enclosing message carries a length prefix that has to agree with
# the payload it wraps, and prost rejects the whole message the moment one does
# not, so no partial attempt ever earns coverage feedback to build on. Measured
# from an empty corpus, 1.4M executions of `column_type_proto_roundtrip` never
# once produced a `Timestamp` scalar type. Dictionary tokens for the tags do not
# help, because the tag is not the part the mutator gets wrong.
#
# The seeds therefore cover the parameterized variants of `ProtoScalarType`,
# which are the ones carrying a type parameter that a decoder can get wrong:
# `Numeric`, `Timestamp`, `TimestampTz`, `Char` and `VarChar`, plus the
# recursive wrappers `Array`, `List`, `Map`, `Record` and `Range` that nest
# them. Mutating a valid seed's parameter bytes is cheap, so this is what puts
# the domain oracles in those targets within reach.
#
# Every seed carries the one-byte mode prefix the target reads off the front of
# the input, which is the byte that selects the raw-bytes arm these seeds are
# for. The value differs per target, so it is spelled out at each `write` call
# rather than shared. Without it the seed's own leading tag is eaten as the mode
# byte and the remainder decodes as garbage.

set -euo pipefail

cd "$(dirname "$0")"

python3 - <<'PY'
import os

def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)

def tag(field: int, wire_type: int) -> bytes:
    return varint((field << 3) | wire_type)

def ld(field: int, payload: bytes) -> bytes:
    """A length-delimited field: nested message, string or bytes."""
    return tag(field, 2) + varint(len(payload)) + payload

def vi(field: int, value: int) -> bytes:
    return tag(field, 0) + varint(value)

# Field numbers from `src/repr/src/relation_and_scalar.proto`. The oneof is not
# dense and not ordered, so these are spelled out rather than derived.
EMPTY_KINDS = {"bool": 1, "int64": 4, "date": 8, "string": 15, "jsonb": 18}
NUMERIC, CHAR, VARCHAR, ARRAY, LIST, RECORD, MAP = 7, 16, 17, 20, 21, 22, 24
RANGE, TIMESTAMP, TIMESTAMPTZ = 33, 37, 38

def scalar(kind_field: int, payload: bytes = b"") -> bytes:
    return ld(kind_field, payload)

# ProtoOptionalNumericMaxScale { ProtoNumericMaxScale value = 1 }, whose own
# `value = 1` is the u8 scale. An absent inner message is `numeric` with no
# declared scale, which is a distinct decode path worth its own seed.
def numeric(max_scale: int | None) -> bytes:
    inner = b"" if max_scale is None else ld(1, vi(1, max_scale))
    return scalar(NUMERIC, inner)

def timestamp(kind_field: int, precision: int | None) -> bytes:
    inner = b"" if precision is None else ld(1, vi(1, precision))
    return scalar(kind_field, inner)

def char_like(kind_field: int, length: int | None) -> bytes:
    inner = b"" if length is None else ld(1, vi(1, length))
    return scalar(kind_field, inner)

def column_type(scalar_bytes: bytes, nullable: bool) -> bytes:
    body = ld(1, scalar_bytes)
    if nullable:
        body += vi(2, 1)
    return body

BOOL = scalar(EMPTY_KINDS["bool"])
INT64 = scalar(EMPTY_KINDS["int64"])

# The full set the seeds are built from. Values sit at the edges of each
# parameter's legal domain, which is where an off-by-one in a decoder's bound
# check shows up.
SCALARS = {
    **{name: scalar(field) for name, field in EMPTY_KINDS.items()},
    "numeric_none": numeric(None),
    "numeric_0": numeric(0),
    "numeric_39": numeric(39),
    "timestamp_none": timestamp(TIMESTAMP, None),
    "timestamp_0": timestamp(TIMESTAMP, 0),
    "timestamp_6": timestamp(TIMESTAMP, 6),
    "timestamptz_none": timestamp(TIMESTAMPTZ, None),
    "timestamptz_6": timestamp(TIMESTAMPTZ, 6),
    "char_none": char_like(CHAR, None),
    "char_1": char_like(CHAR, 1),
    "char_max": char_like(CHAR, 10_485_759),
    "varchar_none": char_like(VARCHAR, None),
    "varchar_1": char_like(VARCHAR, 1),
    "varchar_max": char_like(VARCHAR, 10_485_759),
    # Recursive wrappers, each nesting a parameterized leaf so that mutating the
    # inner parameter stays reachable from a seed.
    "array_int64": scalar(ARRAY, INT64),
    "array_timestamp": scalar(ARRAY, timestamp(TIMESTAMP, 6)),
    "list_char": scalar(LIST, ld(1, char_like(CHAR, 1))),
    "map_varchar": scalar(MAP, ld(1, char_like(VARCHAR, 1))),
    "range_timestamp": scalar(RANGE, ld(1, timestamp(TIMESTAMP, 6))),
    # ProtoRecord { repeated ProtoRecordField fields = 1 }, where a field is
    # { ProtoColumnName ColumnName = 1, ProtoColumnType ColumnType = 2 }.
    "record_two_fields": scalar(
        RECORD,
        ld(1, ld(1, ld(1, b"a")) + ld(2, column_type(INT64, False)))
        + ld(1, ld(1, ld(1, b"b")) + ld(2, column_type(numeric(39), True))),
    ),
}

def write(target: str, raw_mode: int, seeds: dict[str, bytes]) -> None:
    corpus = os.path.join("corpus", target)
    os.makedirs(corpus, exist_ok=True)
    for stale in os.listdir(corpus):
        if stale.startswith("seed_") and stale.endswith(".bin"):
            os.remove(os.path.join(corpus, stale))
    for name, blob in seeds.items():
        with open(os.path.join(corpus, f"seed_{name}.bin"), "wb") as f:
            f.write(bytes([raw_mode]) + blob)
    print(f"  {corpus:<46} {len(seeds):4d} seeds")

print("Seeded:")

# `mode & 1 == 1` selects the raw-bytes arm.
write("scalar_type_proto_roundtrip", 1, SCALARS)

# `nullable` is a bare proto3 bool, so `false` is absent from the wire. Both
# spellings are seeded to keep a presence regression on that field visible.
write(
    "column_type_proto_roundtrip",
    1,
    {
        **{f"{name}_notnull": column_type(s, False) for name, s in SCALARS.items()},
        **{f"{name}_nullable": column_type(s, True) for name, s in SCALARS.items()},
    },
)

# ProtoRelationDesc { ProtoRelationType typ = 1, repeated ProtoColumnName names = 2,
#                     repeated ProtoColumnMetadata metadata = 3 }
# ProtoRelationType { repeated ProtoColumnType column_types = 1, repeated ProtoKey keys = 2 }
# The decoder zips `metadata` against `column_types`, so a seed with a mismatched
# count is rejected at the boundary and teaches the mutator nothing. Seeds keep
# the counts equal and let mutation break them.
def relation_desc(scalars: list[bytes], keys: list[list[int]]) -> bytes:
    columns = b"".join(ld(1, column_type(s, i % 2 == 1)) for i, s in enumerate(scalars))
    key_msgs = b"".join(ld(2, b"".join(vi(1, k) for k in key)) for key in keys)
    names = b"".join(ld(2, ld(1, f"c{i}".encode())) for i in range(len(scalars)))
    # ProtoColumnMetadata { ProtoRelationVersion added = 1, dropped = 2 }, a
    # version being { uint64 value = 1 }. Column 0 is dropped at version 2 so
    # that the added/dropped bookkeeping is exercised, not just the happy path.
    metadata = b"".join(
        ld(3, ld(1, vi(1, 0)) + (ld(2, vi(1, 2)) if i == 0 else b""))
        for i in range(len(scalars))
    )
    return ld(1, columns + key_msgs) + names + metadata

# This target has three arms and dispatches on `mode % 3`, so 2 is the raw one.
write(
    "relation_desc_proto_roundtrip",
    2,
    {
        # `typ` is a required field, so a zero-column desc still carries it. An
        # entirely empty message is rejected at the boundary like any garbage.
        "no_cols": relation_desc([], []),
        "one_col": relation_desc([INT64], []),
        "keyed": relation_desc([INT64, BOOL], [[0], [0, 1]]),
        "parameterized": relation_desc(
            [timestamp(TIMESTAMP, 6), char_like(CHAR, 1), numeric(39)], [[1]]
        ),
    },
)
PY
