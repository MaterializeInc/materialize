# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import math
import random
from decimal import Decimal

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import upsert_recovery_model as model
from materialize.antithesis_driver import kafka
from materialize.antithesis_driver import kafka_formats as formats


def test_shows_accepts_the_text_materialize_renders() -> None:
    # Texts as v26.51.0 casts the columns of an Avro export.
    rendered = [
        ("float4", "1.5", 1.5),
        ("float4", "-0.1", formats.float4(-0.1)),
        ("float4", "NaN", math.nan),
        ("float8", "-2.25e+300", -2.25e300),
        ("float8", "Infinity", math.inf),
        ("float8", "-0", -0.0),
        ("bool", "true", True),
        ("bytes", "\\x00ff106162", b"\x00\xff\x10ab"),
        ("bytes", "\\x", b""),
        ("decimal", "-1234567890123456.7891", Decimal("-1234567890123456.7891")),
        ("decimal", "5", Decimal("5.0000")),
        ("date", "1969-12-31", -1),
        ("millis", "2026-01-02 03:04:05.123", 1767323045123),
        ("millis", "1970-01-01 00:00:00", 0),
        ("micros", "1999-12-31 23:59:59.999999", 946684799999999),
        ("micros", "2026-01-02 03:04:05.1", 1767323045100000),
        ("list", "{}", []),
        ("list", "{1,-2,3}", [1, -2, 3]),
        ("map", "{a=>-1,b=>2}", {"b": 2, "a": -1}),
        ("record", "(1,)", {"a": 1, "b": None}),
        ("record", "(-5,abc)", {"a": -5, "b": "abc"}),
        ("exact", "-9223372036854775808", -(2**63)),
        ("exact", "NULL", "NULL"),
        ("exact", None, None),
    ]
    for kind, text, value in rendered:
        assert formats.shows(kind, text, value), (kind, text, value)
    wrong = [
        ("float4", "1.25", 1.5),
        ("float8", "nonsense", 1.0),
        ("decimal", "5.0001", Decimal("5")),
        ("millis", "1970-01-01 00:00:00.001", 0),
        ("micros", "not a time", 0),
        ("list", "{1,2}", [1, -2]),
        ("map", "{a=>1}", {}),
        ("record", "(1,x)", {"a": 1, "b": None}),
        ("exact", None, "NULL"),
        ("exact", "NULL", None),
        ("bool", "t", True),
    ]
    for kind, text, value in wrong:
        assert not formats.shows(kind, text, value), (kind, text, value)


def test_avro_records_follow_from_their_seed_and_survive_the_wire_format() -> None:
    fastavro = pytest.importorskip("fastavro")
    registry = kafka.Registry("http://unused.invalid")
    for schema_id, evolved in ((1, False), (2, True)):
        registry._schemas[schema_id] = fastavro.parse_schema(
            formats.avro_value_schema(evolved)
        )
    for seed in range(300):
        datum = formats.avro_datum(seed)
        assert repr(datum) == repr(formats.avro_datum(seed))
        assert registry.decode(registry.encode(1, datum))[1]["seed"] == seed
        evolved = registry.decode(registry.encode(2, {**datum, "extra": "x"}))
        assert evolved[0] == 2 and evolved[1]["extra"] == "x"
    kinds = {kind for _, _, kind in formats.AVRO_FIELDS}
    assert kinds == {
        "exact",
        "float4",
        "float8",
        "bool",
        "bytes",
        "decimal",
        "date",
        "millis",
        "micros",
        "list",
        "map",
        "record",
    }
    with pytest.raises(ValueError):
        registry.decode(b"\x01\x00\x00\x00\x01")
    with pytest.raises(ValueError):
        registry.decode(b"\x00\x00\x00\x00\x01\x02")


def test_unfaithful_names_the_fields_that_differ_from_the_seed() -> None:
    datum = formats.avro_datum(7)
    texts = {name: None for name, _, _ in formats.AVRO_FIELDS}
    named = formats.unfaithful("7", tuple(texts.values()))
    nullable = {n for n, _, _ in formats.AVRO_FIELDS if datum[n] is None}
    assert set(named) == set(texts) - nullable
    assert formats.unfaithful("not a seed", ()) == ["seed"]
    assert formats.unfaithful(None, ()) == ["seed"]


def test_json_documents_are_json_and_undecodable_values_are_not() -> None:
    for seed in range(500):
        document = formats.json_document(random.Random(seed))
        canonical = model.canonical(document)
        assert model.canonical(canonical) == canonical, document
    for value in formats.UNDECODABLE:
        with pytest.raises(ValueError):
            model.canonical(value.decode("utf8", "replace"))


def test_a_check_reads_every_export_in_one_statement_of_equal_width() -> None:
    for cls in (kafka.KafkaSource, formats.JsonSource, formats.RecoverySource):
        source = cls.__new__(cls)
        query = source._query()
        selects = query.split(" UNION ALL ")
        assert len(selects) == 1 + len(source._exports())
        assert len({select.count(" AS c") for select in selects}) == 1, query
        assert f"FROM {cls.SOURCE}" in selects[0]
