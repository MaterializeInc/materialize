# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import pytest

from materialize.antithesis import upsert_recovery_model as model


def test_json_null_is_a_value_and_null_payload_is_a_tombstone():
    records = [model.Record(0, 0, "key", "null")]
    assert not model.validate(records, [("key", "null")], False)
    assert model.validate(records, [], False)
    assert model.validate(records, [("key", None)], False)
    records.append(model.Record(0, 1, "key", None))
    assert not model.validate(records, [], False)
    assert model.validate(records, [("key", "null")], False)
    assert model.validate(records, None, False)


def test_errors_replace_prior_values_and_repair_retracts_every_error():
    records = [model.Record(0, 0, "a", "1"), model.Record(0, 1, "b", "2")]
    records.extend((model.Record(0, 2, "a", "{"), model.Record(0, 3, "a", "[,")))
    assert not model.validate(records, None, True)
    assert model.validate(records, [("a", "1"), ("b", "2")], False)
    records.append(model.Record(0, 4, "a", ' {"n":3} '))
    assert not model.validate(records, [("a", '{"n":3}'), ("b", "2")], False)
    assert model.validate(records, None, True)
    assert model.validate(records, [("a", "1"), ("b", "2")], False)
    assert model.validate(records, [("a", '{"n":3}')] * 2 + [("b", "2")], False)


def test_model_refuses_ambiguous_key_order_and_conflicting_acknowledgements():
    with pytest.raises(ValueError, match="partitions"):
        model.expected([model.Record(0, 0, "a", "1"), model.Record(1, 0, "a", "2")])
    with pytest.raises(ValueError, match="conflicting"):
        model.expected([model.Record(0, 0, "a", "1"), model.Record(0, 0, "a", "2")])


def test_canonical_compares_numbers_by_value_and_keeps_the_last_duplicate_key():
    same = [
        ('{"n": 1e2, "f": 2.50}', '{"f":2.5,"n":100}'),
        ("-0.0", "0"),
        ("1E-7", "0.0000001"),
        ("12345678901234567890123456789", "12345678901234567890123456789"),
        ("1.2345678901234567890123", "1.2345678901234567890123"),
        ('{"dup": 1, "dup": 2}', '{"dup":2}'),
        ('"\\u00e9\\n"', '"é\\n"'),
        ('[true, null, "1"]', '[true,null,"1"]'),
    ]
    for document, stored in same:
        assert model.canonical(document) == model.canonical(stored), document
    assert model.canonical("1") != model.canonical('"1"')
    assert model.canonical("1.5") != model.canonical("1.50000000000000000001")
    for invalid in ("NaN", '{"a": Infinity}', "01", "[1,]", "", "{'a': 1}"):
        with pytest.raises(ValueError):
            model.canonical(invalid)


def test_inline_errors_show_each_key_as_its_value_or_its_error():
    records = [
        model.Record(0, 0, "a", '{"n": 1e0}'),
        model.Record(0, 1, "b", "{"),
        model.Record(0, 2, "c", "1"),
        model.Record(0, 3, "c", None),
    ]
    good = [("a", '{"n":1}', None), ("b", None, '("Failed to decode JSON")')]
    assert not model.validate_inline(records, good)
    assert model.validate_inline(records, good[:1])
    assert model.validate_inline(records, [good[0], ("b", "1", None)])
    assert model.validate_inline(records, [good[0], ("b", "1", "error")])
    assert model.validate_inline(records, [("a", '{"n":2}', None), good[1]])
    assert model.validate_inline(records, [*good, ("c", "1", None)])
    assert model.validate_inline(records, [*good, good[0]])
