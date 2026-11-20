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
