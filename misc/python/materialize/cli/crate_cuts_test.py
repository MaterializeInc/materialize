# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for `crate-cuts`.

The split tests use a synthetic workspace: crate `a` has modules `a/base.rs`
and `a/heavy.rs`, where only `heavy.rs` references the external crate `big`.
Crate `b` uses only `base.rs`, crate `c` uses both.
"""

from collections import Counter
from pathlib import Path

from materialize import scip
from materialize.cli.crate_cuts import (
    Model,
    Module,
    Package,
    candidate_cuts,
    metrics,
    module_reach,
    pin_impl,
)


def test_impl_header() -> None:
    def header(descriptors: str) -> tuple[str, str | None] | None:
        return scip.Symbol("p", "0", descriptors).impl_header()

    assert header("row/impl#[Row]pack().") == ("Row", None)
    assert header("row/impl#[Row][Debug]fmt().") == ("Row", "Debug")
    assert header("update/impl#[`SharedSlice<T>`][`From<Vec<T>>`]from().") == (
        "SharedSlice",
        "From",
    )
    assert header("ts/impl#[Timestamp][`PartialEq<&Timestamp>`]eq().") == (
        "Timestamp",
        "PartialEq",
    )
    assert header("row/Row#") is None


def test_type_name() -> None:
    assert scip.Symbol("p", "0", "row/Row#").type_name() == "Row"
    assert scip.Symbol("p", "0", "Row#").type_name() == "Row"
    assert scip.Symbol("p", "0", "row/Row#field.").type_name() is None
    assert scip.Symbol("p", "0", "row/impl#[Row]").type_name() is None


def test_parse_symbol() -> None:
    sym = scip.parse_symbol("rust-analyzer cargo mz-repr 0.0.0 row/Row#")
    assert sym == scip.Symbol("mz-repr", "0.0.0", "row/Row#")
    assert scip.parse_symbol("local 17") is None
    module = scip.parse_symbol("rust-analyzer cargo mz-repr 0.0.0 row/")
    assert module is not None and module.is_module


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _field(number: int, payload: bytes) -> bytes:
    return _varint(number << 3 | 2) + _varint(len(payload)) + payload


def test_read_documents() -> None:
    occurrence = (
        _field(1, b"".join(_varint(v) for v in [3, 4, 200]))
        + _field(2, b"rust-analyzer cargo p 0 a/B#")
        + _varint(3 << 3)
        + _varint(scip.ROLE_DEFINITION)
        + _field(7, b"".join(_varint(v) for v in [3, 0, 9, 1]))
    )
    document = _field(4, b"rust") + _field(1, b"src/a.rs") + _field(2, occurrence)
    index = _field(1, b"") + _field(2, document)

    [doc] = scip.read_documents(index)
    assert doc.relative_path == "src/a.rs"
    [occ] = doc.occurrences
    assert occ.symbol == "rust-analyzer cargo p 0 a/B#"
    assert occ.roles == scip.ROLE_DEFINITION
    assert occ.range == [3, 4, 200]
    assert occ.enclosing_range == [3, 0, 9, 1]


def _model() -> Model:
    def pkg(name: str, deps: set[str], workspace: bool = True) -> Package:
        return Package(name, name, "0", Path(name), workspace, True, deps=deps)

    packages = {
        "big": pkg("big", set(), workspace=False),
        "a": pkg("a", {"big"}),
        "b": pkg("b", {"a"}),
        "c": pkg("c", {"a"}),
    }
    modules = {
        "a/base.rs": Module("a/base.rs", "a", "lib", 10),
        "a/heavy.rs": Module(
            "a/heavy.rs",
            "a",
            "lib",
            10,
            refs=Counter({"a/base.rs": 1}),
            external=Counter({"big": 1}),
        ),
        "b/lib.rs": Module("b/lib.rs", "b", "lib", 10, refs=Counter({"a/base.rs": 1})),
        "c/lib.rs": Module("c/lib.rs", "c", "lib", 10, refs=Counter({"a/heavy.rs": 1})),
    }
    return Model(packages, modules)


def test_split_rewires_consumers() -> None:
    model = _model()
    before = metrics(model)
    assert before.workspace_pairs == 2
    assert before.all_pairs == 5

    cuts = list(candidate_cuts(model, "a"))
    assert cuts == [(["big"], {"a/base.rs"})]

    core = model.split("a", {"a/base.rs"})
    assert model.packages[core].deps == set()
    assert model.packages["b"].deps == {core}, "b only uses core"
    assert model.packages["c"].deps == {"a"}, "c uses rest"
    assert model.packages["a"].deps == {"big", core}

    after = metrics(model)
    # b no longer reaches `big` or `a`, c reaches the new core through a.
    assert after.all_pairs == 6
    assert after.workspace_pairs == 4


def test_snapshot_restore() -> None:
    model = _model()
    before = metrics(model)
    snap = model.snapshot()
    model.split("a", {"a/base.rs"})
    model.restore(snap)
    assert metrics(model) == before
    assert set(model.packages) == {"big", "a", "b", "c"}
    assert model.modules["a/base.rs"].package == "a"


def test_pins_keep_impls_with_their_type() -> None:
    model = _model()
    heavy = model.modules["a/heavy.rs"]
    pin_impl(heavy, "Base", None, {("a", "Base"): {"a/base.rs"}})
    assert heavy.pinned == {"a/base.rs"}
    # The pin drags `base.rs` along with `heavy.rs`, so no split remains.
    assert module_reach(model, "a")["a/base.rs"] == {"big"}
    assert list(candidate_cuts(model, "a")) == []
