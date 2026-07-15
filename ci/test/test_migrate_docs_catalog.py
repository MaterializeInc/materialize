# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pathlib

import importlib.util

_spec = importlib.util.spec_from_file_location(
    "migrate_docs_catalog",
    pathlib.Path(__file__).parent / "migrate-docs-catalog.py",
)
migrate_docs_catalog = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migrate_docs_catalog)

FIXTURE = (pathlib.Path(__file__).parent / "testdata" / "sample_catalog.md").read_text()


def test_reference_defs_parsed():
    refs = migrate_docs_catalog.parse_reference_defs(FIXTURE.splitlines())
    assert refs["`uint8`"] == "/sql/types/uint8"
    assert refs["dataflow"] == "/get-started/arrangements/#dataflows"


def test_inline_links_converts_reference_style():
    refs = {"dataflow": "/get-started/arrangements/#dataflows"}
    assert (
        migrate_docs_catalog.inline_links("in the [dataflow] layer", refs)
        == "in the [dataflow](/get-started/arrangements/#dataflows) layer"
    )


def test_inline_links_leaves_inline_untouched():
    refs = {}
    text = "see [x](/y) here"
    assert migrate_docs_catalog.inline_links(text, refs) == text


def test_migrate_extracts_documented_relation():
    entries, _, types = migrate_docs_catalog.migrate(FIXTURE)
    names = [e["name"] for e in entries]
    assert names == ["mz_foo"]  # mz_baz is NO_COMMENTS, skipped
    foo = entries[0]
    assert "It has more detail." in foo["description"]
    assert "[dataflow](/get-started/arrangements/#dataflows)" in foo["description"]
    assert foo["columns"] == [
        {
            "name": "id",
            "type": "uint8",
            "meaning": "The ID. Corresponds to [`mz_bar.id`](#mz_bar).",
        },
        {"name": "name", "type": "text", "meaning": "The name."},
    ]
    assert types == {"uint8": "/sql/types/uint8", "text": "/sql/types/text"}


def test_migrate_rewrites_markdown_section():
    _, rewritten, _ = migrate_docs_catalog.migrate(FIXTURE)
    assert "<!-- RELATION_SPEC test_schema.mz_foo FROM_YAML -->" in rewritten
    assert '{{< catalog-relation schema="test_schema" name="mz_foo" >}}' in rewritten
    # Old table gone.
    assert "| `id`" not in rewritten
    # Heading kept.
    assert "## `mz_foo`" in rewritten
    # UNDOCUMENTED sibling untouched.
    assert (
        "<!-- RELATION_SPEC_UNDOCUMENTED test_schema.mz_foo_per_worker -->" in rewritten
    )
    # NO_COMMENTS relation untouched (still an inline table).
    assert "<!-- RELATION_SPEC test_schema.mz_baz NO_COMMENTS -->" in rewritten
    assert "| `x`" in rewritten
    # Trailing prose after the mz_baz table preserved.
    assert "See [more](/somewhere) for details." in rewritten
