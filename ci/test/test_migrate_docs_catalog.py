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

import pytest

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


def test_migrate_accepts_non_h2_heading_level():
    # mz_catalog.md uses `###` for relation headings; the converter must not
    # be hardcoded to `##`.
    md = """### `mz_qux`

The `mz_qux` view is documented under a level-3 heading.

<!-- RELATION_SPEC test_schema.mz_qux -->
| Field | Type      | Meaning |
|-------|-----------|---------|
| `id`  | [`uint8`] | The ID. |

[`uint8`]: /sql/types/uint8
"""
    entries, _, _ = migrate_docs_catalog.migrate(md)
    assert len(entries) == 1
    assert entries[0]["name"] == "mz_qux"
    assert "documented under a level-3 heading" in entries[0]["description"]


def test_migrate_raises_when_marker_has_no_preceding_heading():
    md = """<!-- RELATION_SPEC test_schema.mz_qux -->
| Field | Type      | Meaning |
|-------|-----------|---------|
| `id`  | [`uint8`] | The ID. |
"""
    with pytest.raises(ValueError, match="test_schema.mz_qux"):
        migrate_docs_catalog.migrate(md)


def test_parse_table_raises_on_malformed_row():
    lines = [
        "| Field | Type | Meaning |",
        "|-------|------|---------|",
        "| no_backticks_here | text | broken row |",
    ]
    with pytest.raises(ValueError, match="unexpected field format"):
        migrate_docs_catalog._parse_table(lines, 0, {})
