#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Converts inline system-catalog reference tables in the docs into YAML data
entries plus a `catalog-relation` shortcode, the inverse of the table parser
in `lint-docs-catalog.py`.
"""

import re

# Matches Markdown reference-style link definitions, e.g. `[`uint8`]: /sql/types/uint8`.
REFERENCE_DEF_RE = re.compile(r"^\[(?P<label>[^\]]+)\]:\s*(?P<url>\S+)\s*$")
# Reference-style two-part links, e.g. `[text][label]`.
TWO_PART_LINK_RE = re.compile(r"\[([^\]]+)\]\[([^\]]+)\]")
# Reference-style shortcut links, e.g. `[text]`, as long as they are not
# immediately followed by `(` (inline link) or `[` (two-part link).
SHORTCUT_LINK_RE = re.compile(r"\[([^\]]+)\](?!\(|\[)")

HEADING_RE = re.compile(r"^#{2,6} `")
HEADER_SEPARATOR_RE = re.compile(r"\|?(\s*-+\s*)(\|\s*-+\s*){2}\|?")
# Field names are enclosed in backticks.
FIELD_NAME_RE = re.compile(r"`(.*)`")
# Field types are enclosed in backticks and optionally in square brackets.
FIELD_TYPE_RE = re.compile(r"\[?`(.*)`\]?")
RELATION_MARKER_RE = re.compile(r"<!-- RELATION_SPEC (\w+)\.(\w+)([^>]*)-->")


def parse_reference_defs(lines: list[str]) -> dict[str, str]:
    """Parse Markdown reference-style link definitions (`[label]: url`) into a
    map from label (verbatim, backticks included) to URL."""
    refs = {}
    for line in lines:
        m = REFERENCE_DEF_RE.match(line)
        if m:
            refs[m.group("label")] = m.group("url")
    return refs


def inline_links(text: str, refs: dict[str, str]) -> str:
    """Convert reference-style links (`[text][label]` and shortcut `[text]`)
    into inline links (`[text](url)`) using `refs`. Existing inline links and
    unknown labels are left untouched."""

    def two_part_sub(m: re.Match) -> str:
        link_text, label = m.group(1), m.group(2)
        if label in refs:
            return f"[{link_text}]({refs[label]})"
        return m.group(0)

    text = TWO_PART_LINK_RE.sub(two_part_sub, text)

    def shortcut_sub(m: re.Match) -> str:
        link_text = m.group(1)
        if link_text in refs:
            return f"[{link_text}]({refs[link_text]})"
        return m.group(0)

    # The negative lookahead in SHORTCUT_LINK_RE ensures already-inline links
    # (produced above or already present in the source) are never rewritten.
    text = SHORTCUT_LINK_RE.sub(shortcut_sub, text)
    return text


def _parse_table(
    lines: list[str], start: int, refs: dict[str, str]
) -> tuple[list[dict], int]:
    """Parse a Markdown table starting at or after `lines[start]`. Returns the
    parsed columns and the index of the last consumed line.

    Mirrors lint-docs-catalog.py's state machine: the header title row is
    skipped (not parsed), only the separator row is used to locate where body
    rows begin.
    """
    idx = start
    if idx < len(lines) and lines[idx].strip() == "":
        idx += 1  # allow one optional blank line before the table
    while idx < len(lines) and not HEADER_SEPARATOR_RE.match(lines[idx]):
        idx += 1  # skip the header title row (and anything else) until the separator
    idx += 1  # move past the separator row, onto the first body row

    columns = []
    last_row_idx = idx - 1
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped.startswith("|"):
            break
        fields = [f.strip() for f in stripped[1:].split("|")]
        if len(fields) < 3:
            raise ValueError(f"unexpected field format: {stripped}")
        field_match = FIELD_NAME_RE.search(fields[0])
        type_match = FIELD_TYPE_RE.search(fields[1])
        if not field_match or not type_match:
            raise ValueError(f"unexpected field format: {stripped}")
        columns.append(
            {
                "name": field_match.group(1),
                "type": type_match.group(1),
                "meaning": inline_links(fields[2], refs),
            }
        )
        last_row_idx = idx
        idx += 1

    return columns, last_row_idx


def migrate(md_text: str) -> tuple[list[dict], str, dict[str, str]]:
    """Extract migratable relation tables from `md_text` into YAML-ready data
    entries, and rewrite those tables into `catalog-relation` shortcodes.

    A relation is migratable when it has a `RELATION_SPEC schema.object`
    marker whose modifiers do not include `FROM_YAML` (already migrated) or
    `NO_COMMENTS` (no comment table to migrate). `RELATION_SPEC_UNDOCUMENTED`
    markers never match and are left untouched.

    Returns `(relation_entries, rewritten_md, types_seen)`, where
    `types_seen` maps each plain column type name encountered to its
    reference-definition URL, if one exists.
    """
    lines = md_text.splitlines()
    refs = parse_reference_defs(lines)

    heading_idx = None
    entries = []
    types_seen: dict[str, str] = {}
    # (replace_start, replace_end_inclusive, schema, object) spans to rewrite,
    # applied back-to-front so earlier indices stay valid.
    rewrite_spans = []

    idx = 0
    while idx < len(lines):
        if HEADING_RE.match(lines[idx]):
            heading_idx = idx
            idx += 1
            continue

        marker = RELATION_MARKER_RE.search(lines[idx])
        if not marker:
            idx += 1
            continue

        schema, object_name, mods = marker.group(1), marker.group(2), marker.group(3)
        if "FROM_YAML" in mods or "NO_COMMENTS" in mods:
            idx += 1
            continue

        if heading_idx is None:
            raise ValueError(
                f"no heading found before RELATION_SPEC marker for {schema}.{object_name}"
            )
        description_start = heading_idx + 1
        description_lines = lines[description_start:idx]
        while description_lines and description_lines[0].strip() == "":
            description_lines.pop(0)
        while description_lines and description_lines[-1].strip() == "":
            description_lines.pop()
        description = inline_links("\n".join(description_lines), refs)

        columns, last_table_row_idx = _parse_table(lines, idx + 1, refs)
        for column in columns:
            type_name = column["type"]
            ref_url = refs.get("`" + type_name + "`")
            if ref_url is not None:
                types_seen[type_name] = ref_url

        entries.append(
            {"name": object_name, "description": description, "columns": columns}
        )
        rewrite_spans.append(
            (description_start, last_table_row_idx, schema, object_name)
        )

        idx = last_table_row_idx + 1

    rewritten_lines = list(lines)
    for description_start, last_table_row_idx, schema, object_name in reversed(
        rewrite_spans
    ):
        replacement = [
            "",
            f"<!-- RELATION_SPEC {schema}.{object_name} FROM_YAML -->",
            f'{{{{< catalog-relation schema="{schema}" name="{object_name}" >}}}}',
        ]
        rewritten_lines[description_start : last_table_row_idx + 1] = replacement

    rewritten_md = "\n".join(rewritten_lines)
    if md_text.endswith("\n"):
        rewritten_md += "\n"

    return entries, rewritten_md, types_seen


if __name__ == "__main__":
    import sys

    md_path = sys.argv[
        1
    ]  # e.g. doc/user/content/reference/system-catalog/mz_introspection.md
    md_text = open(md_path, encoding="utf-8").read()
    entries, rewritten, types = migrate(md_text)

    # Schema is the data file basename (matches the markers' schema).
    import os

    schema_name = os.path.splitext(os.path.basename(md_path))[0]
    data_path = os.path.join("doc", "user", "data", f"{schema_name}.yml")

    import yaml

    existing = {}
    if os.path.exists(data_path):
        existing = yaml.safe_load(open(data_path, encoding="utf-8")) or {}
    relations = existing.get("relations", [])
    have = {r["name"] for r in relations}
    for e in entries:
        if e["name"] not in have:
            relations.append(e)
    existing["relations"] = relations

    types_path = os.path.join("doc", "user", "data", "catalog_types.yml")
    types_map = {}
    if os.path.exists(types_path):
        types_map = yaml.safe_load(open(types_path, encoding="utf-8")) or {}
    for k, v in types.items():
        types_map.setdefault(k, v)

    open(md_path, "w", encoding="utf-8").write(rewritten)
    with open(data_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(existing, f, sort_keys=False, allow_unicode=True, width=10_000)
    with open(types_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(types_map, f, sort_keys=False, allow_unicode=True, width=10_000)
    print(f"migrated {len(entries)} relations into {data_path}")
