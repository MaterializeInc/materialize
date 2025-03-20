#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Regenerates a Materialize-dialect Chroma syntax file using the local Materialize keywords"""

import argparse
import xml.etree.ElementTree as ET
from pathlib import Path

from materialize import MZ_ROOT

CONFIG_FIELDS = {
    "name": "Materialize SQL dialect",
    "alias": ["materialize", "mzsql"],
    "mime_type": "text/x-materializesql",
}


def keyword_pattern():
    keywords_file = MZ_ROOT / "src/sql-lexer/src/keywords.txt"
    keywords = [
        line.upper()
        for line in keywords_file.read_text().splitlines()
        if not (line.startswith("#") or not line.strip())
    ]
    return f"({'|'.join(keywords)})\\b"


def set_keywords(root: ET.Element):
    rule = root.find(".//rule/token[@type='Keyword']/..")
    if not rule:
        raise RuntimeError("No keyword rule found")
    rule.set("pattern", keyword_pattern())


def set_config(root: ET.Element):
    config = root.find("config")
    if not config:
        raise RuntimeError("No config found")
    for field_name, field_value in CONFIG_FIELDS.items():
        if isinstance(field_value, list):
            for element in config.findall(field_name):
                config.remove(element)
            for item in field_value:
                field = ET.SubElement(config, field_name)
                field.text = item
        else:
            field = config.find(field_name)
            if field is None:
                raise RuntimeError(f"No such config field: '{field_name}'")
            field.text = field_value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--chroma-dir",
        default="../chroma",
    )
    args = parser.parse_args()
    lexer_dir = Path(f"{args.chroma_dir}/lexers/embedded/")
    tree = ET.parse(lexer_dir / "postgresql_sql_dialect.xml")
    root = tree.getroot()
    if not root:
        raise RuntimeError("Could not find root element")
    set_keywords(root)
    set_config(root)
    ET.indent(root, "  ")
    tree.write(lexer_dir / "materialize_sql_dialect.xml", encoding="unicode")


if __name__ == "__main__":
    main()
