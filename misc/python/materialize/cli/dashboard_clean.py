#!/usr/bin/env python3

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Clean the overview dashboard json model for public consumption

This script strips out some extra flags and variables that are meaningless outside of our
internal infrastructure so that external users can use the dashboard without too much of
our cruft.
"""


import json
import re
import sys
import os
from typing import Dict, Any

import click


LOAD_TEST_LABELS = [
    r'''environment=~\"$env\"''',
    r'''environment=\"$env\"''',
    r'''commit_time=~\"$commit_time\"''',
    r'''git_ref=~\"$git_ref\"''',
    r'''id=~\"$id\"''',
    r'''test=~\"$test\"''',
    r'''purpose=~\"$purpose\"''',
    r'''workflow=~\"$workflow\"''',
]

VAR_ALLOWLIST = set(
    ["command", "timely_workers", "built_at", "build_version", "build_sha"]
)


@click.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-i", "--inplace/--no-inplace", default=False)
def main(source: os.PathLike, inplace: bool) -> None:
    """Clean the overview dashboard json model for public consumption
    """
    with open(source) as fh:
        content = fh.read()
    for label in LOAD_TEST_LABELS:
        content = content.replace(label, "")
    content = re.sub(r",,+", ",", content)
    content = content.replace("{,", "{").replace(",}", "}")
    data = json.loads(content)
    for panel in data["panels"]:
        for target in panel.get("targets", []):
            target["expr"] = strip_empty_labels(target["expr"])
        # rows show up as panels, and so can hold panels
        for panel_ in panel.get("panels", {}):
            for target in panel_.get("targets", []):
                target["expr"] = strip_empty_labels(target["expr"])
    data["version"] = 1
    data["title"] = "Materialize Overview"

    lst = data["templating"]["list"]
    data["templating"]["list"] = [
        clean_var(i) for i in lst if i["name"] in VAR_ALLOWLIST
    ]

    if inplace:
        with open(source, "w") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
    else:
        json.dump(data, sys.stdout, indent=2, sort_keys=True)
        print()


def clean_var(var: Dict[str, Any]) -> Dict[str, Any]:
    # Command is the only one that has a default that we want to preserve
    if var["name"] != "command":
        var["current"] = {}

    for key in ("definition", "query"):
        if key in var:
            var[key] = strip_empty_labels(var[key])
    return var


def strip_empty_labels(val: str) -> str:
    return re.sub(r"{,*}", "", val)


if __name__ == "__main__":
    main()
