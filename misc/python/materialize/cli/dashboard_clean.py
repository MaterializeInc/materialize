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
from typing import List, Dict, Any, Optional

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
    r'''num_warehouses=~\"$num_warehouses\"''',
    r'''instance_type=~\"$ec2_instance_type\"''',
    r'''oltp_threads=~\"$oltp_threads\"''',
    r'''olap_threads=~\"$olap_threads\"''',
]

VAR_ALLOWLIST = set(
    [
        "datasource",
        "command",
        "instance",
        "timely_workers",
        "built_at",
        "build_version",
        "build_sha",
        "os",
        "cpu0",
        "ncpus_physical",
        "ncpus_logical",
        "memory_total",
    ]
)

VARS_FOR_WEB: List[Dict[str, Any]] = [
    {
        "current": None,
        "hide": 0,
        "includeAll": False,
        "label": "Datasource",
        "multi": False,
        "name": "datasource",
        "options": [],
        "query": "prometheus",
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "type": "datasource",
    },
    {
        "allFormat": "glob",
        "allValue": None,
        "current": {},
        "datasource": "$datasource",
        "definition": r'label_values(up{instance=~".*:6875"}, instance)',
        "hide": 0,
        "includeAll": False,
        "label": "Instance",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": r'label_values(up{instance=~".*:6875"}, instance)',
        "refresh": 1,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "tagValuesQuery": "",
        "tags": [],
        "tagsQuery": "",
        "type": "query",
        "useTags": False,
    },
]


@click.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-i", "--inplace/--no-inplace", default=False)
@click.option(
    "--for-web/--no-for-web",
    help="Update for our multi-datasource dashboard, instead of our user dashboard",
    default=False,
)
@click.option("--uid")
@click.option("--id")
@click.option("--version", type=click.INT)
@click.option("--title")
def main(
    source: os.PathLike,
    inplace: bool,
    for_web: bool,
    uid: Optional[str],
    id: Optional[str],
    version: Optional[int],
    title: Optional[str],
) -> None:
    """Clean the overview dashboard json model for public consumption"""
    if for_web and any(v is None for v in (uid, id, version, title)):
        print(
            "ERROR: --version, --uid, --title, and --id are all required with --for-web",
            file=sys.stderr,
        )
        sys.exit(1)
    with open(source) as fh:
        content = fh.read()
    for label in LOAD_TEST_LABELS:
        content = content.replace(label, "")
    content = re.sub(r",,+", ",", content)
    content = content.replace("{,", "{").replace(",}", "}")
    data = json.loads(content)

    for annotation in data["annotations"]["list"]:
        clean_prop(annotation, for_web)
    for panel in data["panels"]:
        clean_prop(panel, for_web)
        for target in panel.get("targets", []):
            clean_prop(target, for_web)
        # rows show up as panels, and so can hold panels
        for row_panel in panel.get("panels", {}):
            clean_prop(row_panel, for_web)
            for target in row_panel.get("targets", []):
                clean_prop(target, for_web)

    # adjust for public not needing meta
    data["panels"] = [p for p in data["panels"] if p["title"] != "Meta"]
    for p in data["panels"]:
        if p["title"] == "Materialize Build Info":
            p["gridPos"] = {"h": 4, "w": 7, "x": 17, "y": 5}

        elif p["title"] == "SQL Queries/second":
            p["fieldConfig"]["defaults"]["thresholds"]["steps"][0]["color"] = "green"

    if for_web:
        data["uid"] = uid
        if id is not None:
            data["id"] = None if id in ("null", "None") else int(id)
        else:
            data["id"] = None
        data["version"] = version
        data["title"] = title
    else:
        # this is used by the docker dashboard startup script, so be careful about changing it
        data["uid"] = "materialize-overview"
        data["id"] = None
        data["version"] = 0
        data["title"] = "Materialize Overview"

    lst = []
    if for_web:
        lst.extend(VARS_FOR_WEB)
    lst.extend(data["templating"]["list"])
    data["templating"]["list"] = [
        clean_var(i, for_web) for i in lst if i["name"] in VAR_ALLOWLIST
    ]

    if inplace:
        with open(source, "w") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
    else:
        json.dump(data, sys.stdout, indent=2, sort_keys=True)
        print()


def clean_prop(prop: Dict[str, Any], for_web: bool) -> None:
    if for_web:
        if "expr" in prop:
            prop["expr"] = insert_instance_filter(prop["expr"])
        if "datasource" in prop:
            prop["datasource"] = "$datasource"
        if prop.get("legendFormat") is not None:
            # id is an internal property, instance is unique for all users
            prop["legendFormat"] = prop["legendFormat"].replace(
                "{{id}}", "{{instance}}"
            )
    else:
        if "expr" in prop:
            prop["expr"] = strip_empty_labels(prop["expr"])


def clean_var(var: Dict[str, Any], for_web: bool) -> Dict[str, Any]:
    # Command is the only one that has a default that we want to preserve
    if var["name"] != "command":
        var["current"] = {}

    if var["name"] == "timely_workers":
        var["hide"] = 2

    for key in ("definition", "query"):
        if key in var:
            var[key] = strip_empty_labels(var[key])

    if for_web and var["name"] != "datasource":
        var["datasource"] = "$datasource"

    return var


def strip_empty_labels(val: str) -> str:
    return re.sub(r"{,*}", "", val)


def insert_instance_filter(val: str) -> str:
    if re.search("instance=~.*:6875", val) is not None:
        new_val = re.sub(r'instance=~"[^"]+"', r'instance=~"$instance"', val)
        if new_val == val:
            print(f"warning: val did not get instance filter: {val}", file=sys.stderr)
        val = new_val
    elif "instance" not in val:
        new_val = val.replace("{", r'{instance=~"$instance",')
        if new_val == val:
            print(f"warning: val did not get instance filter: {val}", file=sys.stderr)
        val = new_val
    elif "16875" in val:
        # TODO: handle stripping peeker metrics from this dashboard
        pass
    else:
        print(f"warning: couldn't insert instance in {val}", file=sys.stderr)
    return val.replace(",}", "}")


if __name__ == "__main__":
    main()
