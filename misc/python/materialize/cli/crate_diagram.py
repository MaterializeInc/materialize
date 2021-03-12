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
Generate a dependency graph of our local crates

We have hundreds of crates in our dependency tree, so visualizing the full set of dependencies is
fairly useless (but can be done with crates like cargo-graph).

This script just prints the relationship of our internal crates to help see how things fit into the
materialize ecosystem.
"""

import subprocess
import sys
import os
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import cast, Any, Dict, IO, List, Optional

import click
import toml

from materialize.spawn import runv


@click.command()
@click.option("--include-timely/--no-include-timely")
@click.option(
    "--create-diagram",
    default=None,
    help="The svg diagram to create, otherwise just print dot to stdout",
)
def main(include_timely: bool, create_diagram: Optional[str]) -> None:
    root = Path(os.environ["MZ_ROOT"])
    root_cargo = root / "Cargo.toml"
    with root_cargo.open() as fh:
        data = toml.load(fh)

    members = set()
    areas = defaultdict(list)
    member_meta = {}
    all_deps = {}
    for member_path in data["workspace"]["members"]:
        path = root / member_path / "Cargo.toml"
        with path.open() as fh:
            member = toml.load(fh)
        has_bin = any(root.joinpath(member_path).glob("src/**/main.rs"))
        name = member["package"]["name"]

        member_meta[name] = {
            "has_bin": has_bin,
            "description": member["package"].get("description", name),
        }
        area = member_path.split("/")[0]
        areas[area].append(name)
        members.add(name)
        all_deps[name] = [dep for dep in member.get("dependencies", [])]

    if include_timely:
        members.add("timely")
        members.add("differential-dataflow")
    local_deps = {
        dep_name: [dep for dep in dep_deps if dep in members]
        for dep_name, dep_deps in all_deps.items()
    }

    if include_timely:
        local_deps["differential-dataflow"] = []
        local_deps["timely"] = []
        areas["timely"] = ["differential-dataflow", "timely"]
        member_meta["differential-dataflow"] = {"has_bin": False, "description": ""}
        member_meta["timely"] = {"has_bin": False, "description": ""}

    if create_diagram is not None:
        out = NamedTemporaryFile(mode="w+", prefix="mz-arch-diagram-")
    else:
        out = sys.stdout

    output(member_meta, local_deps, areas, out)

    if create_diagram:
        cmd = ["dot", "-Tsvg", "-o", create_diagram, out.name]
        out.flush()
        try:
            runv(cmd, capture_output=True)
        except subprocess.CalledProcessError as e:
            out.seek(0)
            debug = "/tmp/debug.gv"
            with open(debug, "w") as fh:
                fh.write(out.read())
            if e.stderr:
                print(e.stderr.decode("utf-8"), file=sys.stderr)
            if e.stdout:
                print(e.stdout.decode("utf-8"))
            print(f"ERROR running dot, source in {debug}")
    else:
        print("didn't create diag")


def output(
    member_meta: Dict[str, Dict[str, str]],
    local_deps: Dict[str, List[str]],
    areas: Dict[str, List[str]],
    out: IO,
) -> None:
    def disp(val: str, out: IO = out, **kwargs: Any) -> None:
        print(val, file=out, **kwargs)

    disp("digraph packages {")
    for area, members in areas.items():
        disp(f"    subgraph cluster_{area} {{")
        disp(f'        label = "/{area}";')
        disp(f"        color = blue;")
        for member in members:
            description = member_meta[member]["description"]
            disp(f'        "{member}" [tooltip="{description}"', end="")
            if member_meta[member]["has_bin"]:
                disp(",shape=Mdiamond,color=red", end="")
            disp("];")
        disp("    }")

    for package, deps in local_deps.items():
        for dep in deps:
            disp(
                f'    "{package}" -> "{dep}" [edgetooltip="{package} -> {dep}",URL="none"',
                end="",
            )
            if dep in ("timely", "differential-dataflow"):
                disp("color=green,style=dashed", end="")
            disp("];")
    disp("}")


if __name__ == "__main__":
    main()
