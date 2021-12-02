#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
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

import os
import subprocess
import sys
import webbrowser
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO, Any, DefaultDict, Dict, List, Set, Tuple

import click
import toml

from materialize.spawn import runv

DepBuilder = DefaultDict[str, List[str]]
DepMap = Dict[str, List[str]]


def split_list(items: str) -> List[str]:
    if items:
        return items.split(",")
    return []


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--roots",
    default="",
    type=split_list,
    help="Only include these crates and their dependencies.",
)
@click.option(
    "--show/--no-show",
    default=True,
    help="Wheather or not to immediatly show the generated diagram",
)
@click.option(
    "--diagram-file", default="crates.svg", help="The diagram file to generate."
)
def main(show: bool, diagram_file: str, roots: List[str]) -> None:
    root = Path(os.environ["MZ_ROOT"])
    root_cargo = root / "Cargo.toml"
    with root_cargo.open() as fh:
        data = toml.load(fh)

    members = set()
    areas: DefaultDict[str, List[str]] = defaultdict(list)
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

    # timely is "local" but not in our repo
    members.add("timely")
    members.add("differential-dataflow")

    local_deps: DepMap = {
        dep_name: [dep for dep in dep_deps if dep in members]
        for dep_name, dep_deps in all_deps.items()
    }

    local_deps["differential-dataflow"] = []
    local_deps["timely"] = []
    areas["timely"] = ["differential-dataflow", "timely"]
    member_meta["differential-dataflow"] = {"has_bin": False, "description": ""}
    member_meta["timely"] = {"has_bin": False, "description": ""}

    if roots:
        (local_deps, areas) = filter_to_roots(areas, local_deps, roots)

    here = Path(os.environ["MZ_ROOT"])
    diagram_file = here / diagram_file
    with NamedTemporaryFile(mode="w+", prefix="mz-arch-diagram-") as out:
        write_dot_graph(member_meta, local_deps, areas, out)

        cmd = ["dot", "-Tsvg", "-o", str(diagram_file), out.name]
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
        except FileNotFoundError as e:
            raise click.ClickException(
                f"This script requires the dot program (part of the graphviz package): {e}"
            )

    add_hover_style(diagram_file)

    if show:
        uri = f"file:///{diagram_file}"
        webbrowser.open(uri)


def filter_to_roots(
    areas: DepBuilder, local_deps: DepMap, roots: List[str]
) -> Tuple[DepMap, DepBuilder]:
    new_deps: DefaultDict[str, Set[str]] = defaultdict(set)

    try:
        add_deps(local_deps, new_deps, roots)
    except KeyError as e:
        raise click.ClickException(f"Unknown crate {e}")
    new_deps = {root: list(deps) for root, deps in new_deps.items()}

    filtered_crates = set()
    for root, deps in new_deps.items():
        filtered_crates.add(root)
        filtered_crates.update(deps)

    new_areas = defaultdict(list)
    for area, children in areas.items():
        for child in children:
            if child in filtered_crates:
                new_areas[area].append(child)

    return (new_deps, new_areas)


def add_deps(
    deps: DepMap, new_deps: DefaultDict[str, Set[str]], roots: List[str]
) -> None:
    for root in roots:
        for dep in deps[root]:
            new_deps[root].add(dep)
        add_deps(deps, new_deps, deps[root])


def write_dot_graph(
    member_meta: Dict[str, Dict[str, str]],
    local_deps: DepMap,
    areas: Dict[str, List[str]],
    out: IO,
) -> None:
    def disp(val: str, out: IO = out, **kwargs: Any) -> None:
        print(val, file=out, **kwargs)

    disp("digraph packages {")
    for area, members in areas.items():
        disp(f"    subgraph cluster_{area} " "{")
        disp(f'        label = "/{area}";')
        disp("        color = blue;")
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
    out.flush()


def add_hover_style(diagram_file: Path) -> None:
    found_svg = False
    with open(diagram_file, "r") as fh:
        lines = fh.readlines()
        for i, line in enumerate(lines):
            if "<svg" in line:
                found_svg = True
            if found_svg and ">" in line:
                lines.insert(i + 1, HOVER_STYLE)
                break

    with open(diagram_file, "w") as fh:
        fh.write("".join(lines))


HOVER_STYLE = """\
<style>
  /* edge lines */
  .edge:active path,
  .edge:hover path {
    stroke: fuchsia;
    stroke-width: 3;
    stroke-opacity: 1;
  }
  /* edge finishing arrows */
  .edge:active polygon,
  .edge:hover polygon {
    stroke: fuchsia;
    stroke-width: 3;
    fill: fuchsia;
    stroke-opacity: 1;
    fill-opacity: 1;
  }
  /* edge decoration text */
  .edge:active text,
  .edge:hover text {
    fill: fuchsia;
  }
</style>
"""

if __name__ == "__main__":
    main()
