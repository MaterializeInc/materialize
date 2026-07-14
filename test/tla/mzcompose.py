# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Formal verification of Materialize using TLA+ / TLC.

Discovers all TLA+ specifications (*.tla files with a matching *.cfg) in the
repository and runs the TLC model checker against each one.
"""

from pathlib import Path

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service

SERVICES = [
    Service(
        "tlc",
        {
            "mzbuild": "tlc",
            "volumes": [
                f"{MZ_ROOT}:/repo",
            ],
        },
    ),
]


def find_tla_specs() -> list[Path]:
    """Find all .tla files in the repo that have a matching .cfg file."""
    specs = []
    for tla_file in sorted(MZ_ROOT.rglob("*.tla")):
        cfg_file = tla_file.with_suffix(".cfg")
        if cfg_file.exists():
            specs.append(tla_file)
    return specs


def workflow_default(c: Composition) -> None:
    """Run all TLA+ model checking specs."""
    specs = find_tla_specs()
    assert specs, "No TLA+ specs found (*.tla with matching *.cfg)"

    def process(name: str) -> None:
        with c.test_case(name):
            spec = spec_map[name]
            run_tlc(c, spec)

    spec_map = {spec.stem: spec for spec in specs}
    c.test_parts(list(spec_map.keys()), process)


def run_tlc(c: Composition, tla_file: Path) -> None:
    """Run TLC on a single spec file."""
    cfg_file = tla_file.with_suffix(".cfg")
    spec_dir = f"/repo/{tla_file.parent.relative_to(MZ_ROOT)}"

    result = c.run(
        "tlc",
        "-config", cfg_file.name,
        "-workers", "auto",
        tla_file.name,
        capture=True,
        entrypoint=f"sh -c 'cd {spec_dir} && java -XX:+UseParallelGC -jar /opt/tla2tools.jar \"$@\"' --",
    )
    print(result.stdout)
    assert "No error has been found" in result.stdout, (
        f"TLC found errors in {tla_file.name}:\n{result.stdout}"
    )
