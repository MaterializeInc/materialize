# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Formal verification of Materialize's type system using Coq and Lean 4.

Discovers formal specs in `formal/coq/` and `formal/lean/` and verifies
them using the respective proof checkers.
"""

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service

SERVICES = [
    Service(
        "coq",
        {
            "mzbuild": "coq",
            "volumes": [
                f"{MZ_ROOT}:/repo",
            ],
        },
    ),
    Service(
        "lean4",
        {
            "mzbuild": "lean4",
            "volumes": [
                f"{MZ_ROOT}:/repo",
            ],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    """Run all formal verification checks."""

    def process(name: str) -> None:
        with c.test_case(name):
            checks[name](c)

    checks = {
        "coq": check_coq,
        "lean": check_lean,
    }
    c.test_parts(list(checks.keys()), process)


def check_coq(c: Composition) -> None:
    """Compile and verify all Coq proofs."""
    coq_dir = MZ_ROOT / "formal" / "coq"
    assert (coq_dir / "Types.v").exists(), "formal/coq/Types.v not found"

    # Read .v files in dependency order from _CoqProject.
    coq_project = coq_dir / "_CoqProject"
    v_files = []
    if coq_project.exists():
        for line in coq_project.read_text().splitlines():
            line = line.strip()
            if line.endswith(".v"):
                v_files.append(line)
    else:
        v_files = sorted(f.name for f in coq_dir.glob("*.v"))

    spec_dir = f"/repo/{coq_dir.relative_to(MZ_ROOT)}"
    for v_file in v_files:
        print(f"Checking {v_file}...")
        c.run(
            "coq",
            "-R",
            ".",
            "FormalTypeSystem",
            v_file,
            entrypoint=f"sh -c 'cd {spec_dir} && coqc \"$@\"' --",
        )

    print(f"All {len(v_files)} Coq files verified successfully.")


def check_lean(c: Composition) -> None:
    """Build and verify all Lean 4 proofs."""
    lean_dir = MZ_ROOT / "formal" / "lean"
    assert (lean_dir / "lakefile.lean").exists(), "formal/lean/lakefile.lean not found"

    spec_dir = f"/repo/{lean_dir.relative_to(MZ_ROOT)}"
    c.run(
        "lean4",
        "build",
        entrypoint=f"sh -c 'cd {spec_dir} && lake build'",
    )

    print("All Lean proofs verified successfully.")
