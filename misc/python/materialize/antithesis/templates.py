# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Generate measured workload templates from the authoritative Python commands."""

from __future__ import annotations

import argparse
import ast
import importlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

PROFILE_ENV = "MZ_ANTITHESIS_TEMPLATE_PROFILE"
_DRIVERS = "materialize.antithesis.drivers"
_COMMON = frozenset({"canaries", "pod_restarts", "recovery", "resource_kills"})
_PREFIXES = (
    "parallel_driver_",
    "serial_driver_",
    "singleton_driver_",
    "anytime_",
    "eventually_",
    "finally_",
    "first_",
)


@dataclass(frozen=True)
class Profile:
    drivers: frozenset[str] | None
    setup: tuple[str, ...]


PROFILES = {
    "main": Profile(None, ("postgres_sources", "kafka_sources", "kafka_sinks", "ivm")),
    "transactions": Profile(
        _COMMON
        | {"access_control", "counters", "history", "read_paths", "session_mix"},
        (),
    ),
    "cdc": Profile(
        _COMMON
        | {
            "kafka_sinks",
            "kafka_sources",
            "postgres_sources",
            "shard_audit",
            "source_status",
        },
        ("postgres_sources", "kafka_sources", "kafka_sinks"),
    ),
    "rollouts": Profile(
        _COMMON
        | {
            "access_control",
            "counters",
            "history",
            "ivm",
            "postgres_sources",
            "read_only_isolation",
            "rollouts",
            "shard_audit",
            "source_status",
            "spec_phase",
        },
        ("postgres_sources", "ivm"),
    ),
}


def profile(name: str) -> Profile:
    """Return the named profile, rejecting a misspelled or unsupported name."""
    try:
        return PROFILES[name]
    except KeyError:
        raise ValueError(f"unrecognized workload template {name!r}") from None


def enabled_driver(driver: str, name: str | None = None) -> bool:
    """Whether the chosen template can schedule a driver."""
    selected = profile(name or os.environ.get(PROFILE_ENV, "main"))
    return selected.drivers is None or driver in selected.drivers


def commands(source: Path) -> list[Path]:
    """Discover executable Python test commands in `source`."""
    result = []
    for path in sorted(source.iterdir()):
        if path.is_file() and path.name.startswith(_PREFIXES):
            if path.suffix != ".py" or not path.stat().st_mode & 0o111:
                raise ValueError(f"test command must be executable Python: {path}")
            result.append(path)
    if not result:
        raise ValueError(f"no test commands in {source}")
    if sum(path.name.startswith("first_") for path in result) != 1:
        raise ValueError("workload templates require exactly one first command")
    return result


def included(path: Path, selected: Profile) -> bool:
    """Whether a source command belongs in `selected`."""
    if selected.drivers is None or path.name.startswith("first_"):
        return True
    drivers = set()
    shared = False
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(f"{_DRIVERS}."):
                    drivers.add(alias.name.removeprefix(f"{_DRIVERS}.").split(".")[0])
                elif alias.name == "materialize.antithesis.telemetry":
                    shared = True
            continue
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if node.module.startswith(f"{_DRIVERS}."):
            drivers.add(node.module.removeprefix(f"{_DRIVERS}.").split(".")[0])
        elif node.module == _DRIVERS:
            drivers.update(alias.name for alias in node.names)
        elif node.module == "materialize.antithesis.telemetry":
            shared = True
        elif node.module == "materialize.antithesis":
            shared = shared or any(alias.name == "telemetry" for alias in node.names)
    if shared and not drivers:
        return True
    if len(drivers) != 1:
        raise ValueError(f"test command must identify one driver: {path}")
    return drivers <= selected.drivers


def generate(source: Path, output: Path) -> None:
    """Generate each template without modifying its source commands."""
    source, output = source.resolve(), output.resolve()
    if source.is_relative_to(output) or output.is_relative_to(source):
        raise ValueError("template source and output must not overlap")
    discovered = commands(source)
    for name, selected in PROFILES.items():
        destination = output / name
        if destination.exists():
            shutil.rmtree(destination)
        destination.mkdir(parents=True)
        for command in discovered:
            if not included(command, selected):
                continue
            wrapper = destination / command.name
            wrapper.write_text(
                "#!/usr/bin/env python3\n\n"
                "import sys\n\n"
                "from materialize.antithesis.telemetry import run_command\n\n"
                f"sys.exit(run_command({name!r}, {str(command)!r}))\n"
            )
            wrapper.chmod(0o755)


def initialize(name: str | None = None) -> int:
    """Configure the timeline and seed only the chosen template's workloads."""
    name = name or os.environ.get(PROFILE_ENV, "main")
    selected = profile(name)
    configure = importlib.import_module(f"{_DRIVERS}.configure")
    status = configure.main()
    db = configure.open_state()
    try:
        with db:
            db.execute(
                "INSERT OR REPLACE INTO profile VALUES ('workload_template', ?)",
                (name,),
            )
    finally:
        db.close()
    rollouts = importlib.import_module(f"{_DRIVERS}.rollouts")
    rollouts.seed_serving_version()
    for driver in selected.setup:
        importlib.import_module(f"{_DRIVERS}.{driver}").setup_main()
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate(args.source, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
