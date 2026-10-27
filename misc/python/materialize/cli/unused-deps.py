# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Report dependencies that no target of their crate uses.

Findings come from rustc's stable `unused_crate_dependencies` lint, which names
every dependency the compiler was handed but never loaded.

This deliberately does not use cargo-udeps. udeps infers usage from the binary
dependency depinfo rustc emits, and that depinfo names transitively loaded
crates as well as directly loaded ones. A direct dependency that is reachable
through any other dependency therefore looks used even when the crate under
study never mentions it. Nearly every direct dependency in this workspace is
transitively reachable, so udeps reported success no matter how much had
rotted, and a green run meant nothing. See https://github.com/est31/cargo-udeps/issues/84
"""

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict

import toml

from materialize import MZ_ROOT

# Target kinds that receive `[dependencies]`. Test, bench and example targets
# additionally receive `[dev-dependencies]`, and every one of them warns about
# each dev-dependency it does not personally use, so they are left out. The cost
# is that unused dev-dependencies and build-dependencies go unreported.
CHECKED_KINDS = {"lib", "bin", "proc-macro"}

LINT = "unused_crate_dependencies"


class CargoFailed(Exception):
    pass


def cargo_check() -> list[dict]:
    """Run the lint over the workspace and return the JSON messages."""
    env = dict(os.environ)
    env["RUSTFLAGS"] = f"{env.get('RUSTFLAGS', '')} -W {LINT}".strip()
    proc = subprocess.run(
        [
            "cargo",
            "check",
            "--workspace",
            "--lib",
            "--bins",
            "--features",
            "mz-alloc/jemalloc",
            "--message-format=json",
        ],
        cwd=MZ_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )
    messages = []
    for line in proc.stdout.splitlines():
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if proc.returncode != 0:
        for m in messages:
            if m.get("reason") == "compiler-message":
                if m["message"].get("level") == "error":
                    print(m["message"].get("rendered", ""), file=sys.stderr, end="")
        raise CargoFailed("cargo check failed; see errors above")
    return messages


def exempt(manifest_path: str) -> set[str]:
    """Names this crate's manifest excuses.

    Names are the ones rustc reports, which is the dependency's library name and
    so not always its package name: the `md-5` package is `md5` here. Both
    spellings are accepted for the common case where the two differ only in
    hyphens.

    Two sources. Naming a dependency in the `[features]` table is already a
    statement that the crate wants it for something other than its API, most
    often to turn on one of its Cargo features, so those are excused
    automatically. Anything else needs an explicit
    `[package.metadata.unused-deps] ignore` entry, which is how a dependency
    that is only reachable under an inactive cfg gets recorded.
    """
    with open(manifest_path) as f:
        manifest = toml.load(f)
    metadata = manifest.get("package", {}).get("metadata", {})
    names = set(metadata.get("unused-deps", {}).get("ignore", []))
    for enables in manifest.get("features", {}).values():
        for entry in enables:
            # "dep:foo" activates foo, "foo/bar" activates foo's bar feature.
            names.add(entry.removeprefix("dep:").split("/")[0])
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    try:
        messages = cargo_check()
    except CargoFailed as e:
        print(e, file=sys.stderr)
        return 1

    # `package_id` is an opaque URL, so map units back to packages by the
    # manifest path cargo reports alongside them.
    flagged: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    targets: dict[str, set[str]] = defaultdict(set)
    manifests: dict[str, str] = {}

    for m in messages:
        target = m.get("target") or {}
        kinds = set(target.get("kind") or [])
        if not kinds & CHECKED_KINDS:
            continue
        manifest = m.get("manifest_path")
        if not manifest or not manifest.startswith(str(MZ_ROOT)):
            continue
        unit = f"{target.get('name')} ({'/'.join(sorted(kinds))})"
        manifests[manifest] = manifest

        if m.get("reason") == "compiler-artifact":
            targets[manifest].add(unit)
        elif m.get("reason") == "compiler-message":
            message = m["message"]
            if (message.get("code") or {}).get("code") != LINT:
                continue
            # "extern crate `foo` is unused in crate `bar`"
            text = message.get("message", "")
            name = text.split("`")[1] if "`" in text else None
            if name:
                flagged[manifest][name].add(unit)

    findings: dict[str, list[str]] = {}
    for manifest, deps in flagged.items():
        built = targets[manifest]
        if not built:
            continue
        skip = exempt(manifest)
        unused = sorted(
            name
            for name, units in deps.items()
            # rustc prints the crate name, Cargo.toml spells it with hyphens.
            if name.replace("_", "-") not in skip
            and name not in skip
            and built <= units
        )
        if unused:
            findings[manifest] = unused

    if not findings:
        print("All dependencies are used.")
        return 0

    total = sum(len(v) for v in findings.values())
    print(f"Found {total} unused dependencies:\n", file=sys.stderr)
    for manifest in sorted(findings):
        rel = os.path.relpath(manifest, MZ_ROOT)
        print(f"  {rel}", file=sys.stderr)
        for name in findings[manifest]:
            print(f"      {name}", file=sys.stderr)
    print(
        "\nRemove each one from its crate's Cargo.toml. If it is genuinely needed but\n"
        "invisible here, because it is used only under an inactive cfg or exists to turn\n"
        "on one of its Cargo features, add it to that crate's Cargo.toml instead:\n"
        "\n"
        "    [package.metadata.unused-deps]\n"
        '    ignore = ["some-crate"]  # why it is needed\n',
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
