# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check that every `mz_*` metric named by the skill resolves in the catalog.

Run from the repository root. Exits non-zero and names the offenders on failure.
"""

import fnmatch
import pathlib
import re
import sys

SKILL = pathlib.Path(".agents/skills/mz-release-signoff")
CATALOG = pathlib.Path("doc/user/data/metrics.yml")
ALLOWLIST = SKILL / "scripts" / "metrics-allowlist.txt"

CATALOG_NAME = re.compile(r"^- name: '?(.+?)'?$")
BACKTICKED = re.compile(r"`([^`]+)`")
CANDIDATE = re.compile(r"\b(mz_[a-z0-9_]+)\b")

# Histograms and summaries are catalogued as their expanded families, so a
# reference naming the base is correct and must resolve through any suffix.
SUFFIXES = ("_bucket", "_count", "_sum")


def catalog_names():
    """Return (exact names, glob patterns) from the catalog.

    A `metric!` whose name is built with `format!` is catalogued with its
    placeholders globbed, for example `mz_persist_*_bytes`, so the catalog is
    a mix of literal names and patterns and membership is not a set lookup.
    """
    exact, globs = set(), set()
    for line in CATALOG.read_text().splitlines():
        match = CATALOG_NAME.match(line)
        if match:
            name = match.group(1)
            (globs if "*" in name else exact).add(name)
    # A base name is resolvable when any member of its family is catalogued.
    bases = {
        name.rsplit("_", 1)[0]
        for name in exact
        if name.rsplit("_", 1)[-1] in ("bucket", "count", "sum")
    }
    return exact | bases, globs


def resolves(name, exact, globs):
    if name in exact:
        return True
    # Try the histogram and summary suffixes against the patterns too, so a
    # reference naming the base of a globbed family still resolves.
    candidates = [name] + [name + suffix for suffix in SUFFIXES]
    return any(
        fnmatch.fnmatchcase(candidate, pattern)
        for candidate in candidates
        for pattern in globs
    )


def allowlisted():
    names = set()
    for line in ALLOWLIST.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.add(line)
    return names


def referenced():
    """Yield (name, file) for every mz_* token inside backticks in the skill."""
    for path in sorted(SKILL.rglob("*.md")):
        for token in BACKTICKED.findall(path.read_text()):
            # A prefix wildcard such as `mz_persist_*` names a family, not a
            # metric, and its stem is not itself catalogued. Nothing here can
            # be checked, so skip the whole token.
            if "*" in token:
                continue
            for name in CANDIDATE.findall(token):
                # Brace expansion such as `mz_foo_{sum,count}` leaves a
                # trailing underscore. Resolve the base, which the catalog
                # holds as an expanded family.
                yield name.rstrip("_"), path


def main() -> int:
    if not CATALOG.exists():
        print(f"{CATALOG} not found; run from the repository root", file=sys.stderr)
        return 2

    exact, globs = catalog_names()
    allowed = allowlisted()

    unresolved = {}
    for name, path in referenced():
        if name in allowed or resolves(name, exact, globs):
            continue
        unresolved.setdefault(name, set()).add(str(path))

    stale = sorted(n for n in allowed if resolves(n, exact, globs))
    if stale:
        print("Allowlisted names that now resolve in the catalog; remove them:")
        for name in stale:
            print(f"  {name}")
        print()

    if unresolved:
        print(
            "Metric names in the skill that resolve in neither the catalog nor the allowlist:"
        )
        for name in sorted(unresolved):
            print(f"  {name}  ({', '.join(sorted(unresolved[name]))})")
        print()
        print("Either the metric was renamed, in which case fix the reference, or the")
        print(
            f"catalog cannot see it, in which case add it to {ALLOWLIST} with a reason."
        )

    return 1 if (unresolved or stale) else 0


if __name__ == "__main__":
    sys.exit(main())
