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
FENCED = re.compile(r"^```.*?^```", re.S | re.M)
# A roster row abbreviates a family as `mz_foo_bar`, `_baz`, `_qux`.
CONTINUATION = re.compile(r"^_[a-z0-9_]+$")

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


def resolve_continuation(continuation, base, exact, globs, allowed):
    """Resolve `_baz` against the family of a preceding `mz_foo_bar`.

    Return every prefix of the base that the continuation resolves against,
    longest first. How many components the continuation drops is not stated by
    the row, so every cut has to be tried: the roster writes both
    `mz_persist_gc_seconds`, `_started`, which drops one, and
    `mz_compute_controller_replica_count`, `_peek_count`, which drops two.

    More than one hit means the row is not pinned to a single metric. That
    matters on removal rather than today, because the longest cut is the
    intended one and comes first: delete the intended metric and a shorter cut
    still resolves, so the row stays green while pointing at nothing. The
    caller rejects an ambiguous row for that reason, and the fix is to spell
    the member out in full.
    """
    parts = base.split("_")
    return [
        candidate
        for cut in range(len(parts) - 1, 0, -1)
        for candidate in ["_".join(parts[:cut]) + continuation]
        if candidate in allowed or resolves(candidate, exact, globs)
    ]


def allowlisted():
    names = set()
    for line in ALLOWLIST.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.add(line)
    return names


def names_in(token):
    """Yield every catalogued-namespace metric name inside one code token.

    A wildcard names a family whose stem is not itself catalogued, so it is
    skipped. The test is per word rather than per token because a fenced block
    arrives as one token, and a single wildcard inside it must not exempt the
    whole block.
    """
    for word in token.split():
        if "*" in word:
            continue
        for name in CANDIDATE.findall(word):
            yield name.rstrip("_")


def referenced():
    """Yield (label, target, file) for every metric named in the skill.

    Fenced blocks have to be pulled out before backticks are paired. A fence
    contains backticks of its own, so pairing sequentially across one flips the
    parity of every span after it: prose gets captured as code and the real
    code spans become the separators between matches. Left unhandled, that
    silently disables the check for every file containing a fence.
    """
    for path in sorted(SKILL.rglob("*.md")):
        text = path.read_text()
        for block in FENCED.findall(text):
            for name in names_in(block):
                yield name, name, path
        # Continuations abbreviate within a single roster row, so the base is
        # only sought on the same line. Tracking it across lines attaches a
        # `_sum` to whatever full name happened to appear in an earlier
        # paragraph, which manufactures failures rather than finding them.
        for line in FENCED.sub("\n", text).splitlines():
            previous = None
            for span in BACKTICKED.findall(line):
                stripped = span.strip()
                # A bare histogram suffix is prose about the parts of a
                # histogram, as in "the `_sum` rate of `mz_slow_message_handling`",
                # never a family member abbreviated in a roster row.
                if stripped in SUFFIXES:
                    continue
                if CONTINUATION.match(stripped):
                    # With no in-scope base on this line the continuation
                    # belongs to a family the catalog does not hold, such as
                    # v2_mz_* or container_*, and cannot be checked.
                    if previous:
                        yield (
                            f"{stripped} (after {previous})",
                            (stripped, previous),
                            path,
                        )
                    continue
                for name in names_in(span):
                    yield name, name, path
                    previous = name


def main() -> int:
    if not CATALOG.exists():
        print(f"{CATALOG} not found; run from the repository root", file=sys.stderr)
        return 2

    exact, globs = catalog_names()
    allowed = allowlisted()

    unresolved = {}
    ambiguous = {}
    for label, target, path in referenced():
        if isinstance(target, tuple):
            continuation, base = target
            hits = resolve_continuation(continuation, base, exact, globs, allowed)
            if len(hits) > 1:
                ambiguous.setdefault(label, (hits, set()))[1].add(str(path))
                continue
            if hits:
                continue
        else:
            if target in allowed or resolves(target, exact, globs):
                continue
        unresolved.setdefault(label, set()).add(str(path))

    stale = sorted(n for n in allowed if resolves(n, exact, globs))
    if stale:
        print("Allowlisted names that now resolve in the catalog; remove them:")
        for name in stale:
            print(f"  {name}")
        print()

    if ambiguous:
        print("Abbreviated metric names that resolve more than one way:")
        for label in sorted(ambiguous):
            hits, paths = ambiguous[label]
            print(f"  {label}  ({', '.join(sorted(paths))})")
            print(f"    resolves to: {', '.join(hits)}")
        print()
        print("The row is not pinned to one metric, so removing the intended one")
        print("leaves the lint green. Spell the member out in full.")
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

    return 1 if (unresolved or stale or ambiguous) else 0


if __name__ == "__main__":
    sys.exit(main())
