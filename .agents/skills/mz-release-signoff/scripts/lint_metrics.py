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
# Markdown allows a fence to open with backticks or tildes and to be indented by
# up to three spaces, and leaves an unterminated fence running to end of file.
# Missing any of those shapes costs silent under-coverage rather than a failure,
# so match them all. A fence nested in a longer one would need a backreference on
# the opening run, which the skill's markdown does not call for.
FENCED = re.compile(r"^ {0,3}(?:`{3}|~{3}).*?(?:^ {0,3}(?:`{3}|~{3})|\Z)", re.S | re.M)
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

    Return every name the continuation resolves to, longest base first. How
    many components the continuation drops is not stated by the row, so every
    cut of the base has to be tried: the roster writes both
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
    candidates = [
        "_".join(parts[:cut]) + continuation for cut in range(len(parts) - 1, 0, -1)
    ]
    return [
        candidate
        for candidate in candidates
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

    A wildcard names a family whose stem is not itself catalogued, so a name
    written as a glob stem is skipped. The test is per name rather than per
    whitespace-delimited word, because a PromQL selector puts a label matcher
    such as {mz_version!~".*-dev.*"} in the same word as the metric name.
    """
    for match in CANDIDATE.finditer(token):
        name = match.group(1)
        follows = token[match.end() : match.end() + 1]
        if follows == "*":
            continue
        # A trailing underscore continues the name only when something follows
        # it: brace expansion such as `mz_foo_{sum,count}`, whose base the
        # catalog holds as an expanded family. Standing alone the token is a
        # prefix named in prose, as in "its `mz_compute_` prefix".
        if name.endswith("_"):
            if follows != "{":
                continue
            name = name.rstrip("_")
        yield name


def names_in_document(text):
    """Yield (label, name, base) for every metric named in one markdown file.

    `base` is None for a name written out in full, and the name an abbreviation
    attaches to otherwise.

    Fenced blocks have to be pulled out before backticks are paired. A fence
    contains backticks of its own, so pairing sequentially across one flips the
    parity of every span after it: prose gets captured as code and the real
    code spans become the separators between matches. Left unhandled, that
    silently disables the check for every file containing a fence.
    """
    for block in FENCED.findall(text):
        for name in names_in(block):
            yield name, name, None
    # Continuations abbreviate within a single roster row, so the base is only
    # sought on the same line. Tracking it across lines attaches a `_sum` to
    # whatever full name happened to appear in an earlier paragraph, which
    # manufactures failures rather than finding them.
    for line in FENCED.sub("\n", text).splitlines():
        previous = None
        for span in BACKTICKED.findall(line):
            stripped = span.strip()
            # A bare histogram suffix is prose about the parts of a histogram,
            # as in "the `_sum` rate of `mz_slow_message_handling`", never a
            # family member abbreviated in a roster row.
            if stripped in SUFFIXES:
                continue
            if CONTINUATION.match(stripped):
                # With no in-scope base on this line the continuation belongs
                # to a family the catalog does not hold, such as v2_mz_* or
                # container_*, and cannot be checked.
                if previous:
                    yield f"{stripped} (after {previous})", stripped, previous
                continue
            for name in names_in(span):
                yield name, name, None
                previous = name


def referenced():
    for path in sorted(SKILL.rglob("*.md")):
        for label, name, base in names_in_document(path.read_text()):
            yield label, name, base, path


# How many names each file contributes. A parity bug drops a whole file at once
# and changes no name that survives, so a count is what catches it, and the
# version of this lint that shipped without one checked nothing in three of the
# six files while exiting 0. Update deliberately when a reference gains or loses
# a metric, never to make the test pass.
REFERENCE_NAME_COUNTS = {
    "SKILL.md": 18,
    "adapter.md": 35,
    "compute.md": 31,
    "persist.md": 84,
    "reference-dashboards.md": 24,
    "sources-and-sinks.md": 63,
}


def reference_name_counts():
    counts = {}
    for _, _, _, path in referenced():
        counts[path.name] = counts.get(path.name, 0) + 1
    return counts


def main() -> int:
    if not CATALOG.exists():
        print(f"{CATALOG} not found; run from the repository root", file=sys.stderr)
        return 2

    exact, globs = catalog_names()
    allowed = allowlisted()

    unresolved = {}
    ambiguous = {}
    for label, name, base, path in referenced():
        if base is not None:
            hits = resolve_continuation(name, base, exact, globs, allowed)
            if len(hits) > 1:
                ambiguous.setdefault(label, (hits, set()))[1].add(str(path))
                continue
            if hits:
                continue
        elif name in allowed or resolves(name, exact, globs):
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
