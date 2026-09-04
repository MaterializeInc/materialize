# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Runs every criterion benchmark at an ancestor commit and at the current commit,
then fails when a benchmark regressed beyond a threshold.
"""

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from materialize import MZ_ROOT, buildkite, git, spawn
from materialize.cargo_bench.compare import (
    CompareReport,
    Verdict,
    compare,
    render_markdown,
)
from materialize.cargo_bench.targets import (
    BenchTarget,
    bench_targets,
    cargo_bench_args,
)
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser

SERVICES = []

BASELINE = "ancestor"

REPORTS_ARTIFACT = "criterion-reports.tar.zst"


@dataclass(frozen=True)
class TargetFailure:
    target: BenchTarget
    returncode: int


def resolve_ancestor() -> str:
    """Pick the commit to compare against: the merge base in a pull request, else the latest release tag."""
    if buildkite.is_in_pull_request():
        return buildkite.get_merge_base()
    current = MzVersion.parse_cargo()
    # Release candidates carry a semver prerelease and would otherwise win
    # over the release they precede.
    releases = [
        v
        for v in git.get_version_tags(version_type=MzVersion)
        if v.prerelease is None and v < current
    ]
    if not releases:
        raise RuntimeError(f"no release tag older than {current} found")
    return git.rev_parse(f"{max(releases)}^{{commit}}")


def target_dir() -> Path:
    """Resolve `CARGO_TARGET_DIR`, defaulting to `<MZ_ROOT>/target`.

    A relative override is resolved against `MZ_ROOT`, not the process cwd,
    since mzcompose workflows can be invoked from any directory.
    """
    value = Path(os.getenv("CARGO_TARGET_DIR", str(MZ_ROOT / "target")))
    return value if value.is_absolute() else MZ_ROOT / value


def load_targets(cwd: Path, packages: list[str]) -> list[BenchTarget]:
    metadata = json.loads(
        spawn.capture(["cargo", "metadata", "--no-deps", "--format-version=1"], cwd=cwd)
    )
    targets = bench_targets(metadata)
    if packages:
        targets = [t for t in targets if t.package in packages]
    return targets


def run_targets(
    targets: list[BenchTarget],
    cwd: Path,
    env: dict[str, str],
    criterion_args: list[str],
) -> list[TargetFailure]:
    """Run each target on its own so one failure does not take down the rest. Returns the failures."""
    failures = []
    base_criterion_home = Path(env["CRITERION_HOME"])
    for target in targets:
        print(f"--- Benchmarking {target.package}/{target.name} in {cwd}")
        # Benchmark ids are only unique within a target, so a collision
        # across targets sharing one criterion home would silently merge two
        # unrelated benchmarks.
        target_env = dict(
            env,
            CRITERION_HOME=str(base_criterion_home / target.package / target.name),
        )
        try:
            spawn.runv(
                cargo_bench_args(target, criterion_args), cwd=cwd, env=target_env
            )
        except subprocess.CalledProcessError as e:
            print(f"^^^ +++ {target.package}/{target.name} failed with {e.returncode}")
            failures.append(TargetFailure(target, e.returncode))
    return failures


def run_ancestor(
    ancestor: str,
    packages: list[str],
    env: dict[str, str],
) -> list[TargetFailure]:
    """Build and bench every target at `ancestor` in a scratch worktree. Returns the failures."""
    worktree = Path(tempfile.mkdtemp(prefix="cargo-bench-ancestor-"))
    added = False
    try:
        # A cancelled or timed-out job destroys the container's /tmp worktree
        # before the `finally` block below runs to remove it, and CI agents
        # reuse their checkout across jobs, so stale registrations would
        # otherwise accumulate in `.git/worktrees`.
        spawn.runv(["git", "worktree", "prune"], cwd=MZ_ROOT)
        # `git worktree add` can fail on a bad ancestor ref, in which case
        # there is no worktree to remove, only the scratch directory to clean up.
        spawn.runv(
            ["git", "worktree", "add", "--detach", str(worktree), ancestor],
            cwd=MZ_ROOT,
        )
        added = True
        targets = load_targets(worktree, packages)
        return run_targets(targets, worktree, env, ["--save-baseline", BASELINE])
    finally:
        if added:
            try:
                spawn.runv(
                    ["git", "worktree", "remove", "--force", str(worktree)],
                    cwd=MZ_ROOT,
                )
            except subprocess.CalledProcessError as e:
                # A failing remove must not mask an in-flight exception from
                # the try block above.
                print(f"^^^ +++ removing worktree {worktree} failed: {e}")
        shutil.rmtree(worktree, ignore_errors=True)


def render_report(
    ancestor: str | None,
    threshold: float,
    report: CompareReport,
    ancestor_failures: list[TargetFailure],
    current_failures: list[TargetFailure],
) -> str:
    sections = []
    if ancestor is not None:
        sections.append(
            f"Ancestor: `{ancestor}`, regression threshold: {threshold:.0%}"
        )
    else:
        sections.append("Ancestor run skipped, no comparison performed")
    if current_failures:
        sections.append(
            "Bench targets failing on the current commit:\n"
            + "\n".join(
                f"* `{f.target.package}/{f.target.name}` exited with {f.returncode}"
                for f in current_failures
            )
        )
    if ancestor_failures:
        sections.append(
            "Bench targets skipped on the ancestor (their benchmarks show as new):\n"
            + "\n".join(
                f"* `{f.target.package}/{f.target.name}` exited with {f.returncode}"
                for f in ancestor_failures
            )
        )
    if report.warnings:
        sections.append("Warnings:\n" + "\n".join(f"* {w}" for w in report.warnings))
    sections.append(render_markdown(report))
    return "\n\n".join(sections)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--ancestor",
        help="commit to compare against, defaults to the merge base in a PR and the latest release tag otherwise",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.10,
        help="relative mean slowdown that counts as a regression",
    )
    parser.add_argument(
        "--package",
        action="append",
        default=[],
        help="only run bench targets of this package, repeatable",
    )
    parser.add_argument(
        "--skip-ancestor",
        action="store_true",
        help="run only the current commit, no comparison",
    )
    args = parser.parse_args()
    if args.skip_ancestor and args.ancestor:
        parser.error("--ancestor has no effect with --skip-ancestor")

    current_targets = load_targets(MZ_ROOT, args.package)
    if not current_targets:
        raise RuntimeError("no bench targets found")

    criterion_home = target_dir() / "criterion-compare"
    # Stale baselines from an earlier run would silently become the
    # comparison target, so start from an empty directory every time.
    shutil.rmtree(criterion_home, ignore_errors=True)
    criterion_home.mkdir(parents=True)
    env = dict(
        os.environ,
        CARGO_TARGET_DIR=str(target_dir()),
        CRITERION_HOME=str(criterion_home),
    )

    ancestor: str | None = None
    ancestor_failures: list[TargetFailure] = []
    if not args.skip_ancestor:
        ancestor = args.ancestor or resolve_ancestor()
        assert ancestor is not None
        print(f"--- Comparing against ancestor {ancestor}")
        ancestor_failures = run_ancestor(ancestor, args.package, env)

        # Criterion's --save-baseline leaves a `new/` copy of the ancestor
        # run behind in addition to the baseline it saves. An id absent at
        # HEAD would otherwise keep that copy and get reported as a HEAD
        # result carrying the ancestor's numbers. Criterion recreates `new/`
        # on the HEAD run and only reads `ancestor/estimates.json` and
        # `ancestor/sample.json` for comparison, so removing it here is safe.
        for d in criterion_home.rglob("new"):
            if d.is_dir():
                shutil.rmtree(d)

    current_failures = run_targets(
        current_targets, MZ_ROOT, env, ["--baseline-lenient", BASELINE]
    )

    report = compare(criterion_home, args.threshold)
    markdown = render_report(
        ancestor, args.threshold, report, ancestor_failures, current_failures
    )
    print(markdown)

    failed = report.has_regressions or bool(current_failures)
    if buildkite.is_in_buildkite():
        buildkite.add_annotation(
            "error" if failed else "info", "Cargo bench results", markdown
        )
        try:
            spawn.runv(
                [
                    "tar",
                    "-caf",
                    REPORTS_ARTIFACT,
                    "-C",
                    str(criterion_home.parent),
                    criterion_home.name,
                ],
                cwd=MZ_ROOT,
            )
            buildkite.upload_artifact(REPORTS_ARTIFACT, cwd=MZ_ROOT)
            (MZ_ROOT / REPORTS_ARTIFACT).unlink(missing_ok=True)
        except subprocess.CalledProcessError as e:
            print(f"^^^ +++ uploading criterion reports failed: {e}")

    if current_failures:
        raise RuntimeError(
            f"{len(current_failures)} bench target(s) failed on the current commit"
        )
    if report.has_regressions:
        regressions = [r.id for r in report.results if r.verdict == Verdict.REGRESSION]
        raise RuntimeError(f"benchmark regressions: {', '.join(regressions)}")
