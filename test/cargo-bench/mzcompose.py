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
    BuiltBench,
    bench_executables,
    bench_targets,
    cargo_build_args,
    package_manifests,
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


def load_targets(
    cwd: Path, packages: list[str], env: dict[str, str] | None = None
) -> tuple[list[BenchTarget], dict[str, str]]:
    """Enumerate bench targets and package manifest paths for one checkout via `cargo metadata`."""
    metadata = json.loads(
        spawn.capture(
            ["cargo", "metadata", "--no-deps", "--format-version=1"], cwd=cwd, env=env
        )
    )
    targets = bench_targets(metadata)
    if packages:
        targets = [t for t in targets if t.package in packages]
    return targets, package_manifests(metadata)


def build_benches(
    cwd: Path,
    targets: list[BenchTarget],
    manifests: dict[str, str],
    env: dict[str, str],
) -> tuple[list[BuiltBench], list[TargetFailure]]:
    """Compile every bench target of one checkout in as few `cargo bench --no-run` invocations as possible.

    A single invocation covering every target builds with full core
    parallelism, but cargo's message stream gives no way to tell which target
    broke when the invocation itself exits non-zero. The fallback isolates
    the failing targets with one invocation each; cargo's incremental cache
    makes recompiling the targets that already succeeded cheap.
    """
    print(f"--- Building {len(targets)} bench targets in {cwd}")
    if not targets:
        # An unrestricted `cargo bench --no-run` would build every bench
        # target in the whole workspace instead of nothing.
        return [], []
    try:
        output = spawn.capture(cargo_build_args(targets), cwd=cwd, env=env)
        return bench_executables(output.splitlines(), manifests), []
    except subprocess.CalledProcessError:
        pass

    built: list[BuiltBench] = []
    failures: list[TargetFailure] = []
    for target in targets:
        try:
            output = spawn.capture(cargo_build_args([target]), cwd=cwd, env=env)
            built.extend(bench_executables(output.splitlines(), manifests))
        except subprocess.CalledProcessError as e:
            print(
                f"^^^ +++ {target.package}/{target.name} failed to build with {e.returncode}"
            )
            failures.append(TargetFailure(target, e.returncode))
    return built, failures


def run_bench(
    built: BuiltBench,
    criterion_args: list[str],
    env: dict[str, str],
    target_home: Path,
    label: str,
) -> int | None:
    """Run one compiled bench binary directly and return its exit code on failure, else `None`.

    The binary is invoked directly rather than through `cargo bench` again,
    since cargo has already compiled it and re-running cargo would recheck
    the whole workspace for no benefit. `--bench` is required: a criterion
    binary invoked without it runs in test mode and measures nothing. Cargo
    itself runs bench binaries with the package directory as cwd, which is
    also what `CARGO_MANIFEST_DIR` would otherwise expand to at build time,
    so both are set here for a binary that inspects either at runtime.
    """
    print(f"--- Benchmarking {built.package}/{built.name} ({label})")
    target_env = dict(
        env,
        CARGO_MANIFEST_DIR=str(built.manifest_dir),
        CRITERION_HOME=str(target_home),
    )
    try:
        spawn.runv(
            [str(built.executable), "--bench", *criterion_args],
            cwd=built.manifest_dir,
            env=target_env,
        )
        return None
    except subprocess.CalledProcessError as e:
        print(f"^^^ +++ {built.package}/{built.name} failed with {e.returncode}")
        return e.returncode


def run_benches(
    head_built: list[BuiltBench],
    ancestor_built: list[BuiltBench],
    head_targets: list[BenchTarget],
    ancestor_targets: list[BenchTarget],
    env: dict[str, str],
    criterion_home: Path,
) -> tuple[list[TargetFailure], list[TargetFailure]]:
    """Run every HEAD bench binary, its ancestor counterpart first when one was built for the same target.

    Interleaving ancestor and HEAD per target, rather than running every
    ancestor target and then every HEAD target, keeps the machine's page
    cache and thermal state close between the two runs of a benchmark, which
    is what produced repeatable false regressions in the I/O-bound
    mz-ore/pager benches when the two phases ran far apart. A target present
    only in the ancestor is never run, since there is no HEAD result to
    compare it against.
    """
    ancestor_by_key = {(b.package, b.name): b for b in ancestor_built}
    ancestor_target_by_key = {(t.package, t.name): t for t in ancestor_targets}
    head_target_by_key = {(t.package, t.name): t for t in head_targets}

    ancestor_failures: list[TargetFailure] = []
    current_failures: list[TargetFailure] = []
    for head_bin in sorted(head_built, key=lambda b: (b.package, b.name)):
        key = (head_bin.package, head_bin.name)
        # Benchmark ids are only unique within a target, so a collision
        # across targets sharing one criterion home would silently merge two
        # unrelated benchmarks.
        target_home = criterion_home / head_bin.package / head_bin.name
        ancestor_bin = ancestor_by_key.get(key)
        if ancestor_bin is not None:
            if ancestor_bin.executable == head_bin.executable:
                raise RuntimeError(
                    f"{head_bin.package}/{head_bin.name}: ancestor and current bench "
                    f"binaries resolve to the same file {head_bin.executable}"
                )
            rc = run_bench(
                ancestor_bin,
                ["--save-baseline", BASELINE],
                env,
                target_home,
                "ancestor",
            )
            if rc is not None:
                ancestor_failures.append(TargetFailure(ancestor_target_by_key[key], rc))
            # Criterion's --save-baseline leaves a `new/` copy of the
            # ancestor run behind in addition to the baseline it saves. An id
            # absent at HEAD would otherwise keep that copy and get reported
            # as a HEAD result carrying the ancestor's numbers. Criterion
            # recreates `new/` on the HEAD run and only reads
            # `ancestor/estimates.json` and `ancestor/sample.json` for
            # comparison, so removing it here is safe.
            for d in target_home.rglob("new"):
                if d.is_dir():
                    shutil.rmtree(d)
        rc = run_bench(
            head_bin, ["--baseline-lenient", BASELINE], env, target_home, "current"
        )
        if rc is not None:
            current_failures.append(TargetFailure(head_target_by_key[key], rc))
    return ancestor_failures, current_failures


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

    head_targets, head_manifests = load_targets(MZ_ROOT, args.package)
    if not head_targets:
        raise RuntimeError("no bench targets found")

    criterion_home = target_dir() / "criterion-compare"
    # Stale baselines from an earlier run would silently become the
    # comparison target, so start from an empty directory every time.
    shutil.rmtree(criterion_home, ignore_errors=True)
    criterion_home.mkdir(parents=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir()))
    # Persist's test storage configs panic under CI when no external Postgres
    # or S3 endpoint is configured. This step measures code, not network
    # storage, so the benches run as they do locally and skip those variants.
    env.pop("CI", None)

    ancestor: str | None = None
    if args.skip_ancestor:
        head_built, current_build_failures = build_benches(
            MZ_ROOT, head_targets, head_manifests, env
        )
        ancestor_run_failures, current_run_failures = run_benches(
            head_built, [], head_targets, [], env, criterion_home
        )
        ancestor_failures = ancestor_run_failures
        current_failures = current_build_failures + current_run_failures
    else:
        ancestor = args.ancestor or resolve_ancestor()
        assert ancestor is not None
        print(f"--- Comparing against ancestor {ancestor}")
        worktree = Path(tempfile.mkdtemp(prefix="cargo-bench-ancestor-"))
        added = False
        try:
            # A cancelled or timed-out job destroys the container's /tmp
            # worktree before the `finally` block below runs to remove it,
            # and CI agents reuse their checkout across jobs, so stale
            # registrations would otherwise accumulate in `.git/worktrees`.
            spawn.runv(["git", "worktree", "prune"], cwd=MZ_ROOT)
            # `git worktree add` can fail on a bad ancestor ref, in which
            # case there is no worktree to remove, only the scratch
            # directory to clean up.
            spawn.runv(
                ["git", "worktree", "add", "--detach", str(worktree), ancestor],
                cwd=MZ_ROOT,
            )
            added = True
            # Cargo hashes workspace members by their path relative to the
            # workspace root, so the ancestor worktree and the HEAD checkout,
            # being two copies of the same workspace at different paths,
            # would alias each other's units and fingerprints in a shared
            # target dir. That is what let a build script binary compiled for
            # the ancestor worktree's manifest directory get reused when
            # building HEAD, and it failed once the ancestor worktree was
            # removed. Building the ancestor into its own target dir avoids
            # the aliasing.
            ancestor_env = dict(
                env, CARGO_TARGET_DIR=str(target_dir() / "cargo-bench-ancestor")
            )
            ancestor_targets, ancestor_manifests = load_targets(
                worktree, args.package, ancestor_env
            )
            # Building ancestor before HEAD lets both build phases run at
            # full core parallelism back to back, then the run phase below
            # benchmarks matching targets ancestor then HEAD while the
            # worktree the ancestor binaries were compiled against is still
            # present. Bench binaries read fixtures relative to their
            # manifest dir, so the worktree must outlive the run phase.
            ancestor_built, ancestor_build_failures = build_benches(
                worktree, ancestor_targets, ancestor_manifests, ancestor_env
            )
            head_built, current_build_failures = build_benches(
                MZ_ROOT, head_targets, head_manifests, env
            )
            ancestor_run_failures, current_run_failures = run_benches(
                head_built,
                ancestor_built,
                head_targets,
                ancestor_targets,
                env,
                criterion_home,
            )
            ancestor_failures = ancestor_build_failures + ancestor_run_failures
            current_failures = current_build_failures + current_run_failures
        finally:
            if added:
                try:
                    spawn.runv(
                        ["git", "worktree", "remove", "--force", str(worktree)],
                        cwd=MZ_ROOT,
                    )
                except subprocess.CalledProcessError as e:
                    # A failing remove must not mask an in-flight exception
                    # from the try block above.
                    print(f"^^^ +++ removing worktree {worktree} failed: {e}")
            shutil.rmtree(worktree, ignore_errors=True)

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
