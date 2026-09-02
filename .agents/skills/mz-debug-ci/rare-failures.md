# Rare failure classes for mz-debug-ci

Triage patterns for failure classes that come up rarely. SKILL.md's Step 4
points here by keyword.

## Feature benchmark regressions

A `New regression against <version>` annotation shows one comparison table,
but the harness re-runs every flagged scenario and annotates only the last
run: grep the console log for `Run [0-9] with scenarios` and `!!YES!!` to
see whether the scenario survived every re-run. Even surviving all re-runs
does not prove a code cause, since agent-level speed offsets are stable
within a job, and these scenarios trip on noise regularly, so check the
base rate with `bin/ci-failures '<ScenarioName>'`. The decisive check is
reproducing locally: check out the exact commit the build ran and run the
reproducer command from the annotation. mzbuild pulls the images CI already
published for that commit, so nothing needs to build. Pass `--release` for
nightly and release-qualification builds (they build with `CI_LTO=1`); PR
test builds and flag-less local runs both use the optimized profile, so
no flag there.

## cargo-fuzz crashes

The fuzz target's own panic text is not in the console log; it is in
`<target>.log` inside the `fuzz-logs.tar.zst` build artifact, and the
annotation names the file. For known-vs-new use
`bin/ci-failures '<target name>'`; cargo-fuzz runs only in release
qualification, so that is its entire history.

## Limits test failures

For red `Product limits` / `Instance size limits` jobs, the mz-limits-test
skill explains the framework, its typical failure modes, and
single-scenario local reruns.

## Miri failures

`:rust: Miri test (full)` fails as either `timed_out` (a test too slow under
Miri's interpreter) or `failed` with `unsupported operation` (code Miri
cannot execute, e.g. a foreign function). Neither is a product bug, and the
standard reaction is not to make the code Miri-compatible: add
`#[cfg_attr(miri, ignore)]` with a short reason to the offending test, as
about a thousand existing tests do.

## `ImagesNotPublicError`

A PR adding a new publishable mzbuild image fails `:rust: Build x86_64`
until the image's DockerHub/GHCR repos are made public, a process step
described by the `images-not-public` annotation. The check runs only on the
x86_64 build, so aarch64 passing while x86_64 fails is expected, not an
arch bug.
