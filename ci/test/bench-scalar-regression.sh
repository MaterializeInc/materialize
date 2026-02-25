#!/usr/bin/env bash

# Scalar function benchmark regression test.
#
# Runs criterion benchmarks on upstream/main and the current branch,
# then compares results. Criterion reports per-benchmark changes with
# statistical confidence; exit code is always 0 (regressions are
# informational, not gating) unless a build fails.
#
# Usage:
#   ci/test/bench-scalar-regression.sh [--bench-filter FILTER]
#
# Options:
#   --bench-filter FILTER   Only run benchmarks matching FILTER (passed to
#                           criterion via `-- FILTER`). Default: run all.
#
# The script expects `upstream/main` to be a valid ref (fetch it first
# with `git fetch upstream`).

set -euo pipefail

BENCH_FILTER=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bench-filter)
            BENCH_FILTER="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

BENCH_TARGETS=(
    --bench bench_unary
    --bench bench_binary
    --bench bench_variadic
    --bench bench_array
)

CRITERION_ARGS=()
if [[ -n "$BENCH_FILTER" ]]; then
    CRITERION_ARGS+=("$BENCH_FILTER")
fi

PR_BRANCH=$(git rev-parse --abbrev-ref HEAD)
MAIN_REF="upstream/main"

echo "--- Benchmark regression test"
echo "PR branch:  $PR_BRANCH"
echo "Base ref:   $MAIN_REF"

# Stash any uncommitted changes so checkout is clean.
STASHED=false
if ! git diff --quiet || ! git diff --cached --quiet; then
    git stash push -m "bench-scalar-regression: auto-stash"
    STASHED=true
fi

cleanup() {
    echo "--- Returning to $PR_BRANCH"
    git checkout "$PR_BRANCH"
    if $STASHED; then
        git stash pop
    fi
}
trap cleanup EXIT

# ---- Phase 1: baseline on main ----

echo "--- Building and benchmarking on $MAIN_REF"
git checkout "$MAIN_REF"

cargo bench -p mz-expr "${BENCH_TARGETS[@]}" -- --save-baseline main "${CRITERION_ARGS[@]}"

# ---- Phase 2: PR branch ----

echo "--- Building and benchmarking on $PR_BRANCH"
git checkout "$PR_BRANCH"
if $STASHED; then
    git stash pop
    STASHED=false
fi

cargo bench -p mz-expr "${BENCH_TARGETS[@]}" -- --baseline main "${CRITERION_ARGS[@]}"

echo "--- Benchmark comparison complete"
echo "Criterion reports per-benchmark changes above. Look for 'regressed' lines."
