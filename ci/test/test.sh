#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# test.sh — runs tests and lints in CI.

set -euo pipefail

die() {
    echo "$@" >&2
    exit 1
}

fast=
for arg
do
    case "$arg" in
        --fast) fast=1 ;;
        --fast=*) die "--fast option does not take an argument" ;;
        -*) die "unknown option $arg" ;;
        *) die "usage: $0 [--fast]" ;;
    esac
done

passed=0
total=0

try() {
    # Start a collapsed-by-default log section.
    echo "--- $@"

    # Try the command.
    if "$@"; then
        ((++passed))
    else
        # The command failed. Tell Buildkite to uncollapse this log section, so
        # that the errors are immediately visible.
        [[ "${BUILDKITE-}" ]] && echo "^^^ +++"
    fi
    ((++total))
}

export RUST_BACKTRACE=full

try bin/lint
try cargo fmt -- --check
try cargo test
# Intentionally run check last, since otherwise it won't use the cache.
# https://github.com/rust-lang/rust-clippy/issues/3840
try bin/check

# TODO(benesch): maybe some subset of testdrive tests can be run in fast mode?
if [[ ! "$fast" ]]; then
    try cargo build

    target/debug/materialized &
    materialized_pid=$!
    trap "kill -9 $materialized_pid &> /dev/null" EXIT

    # "Wait" for materialized to start up.
    #
    # TODO(benesch): we need proper synchronization here.
    sleep 0.1

    args=()
    [[ "$KAFKA_ADDR" ]] && args+=("--kafka-addr" "$KAFKA_ADDR")
    [[ "$SCHEMA_REGISTRY_URL" ]] && args+=("--schema-registry-url" "$SCHEMA_REGISTRY_URL")
    try target/debug/testdrive "${args[@]}" test/*.td
fi

echo "+++ Status report"
echo "$passed/$total commands passed"
if ((passed != total)); then
    exit 1
fi
