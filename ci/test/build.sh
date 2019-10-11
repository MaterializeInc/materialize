#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# build.host.sh — packages test binaries into Docker images in CI.

set -euo pipefail

. misc/shlib/shlib.bash

docker_run() {
    # NOTE(benesch): we need to explicitly specify OPENSSL_LIB_DIR and
    # OPENSSL_INCLUDE_DIR to work around a bug in pkg-config-rs:
    # https://github.com/rust-lang/pkg-config-rs/pull/37.
    docker run \
        --rm --interactive --tty \
        --volume "$SSH_AUTH_SOCK:/tmp/ssh-agent.sock" \
        --volume "$HOME/.cargo:/cargo" \
        --volume "$(pwd):/workdir" \
        --env BUILDKITE \
        --env SSH_AUTH_SOCK=/tmp/ssh-agent.sock \
        --env CARGO_HOME=/cargo \
        --env OPENSSL_STATIC=1 \
        --env OPENSSL_LIB_DIR=/usr/lib/x86_64-linux-gnu \
        --env OPENSSL_INCLUDE_DIR=/usr/include \
        --user "$(id -u):$(id -g)" \
        materialize/ci-builder:1.38.0-20190926-131010 bash -c "$1"
}

ci_init

ci_collapsed_heading "Building standalone binaries"
docker_run "cargo build --release"

# NOTE(benesch): The two invocations of `cargo test --no-run` here deserve some
# explanation. The first invocation prints error messages to stdout in a human
# readable form. If that succeeds, the second invocation instructs Cargo to dump
# the locations of the test binaries it built in a machine readable form.
# Without the first invocation, the error messages would also be sent to the
# output file in JSON, and the user would only see a vague "could not compile
# <package>" error. Note that the `cargo build` above doesn't guarantee that the
# tests will build, since errors may be present in test code but not in release
# code.
ci_collapsed_heading "Building test binaries"
docker_run "cargo test --no-run && cargo test --no-run --message-format=json > test-binaries.json"

ci_collapsed_heading "Preparing Docker context"
{
    cp target/release/materialized misc/docker/ci-raw-materialized

    # NOTE(benesch): the debug information is large enough that it slows down CI,
    # since we're packaging these binaries up into Docker images and shipping them
    # around. A bit unfortunate, since it'd be nice to have useful backtraces if
    # something crashes.
    docker_run "find target -maxdepth 2 -type f -executable | xargs strip"

    # Move test binaries into the context for the ci-cargo-test image.
    #
    # To make things a bit more readable, we name the test binaries after the
    # crate and target kind they come from, rather than the random hash that
    # Cargo uses. For example, we rename sqllogictest-91e9c20ba29cec69 to
    # sqllogictest.lib or sqllogictest.bin.sqllogictest, depending upon whether
    # the binary is testing the library or the binary of the same name.
    #
    # We also write out a manifest that indicates the directory in which each
    # test binary is to be run. `cargo test` executes test binaries in the
    # root of the crate they come from, and we need to emulate this behavior
    # since some of the test binaries attempt to find test data relative to
    # their CWD.
    mkdir -p misc/docker/ci-cargo-test/{tests,shlib}
    while read -r executable cwd slug; do
        echo "$slug $cwd" >> misc/docker/ci-cargo-test/tests/manifest
        mv "$executable" misc/docker/ci-cargo-test/tests/"$slug"
    done < <(jq -r 'select(.profile.test)
        | . * {
            "crate_name": (.package_id | split(" ") | .[0]),
            "crate_path": (.package_id | match("\\(path\\+file://(.*)\\)"; "g") | .captures[0].string),
            "target": { "kind": .target.kind | join("") },
        }
        | {
            "executable": .executable | sub("^/workdir/"; ""),
            "cwd": .crate_path | sub("^/workdir/"; ""),
            "slug": (.crate_name
                    + ".\(.target.kind)"
                    + if (.target.kind != "lib") then ".\(.target.name)" else "" end)
        }
        | "\(.executable) \(.cwd) \(.slug)"' test-binaries.json)
    mkdir misc/docker/ci-cargo-test/tests/examples
    cp target/debug/examples/pingpong misc/docker/ci-cargo-test/tests/examples
    cp target/release/testdrive misc/docker/ci-cargo-test/tests
    cp misc/shlib/* misc/docker/ci-cargo-test/shlib

    # Move standalone binaries into the contexts for their respective images.
    mv target/release/materialized misc/docker/ci-materialized
    mv target/release/testdrive misc/docker/ci-testdrive
    mv target/release/sqllogictest misc/docker/ci-sqllogictest
    mv target/release/metrics misc/docker/ci-metrics
}

images=(
    materialized
    testdrive
    sqllogictest
    metrics
    cargo-test
)

if [[ "$BUILDKITE_BRANCH" = master ]]; then
    images+=(raw-materialized)
fi

for image in "${images[@]}"; do
    ci_collapsed_heading "Building Docker image $image"
    tag=materialize/ci-$image:${BUILDKITE_BUILD_NUMBER}
    docker build \
        --tag "$tag" \
        --pull \
        "misc/docker/ci-$image"
    docker push "$tag"
done
