#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# build.sh — packages test binaries into Docker images in CI.

set -euo pipefail

. misc/shlib/shlib.bash

docker_run() {
    docker run \
        --rm --interactive --tty \
        --volume "$SSH_AUTH_SOCK:/tmp/ssh-agent.sock" \
        --volume "$HOME/.cargo:/cargo" \
        --volume "$(pwd):/workdir" \
        --env BUILDKITE \
        --env SSH_AUTH_SOCK=/tmp/ssh-agent.sock \
        --env CARGO_HOME=/cargo \
        --user "$(id -u):$(id -g)" \
        materialize/ci-builder:1.40.0-20200130-101425 bash -c "$1"
}

ci_init

ci_collapsed_heading "Building standalone binaries"
docker_run "cargo build --locked --release"

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
docker_run "cargo test --locked --no-run && cargo test --locked --no-run --message-format=json > test-binaries.json"

ci_collapsed_heading "Preparing Docker context"
{
    cp target/release/materialized misc/docker/ci-raw-materialized
    cp target/release/materialized misc/docker/ci-materialized

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
    mv target/release/peeker misc/docker/ci-peeker
    mv target/release/billing-demo misc/docker/ci-billing-demo
}

if [[ "$BUILDKITE_BRANCH" = master ]]; then
    ci_collapsed_heading "Building .deb package"
    # shellcheck disable=SC2016
    docker_run 'cargo-deb --deb-version 0.1.0-$(git rev-list HEAD | wc -l)-$(git rev-parse HEAD) -p materialized -o target/debian/materialized.deb'
    aws s3 cp \
        --acl=public-read \
        target/debian/materialized.deb \
        s3://downloads.mtrlz.dev/materialized-"$BUILDKITE_BUILD_NUMBER"-x86_64.deb
fi

images=(
    materialized
    testdrive
    sqllogictest
    peeker
    billing-demo
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
