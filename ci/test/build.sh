#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# build.host.sh — packages test binaries into Docker images in CI.

set -euo pipefail

. misc/shlib/shlib.bash

# If you add a new binary, add it to this array to have a Docker image named
# materialize/test-BINARY created. You will also need to create a
# Dockerfile.BINARY file that specifies how to build the docker image.
binaries=(
    materialized
    sqllogictest
    testdrive
)

docker_run() {
    docker run \
        --rm --interactive --tty \
        --volume "$SSH_AUTH_SOCK:/tmp/ssh-agent.sock" \
        --volume "$HOME/.cargo:/cargo" \
        --volume "$(pwd):/workdir" \
        --env BUILDKITE \
        --env SSH_AUTH_SOCK=/tmp/ssh-agent.sock \
        --env SCCACHE_MEMCACHED=tcp://buildcache.internal.mtrlz.dev:11211 \
        --env RUSTC_WRAPPER=sccache \
        --env CARGO_HOME=/cargo \
        --user "$(id -u):$(id -g)" \
        materialize/test:20190727-014532 bash -c "$1"
}

ci_init

ci_collapsed_heading "Building standalone binaries"
docker_run "cargo build --release"

ci_collapsed_heading "Building test binaries"
docker_run "cargo test --no-run --message-format=json > test-binaries.json"

ci_collapsed_heading "Preparing Docker context"
{
    rm -rf docker-context
    mkdir -p docker-context/{tests,shlib}

    # Copy test binaries into docker-context/tests.
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
    while read -r executable cwd slug; do
        echo "$slug $cwd" >> docker-context/tests/manifest
        mv "$executable" docker-context/tests/"$slug"
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

    # Copy standalone binaries into docker-context.
    for binary in "${binaries[@]}"; do
        mv "target/release/$binary" docker-context
    done

    # Special-case testdrive, which gets packaged into the test image because
    # some unit tests depend upon it being available.
    cp docker-context/testdrive docker-context/tests

    # NOTE(benesch): the debug information is large enough that it slows down CI,
    # since we're packaging these binaries up into Docker images and shipping them
    # around. A bit unfortunate, since it'd be nice to have useful backtraces if
    # something crashes.
    docker_run "find docker-context -type f -executable | xargs strip"

    # Copy other miscellaneous scripts into the context.
    cp ci/test/run-tests docker-context/run-tests
    cp misc/shlib/* docker-context/shlib
}

for image in "${binaries[@]}" tests; do
    ci_collapsed_heading "Building Docker image $image"
    tag=materialize/test-$image:${BUILDKITE_BUILD_NUMBER}
    docker build \
        --tag "$tag" \
        --file "ci/test/Dockerfile.$image" \
        --pull --stream \
        docker-context
    docker push "$tag"
done
