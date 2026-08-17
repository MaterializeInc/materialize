#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# tla-holds.sh: model check the two-runtime read-hold protocol in
# `doc/developer/design/20260720_two_runtime_compute/protocol-holds`.
#
# Builds a TLC image (headless JRE plus a digest-pinned `tla2tools.jar`)
# and runs every `*.cfg` beside a spec of the same name. TLC exits
# non-zero on an invariant violation and prints the counterexample
# trace, so a green run means every configured invariant held across the
# whole state space TLC explored.
#
# What a green run does NOT mean: the model is parameterised by process
# and timestamp counts, so it is exhaustive only for the sizes each
# `.cfg` names. Raising them is how you buy more confidence.
#
# Pass arguments to run something else in the image, for example a
# single spec with a larger heap:
#   ci/test/tla-holds.sh tlc2.TLC -workers 4 -config Holds.cfg Holds.tla

set -euo pipefail

cd "$(dirname "$0")/../.."

spec_dir="doc/developer/design/20260720_two_runtime_compute/protocol-holds"
# Pinned together: the URL and the digest of what it must serve.
tla_version="v1.8.0"
tla_sha256="ab323b79802aedc3203b3f9af37c6aca3ed43f4e0225b36f2aa77b26de46c05f"
image_tag="mz-tla-holds:latest"

docker build \
    --build-arg "TLA_VERSION=$tla_version" \
    --build-arg "TLA_SHA256=$tla_sha256" \
    --tag "$image_tag" \
    "$spec_dir"

run_in_image() {
    docker run --rm \
        --user "$(id -u):$(id -g)" \
        -v "$PWD/$spec_dir:/spec" \
        "$image_tag" \
        "$@"
}

if [[ $# -gt 0 ]]; then
    run_in_image "$@"
    exit 0
fi

# Explicit manifest rather than globbing, because half of these are supposed to
# fail. A model that can only express the proposed design cannot tell you the
# design fixed anything, so the refutations are checked too: if one of them
# starts passing, the model has stopped discriminating and a green run of the
# others means nothing.
#
# Format: <config> <spec> <expect: hold|refute>
checks=(
    "Holds.cfg Holds.tla hold"
    "HoldsBroadcast.cfg Holds.tla refute"
    "HoldsRouted.cfg Holds.tla refute"
)

status=0
for check in "${checks[@]}"; do
    read -r config spec expect <<< "$check"
    echo "--- $config against $spec, expecting to $expect"
    # `-deadlock` because these specs model queues that legitimately drain to a
    # state with nothing left to do. Deadlock is not a bug here; liveness is
    # stated as explicit properties instead.
    if run_in_image tlc2.TLC -deadlock -workers auto -config "$config" "$spec"; then
        result=hold
    else
        result=refute
    fi
    if [[ "$result" != "$expect" ]]; then
        echo "!!! $config: expected to $expect, but it did $result" >&2
        status=1
    fi
done

exit $status
