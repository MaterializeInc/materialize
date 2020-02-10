#!/usr/bin/env bash
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

EXEC_DIR="${0%/*}"

# shellcheck source=SCRIPTDIR/../../shlib/shlib.bash
source "$EXEC_DIR/../../shlib/shlib.bash"

REPO=materialize/cli
PUSH=n

usage() {
    echo "$0 [-p|--push]

Build the docker image, and optionally push the latest tag"
    exit 1
}

main() {
    parse_args "$@"
    cd "$EXEC_DIR"
    runv docker build -t $REPO:latest -t $REPO:local .
    if [[ $PUSH = y ]]; then
        runv docker push $REPO:latest
    fi
}

parse_args() {
    if [[ $# -gt 0 ]]; then
        case "$1" in
            --push|-p) PUSH=y ;;
            *) usage ;;
        esac
    fi
}

main "$@"
