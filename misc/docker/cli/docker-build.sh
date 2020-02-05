#!/usr/bin/env bash
# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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
