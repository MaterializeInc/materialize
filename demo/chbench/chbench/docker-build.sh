#!/usr/bin/env bash

set -euo pipefail

EXEC_DIR="${0%/*}"

# shellcheck source=SCRIPTDIR/../../../misc/shlib/shlib.bash
source "$EXEC_DIR/../../../misc/shlib/shlib.bash"

REPO=materialize/chbenchmark
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
