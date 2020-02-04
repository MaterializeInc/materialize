#!/usr/bin/env bash

set -euo pipefail

REPO=materialize/chbenchmark
PUSH=n

usage() {
    echo "$0 [-p|--push]

Build the docker image, and optionally push the latest tag"
    exit 1
}

main() {
    parse_args "$@"
    cd "${0%/*}"
    docker build -t $REPO:latest -t $REPO:local .
    if [[ $PUSH = y ]]; then
        docker push $REPO:latest
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
