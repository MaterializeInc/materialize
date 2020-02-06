#!/usr/bin/env bash
# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# build the docker image, and run docker compose with a locally-built docker image

set -euo pipefail

EXEC_DIR="${0%/*}"
REPO=materialize/


# shellcheck source=SCRIPTDIR/../../misc/shlib/shlib.bash
source "$EXEC_DIR/../../misc/shlib/shlib.bash"

REPO=materialize/http-demo
TODAY="$(date +%Y-%m-%d)"
BUILD=n
RUN=n
PUSH=n

usage() {
    # backticks are more readable here
    # shellcheck disable=SC2006
    echo "$0 [build] [run] [push]

Build the demo docker image in ./apps.

Updates to apps must be pushed, and the default tag must be set to today ($TODAY) in
docker-compose.yml if you would like people to use it.

Possible commands (all can be specified simultaneously):

    `us build`     build the the $REPO docker image from ./apps
    `us run`       run docker compose, using the most recent 'local' image
                    Note that this will fail if you have never run 'build',
                    use 'docker-compose up' to run normally
    `us push`      push the today ($TODAY) tag"

    exit 1
}

main() {
    parse_args "$@"
    cd "$EXEC_DIR"
    if [[ $BUILD = y ]]; then
        (
            cd apps
            runv docker build -t "$REPO:$TODAY" -t $REPO:local .
        )
    fi
    if [[ $RUN = y ]]; then
        MZ_HTTP_DEMO_VERSION=local runv docker-compose up
    fi
    if [[ $PUSH = y ]]; then
        runv docker push "$REPO:$TODAY"
    fi
}

parse_args() {
    local arg
    while [[ $# -gt 0 ]]; do
        arg=$1 && shift
        case "$arg" in
            push) PUSH=y ;;
            build) BUILD=y ;;
            run) RUN=y ;;
            *) usage ;;
        esac
    done
    if [[ $BUILD = n && $RUN = n && $PUSH = n ]]; then
        die "At least one of 'build', 'push', or 'run' are required"
    fi
}

main "$@"
