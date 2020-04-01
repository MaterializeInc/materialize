#!/usr/bin/env bash

set -euo pipefail

REPO=materialize/mzcli
NOW=$(date +%Y-%m-%d)

usage() {
    echo "usage: $0 [-p|-n] [--push|--no-push|--only-push]"
    exit 0
}

main() {
    local push=${1:-__prompt}
    [[ $push != __prompt ]] && shift
    local do_build=Y
    local do_push
    case $push in
        -p|--push) do_push=Y ;;
        -n|--no-push) do_push=N ;;
        --only-push)
            do_build=N
            do_push=Y
            ;;
        -h|--help) usage ;;
        *) read -r -p "Run docker push [y/N] " do_push
    esac

    if [[ $do_build == Y ]]; then
        build
    fi
    if [[ ${do_push^^} =~ Y.* ]] ; then
        push
    else
        echo "Skipping docker push"
    fi
    echo "finished"
}

build() {
    run docker build -t "${REPO}:latest" -t "${REPO}:local" -t "${REPO}:${NOW}" .
}

push() {
    for tag in latest "$NOW"; do
        run docker push "${REPO}:${tag}"
    done
}

run() {
    echo "$ $*"
    "$@"
}

main "$@"
