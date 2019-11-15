#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# mkpipeline.sh — dynamically renders a pipeline.yml for Buildkite.

set -euo pipefail

cd "$(dirname "$0")/../.."

. misc/shlib/shlib.bash

# changes_matching GLOB
#
# Reports whether there were any changes on this branch in a file whose name
# matches GLOB. Note that this function always returns true if there were any
# changes to the build configuration itself.
changes_matching() {
    if [[ "${BUILDKITE_BRANCH:-}" = master ]] ||
        ! git diff --no-patch --quiet origin/master... -- "bin/*" "ci/*" "$@"
    then
        echo true
    else
        echo false
    fi
}

run git fetch origin master

CHANGED_DOC_USER=$(changes_matching "doc/user/*")
CHANGED_RUST=$(changes_matching "src/*" Cargo.lock)
CHANGED_SLT=$(changes_matching "test/*.slt" "test/**/*.slt")
CHANGED_TESTDRIVE=$(changes_matching "test/*.td" "test/**/*.td")
export CHANGED_DOC_USER CHANGED_RUST CHANGED_SLT CHANGED_TESTDRIVE

GIT_PAGER="" run git diff --stat origin/master...
env | grep CHANGED

buildkite-agent pipeline upload ci/test/pipeline.yml
