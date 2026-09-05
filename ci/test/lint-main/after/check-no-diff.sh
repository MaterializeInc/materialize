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
# Fails if the lint checks modified the working tree, which usually means a
# generated file is stale. Changes that existed before the checks ran, as
# snapshotted by before/save-diff-state.sh, do not count: a dirty tree, such
# as the working copy of a colocated jj repo, must be able to pass the lint.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

check_no_new_diff() {
    local before=target/lint/diff-before.patch
    if [[ ! -f "$before" ]]; then
        # No snapshot, so require a fully clean tree.
        git diff --compact-summary --exit-code
        return
    fi
    if ! git diff | cmp -s "$before" -; then
        echo "The lint checks changed the working tree (< before, > after):"
        diff target/lint/diff-before.summary <(git diff --compact-summary) || true
        return 1
    fi
}

try check_no_new_diff

try_status_report
