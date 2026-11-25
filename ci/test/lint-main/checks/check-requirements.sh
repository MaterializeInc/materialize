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
# check-requirements.sh — check that every requirements.txt is compiled.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

# Renovate recompiles each requirements.txt with the command in its header, and
# ci/lint-malicious-deps.sh relies on it pinning every transitive dependency.
status=0
while IFS= read -r f; do
    in=$(dirname "$f")/requirements.in
    if [[ ! -f "$in" ]] || ! sed -n 2p "$f" | grep -qxE '#    uv pip compile .* requirements\.in --output-file=requirements\.txt'; then
        echo "lint: $f must be compiled from $in with uv pip compile" >&2
        status=1
    fi
done < <(git_files requirements.txt '*/requirements.txt')
exit $status
