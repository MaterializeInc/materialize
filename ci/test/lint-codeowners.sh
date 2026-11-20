#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

auth=()
if [[ "${GITHUB_TOKEN:-}" ]]; then
    auth=(-H "Authorization: Bearer $GITHUB_TOKEN")
fi

errors=$(curl -fsSL "${auth[@]}" \
    "https://api.github.com/repos/MaterializeInc/materialize/codeowners/errors?ref=${BUILDKITE_COMMIT:-main}" \
    | jq -r '.errors[].message')

if [[ "$errors" ]]; then
    echo "$errors"
    exit 1
fi
