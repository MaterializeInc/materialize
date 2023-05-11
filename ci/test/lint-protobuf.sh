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
# lint-protobuf.sh - lint the protobuf compatibility.

set -euo pipefail

. misc/shlib/shlib.bash
. misc/buildkite/git.bash

fetch_pr_target_branch

ci_collapsed_heading "Lint protobuf"
ci_try buf breaking src --against ".git#branch=$BUILDKITE_PULL_REQUEST_BASE_BRANCH,subdir=src"

ci_status_report
