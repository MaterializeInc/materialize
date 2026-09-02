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

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash
. misc/buildkite/git.bash

if [[ "${MZ_LINT_OFFLINE:-}" != 1 ]]; then
  fetch_pr_target_branch
fi
