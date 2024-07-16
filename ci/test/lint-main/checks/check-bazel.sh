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
# check-cargo.sh — check for Bazel issues (e.g., ensure all of our generated files are up to date).

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

try bin/bazel gen

# Make sure we didn't generate any changes.
try git diff --compact-summary --exit-code

try_status_report
