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
# check-cargo.sh — check for cargo issues (e.g., ensure that all crates use the same rust version).

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

try bin/lint-cargo

try cargo --locked fmt -- --check
try cargo --locked deny check licenses bans sources
try cargo hakari generate --diff
try cargo hakari manage-deps --dry-run
try cargo deplint Cargo.lock ci/test/lint-deps.toml

try_status_report
