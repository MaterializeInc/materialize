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
# lint-slow.sh — slow linters that require building code.

set -euo pipefail

. misc/shlib/shlib.bash

try cargo clippy --all-targets -- -D warnings

try bin/doc
try bin/doc --document-private-items

try bin/xcompile cargo test --locked --doc

try bin/ci-closed-issues-detect --changed-lines-only

try_status_report
