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

ci_try bin/xcompile cargo test --locked --doc
ci_try cargo clippy --all-targets -- -D warnings

ci_try bin/doc
ci_try bin/doc --document-private-items

ci_status_report
