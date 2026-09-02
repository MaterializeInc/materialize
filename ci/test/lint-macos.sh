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
# lint-macos.sh — clippy on the manually installed macOS agent.

set -euo pipefail

RESULT=0
cargo clippy --all-targets -- -D warnings || RESULT=$?

# The macOS agent is managed by hand and reclaims no disk on its own, so drop an
# oversized target directory here. This has to run on the failure path too: a
# failed clippy leaves the same grown target behind, and that is exactly when
# the next run is most likely to run out of disk.
if [ -d target ] && [ "$(du -sg target | cut -f1)" -gt 50 ]; then
    rm -rf target
fi

exit $RESULT
