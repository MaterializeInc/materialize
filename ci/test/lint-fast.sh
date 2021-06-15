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
# lint-fast.sh — fast linters that don't require building any code.

set -euo pipefail

. misc/shlib/shlib.bash

ci_init

ci_try bin/lint
ci_try cargo --locked fmt -- --check
ci_try cargo --locked deny check licenses bans sources

ci_status_report
