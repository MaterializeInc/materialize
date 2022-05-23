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
# lang.js.sh — run JavaScript language tests.

set -euo pipefail

cd "$(dirname "$0")/../../.."

. misc/shlib/shlib.bash

cd test/lang/js

yarn install --frozen-lockfile

ci_try yarn run fmt-check
ci_try yarn test

ci_status_report
