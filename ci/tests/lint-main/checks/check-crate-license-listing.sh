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
# check-crate-license-listing.sh — check absence of failures in generating the license metadata page.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

# Smoke out failures in generating the license metadata page, even though we
# don't care about its output in the test pipeline, so that we don't only
# discover the failures after a merge to main.
try cargo --locked about generate ci/deploy/licenses.hbs > /dev/null

try_status_report
