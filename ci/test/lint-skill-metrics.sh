#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Checks that every `mz_*` metric named by the mz-release-signoff skill still
# resolves against the generated metrics catalog, so the skill's references
# cannot rot silently when a metric is renamed. A reference that names a metric
# the product no longer exports is worse than no reference, because a release
# verifier reads the resulting empty query as a healthy zero.
#
# Names the catalog structurally cannot see are exempted one at a time in
# .agents/skills/mz-release-signoff/scripts/metrics-allowlist.txt, with a reason.
#
# Example usages:
#
#     $ ci/test/lint-skill-metrics.sh

set -euo pipefail

. misc/shlib/shlib.bash

ci_uncollapsed_heading "Linting mz-release-signoff metric names"

try python3 .agents/skills/mz-release-signoff/scripts/lint_metrics.py

try_status_report
