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
# Compare dependencies and their flags against a ground truth. This serves against accidentally enabling dependencies
# or feature flags.
#
# Example usages:
#
#     $ ci/test/lint-docs-catalog.sh
#
# To rewrite the stored files, pass the --rewrite option.

set -euo pipefail

. misc/shlib/shlib.bash

catalog_files=(
  doc/user/content/sql/system-catalog/mz_internal.md
  doc/user/content/sql/system-catalog/mz_catalog.md
)

slt_directory=test/sqllogictest/autogenerated

rewrite=false

if [[ "${1:-}" = --rewrite ]]; then
    shift
    rewrite=true
    ci_uncollapsed_heading "Linting catalog docs; rewriting .slt files"
else
    ci_uncollapsed_heading "Linting catalog docs, run $0 --rewrite to update .slt files"
fi

for catalog_file in "${catalog_files[@]}"; do
    slt="$slt_directory/$(basename "$catalog_file" .md).slt"
    if $rewrite; then
        ci_try bin/pyactivate ci/test/lint-docs-catalog.py "$catalog_file" > "$slt"
    else
        ci_try diff "$slt" <(bin/pyactivate ci/test/lint-docs-catalog.py "$catalog_file")
    fi
done

ci_status_report
