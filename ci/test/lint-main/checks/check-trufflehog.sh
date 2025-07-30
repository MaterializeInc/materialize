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
# check-trufflehog.sh - Scan repository for secrets

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if ! trufflehog --version >/dev/null 2>/dev/null; then
  echo "lint: trufflehog is not installed"
  echo "hint: refer to https://github.com/trufflesecurity/trufflehog?tab=readme-ov-file#floppy_disk-installation for install instructions"
  exit 1
fi

git ls-files | grep -Ev '^(misc/shlib/shlib\.bash|test/lang/js/yarn\.lock)$' | xargs trufflehog --no-fail --no-update --no-verification --json filesystem | trufflehog_jq_filter_files > trufflehog.log

try test ! -s trufflehog.log

if try_last_failed; then
    printf "%s\n" "lint: $(red error:) new secrets found"
    printf "%s\n" "lint: $(green hint:) don't check in secrets and revoke them immediately"
    printf "%s\n" "lint: $(green hint:) mark false positives in misc/shlib/shlib.bash's trufflehog_jq_filter_(files|common)"
fi

jq -c -r '. | "\(.SourceMetadata.Data.Filesystem.file):\(.SourceMetadata.Data.Filesystem.line): Secret found: \(.Raw)"' trufflehog.log

rm -f trufflehog.log

try_status_report
