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
# check-helm-docs.sh - Make sure helm docs are up to date

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if ! helm-docs --version >/dev/null 2>/dev/null; then
  echo "lint: helm-docs is not installed"
  echo "hint: refer to https://github.com/norwoodj/helm-docs?tab=readme-ov-file#installation for install instructions"
fi

ci_collapsed_heading "Verify that helm docs are up-to-date"
HELM_CHARTS_DIR=misc/helm-charts/operator
TEMP_DIR=$(mktemp -d)
cp -a $HELM_CHARTS_DIR/. "$TEMP_DIR"
helm-docs -c "$TEMP_DIR"
try diff -r $HELM_CHARTS_DIR "$TEMP_DIR"
rm -rf "$TEMP_DIR"

try_status_report
