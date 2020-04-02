#!/usr/bin/env bash
# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# deb.sh — deploys official Debian packages in CI.

set -euo pipefail

. misc/shlib/shlib.bash

# Publish version that was already uploaded to Bintray
# in ci/test/build.sh
commit_index=$(git rev-list HEAD | wc -l)
commit_hash=$(git rev-parse HEAD)
upload="https://api.bintray.com/content/materialize/apt/materialized-unstable/dev-$commit_index-$commit_hash/publish"
echo "Marking release public in bintray: $upload"

curl -f -X POST -u ci@materialize:"$BINTRAY_API_KEY" "$upload"
