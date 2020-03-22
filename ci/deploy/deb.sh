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

the_deb="s3://downloads.mtrlz.dev/materialized-$MATERIALIZED_IMAGE_ID-x86_64.deb"
echo "Downloading deb from s3: $the_deb"
aws s3 cp \
    "$the_deb" \
    ./materialized.deb

curl -F package=@materialized.deb https://"$FURY_APT_PUSH_SECRET"@push.fury.io/materialize

# Publish version that was already uploaded to Bintray
# in ci/test/build.sh
COMMIT_INDEX=$(git rev-list HEAD | wc -l)
COMMIT_HASH=$(git rev-parse HEAD)
upload="https://api.bintray.com/content/materialize/apt/materialized-unstable/dev-$COMMIT_INDEX-$COMMIT_HASH/publish"
echo "Marking release public in bintray: $upload"

curl -f -X POST -u ci@materialize:"$BINTRAY_API_KEY" "$upload"
