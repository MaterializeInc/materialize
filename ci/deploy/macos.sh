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
# macos.sh — deploys official macOS tarballs in CI.

set -euo pipefail

. misc/shlib/shlib.bash

cargo build --bin materialized --release

# Keep archive building in sync with docker.sh.
# TODO(benesch): extract into shared script.
mkdir -p scratch/materialized/{bin,etc/materialized}
cp target/release/materialized scratch/materialized/bin
tar czf materialized.tar.gz -C scratch materialized

aws s3 cp \
    --acl=public-read \
    "materialized.tar.gz" \
    "s3://downloads.mtrlz.dev/materialized-$BUILDKITE_COMMIT-x86_64-apple-darwin.tar.gz"

echo -n > empty
aws s3 cp \
    --website-redirect="/materialized-$BUILDKITE_COMMIT-x86_64-apple-darwin.tar.gz" \
    --acl=public-read \
    empty \
    "s3://downloads.mtrlz.dev/materialized-latest-x86_64-apple-darwin.tar.gz"
