#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# macos.sh — deploys official macOS tarballs in CI.

set -euo pipefail

. misc/shlib/shlib.bash

cargo build --bin materialized --release

# Keep archive building in sync with docker.sh.
# TODO(benesch): extract into shared script.
mkdir -p scratch/materialized/{bin,etc/materialized}
cp target/release/materialized scratch/materialized/bin
cp misc/dist/etc/materialized/bootstrap.sql scratch/materialized/etc/materialized
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
