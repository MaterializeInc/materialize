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
# docker.sh — deploys official Docker images in CI.

set -euo pipefail

. misc/shlib/shlib.bash

version=${BUILDKITE_TAG:-$BUILDKITE_COMMIT}

# Keep archive building in sync with macos.sh.
# TODO(benesch): extract into shared script.
runv docker run --rm --entrypoint bash materialize/materialized -c "
    set -euo pipefail
    mkdir -p scratch/materialized/{bin,etc/materialized}
    cd scratch/materialized
    cp /usr/local/bin/materialized bin
    cd ..
    tar cz materialized
" > materialized.tar.gz

runv aws s3 cp \
    --acl=public-read \
    "materialized.tar.gz" \
    "s3://downloads.mtrlz.dev/materialized-$version-x86_64-unknown-linux-gnu.tar.gz"

if [[ -z $BUILDKITE_TAG ]]; then
    echo -n > empty
    runv aws s3 cp \
        --website-redirect="/materialized-$version-x86_64-unknown-linux-gnu.tar.gz" \
        --acl=public-read \
        empty \
        "s3://downloads.mtrlz.dev/materialized-latest-x86_64-unknown-linux-gnu.tar.gz"
fi
