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

docker pull "materialize/ci-raw-materialized:$MATERIALIZED_IMAGE_ID"

version=${BUILDKITE_TAG:-$BUILDKITE_COMMIT}

if [[ $BUILDKITE_TAG ]]; then
    tags=("$BUILDKITE_TAG")
else
    tags=("unstable-$BUILDKITE_COMMIT" latest)
fi

for tag in "${tags[@]}"; do
    echo "Processing docker tag $tag"
    runv docker tag "materialize/ci-raw-materialized:$MATERIALIZED_IMAGE_ID" "materialize/materialized:$tag"
    runv docker push "materialize/materialized:$tag"
done

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

aws s3 cp \
    --acl=public-read \
    "materialized.tar.gz" \
    "s3://downloads.mtrlz.dev/materialized-$version-x86_64-unknown-linux-gnu.tar.gz"

if [[ -z $BUILDKITE_TAG ]]; then
    echo -n > empty
    aws s3 cp \
        --website-redirect="/materialized-$version-x86_64-unknown-linux-gnu.tar.gz" \
        --acl=public-read \
        empty \
        "s3://downloads.mtrlz.dev/materialized-latest-x86_64-unknown-linux-gnu.tar.gz"
fi

docker pull "materialize/ci-peeker:$MATERIALIZED_IMAGE_ID"

for tag in "${tags[@]}"; do
    echo "Processing docker tag for peeker: $tag"
    runv docker tag "materialize/ci-peeker:$MATERIALIZED_IMAGE_ID" "materialize/peeker:$tag"
    runv docker push "materialize/peeker:$tag"
done

docker pull "materialize/ci-billing-demo:$MATERIALIZED_IMAGE_ID"

for tag in "${tags[@]}"; do
    echo "Processing docker tag for billing-demo: $tag"
    runv docker tag "materialize/ci-billing-demo:$MATERIALIZED_IMAGE_ID" "materialize/billing-demo:$tag"
    runv docker push "materialize/billing-demo:$tag"
done
