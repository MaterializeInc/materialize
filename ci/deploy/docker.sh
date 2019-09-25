#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# docker.sh — deploys official Docker images in CI.

set -euo pipefail

source misc/shlib/shlib.bash

docker pull "materialize/ci-raw-materialized:$MATERIALIZED_IMAGE_ID"

for tag in "unstable-$BUILDKITE_COMMIT" latest; do
    echo "Processing docker tag $tag"
    run docker tag "materialize/ci-raw-materialized:$MATERIALIZED_IMAGE_ID" "materialize/materialized:$tag"
    run docker push "materialize/materialized:$tag"
done

docker pull "materialize/ci-metrics:$MATERIALIZED_IMAGE_ID"

for tag in "unstable-$BUILDKITE_COMMIT" latest; do
    echo "Processing docker tag for metrics: $tag"
    run docker tag "materialize/ci-metrics:$MATERIALIZED_IMAGE_ID" "materialize/metrics:$tag"
    run docker push "materialize/metrics:$tag"
done
