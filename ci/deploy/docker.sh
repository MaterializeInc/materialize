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
    runv docker tag "materialize/ci-raw-materialized:$MATERIALIZED_IMAGE_ID" "materialize/materialized:$tag"
    runv docker push "materialize/materialized:$tag"
done

docker pull "materialize/ci-peeker:$MATERIALIZED_IMAGE_ID"

for tag in "unstable-$BUILDKITE_COMMIT" latest; do
    echo "Processing docker tag for peeker: $tag"
    runv docker tag "materialize/ci-peeker:$MATERIALIZED_IMAGE_ID" "materialize/peeker:$tag"
    runv docker push "materialize/peeker:$tag"
done
