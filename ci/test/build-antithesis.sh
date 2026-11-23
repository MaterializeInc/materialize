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
# build-antithesis.sh -- builds the images and the Kubernetes manifests of
# test/antithesis and pushes them to the Antithesis registry.
#
# With `--render <dir>` it only writes the manifests to <dir>/manifests, for
# `snouty validate <dir>`.

set -euo pipefail

cd "$(dirname "$0")/../.."

registry=us-central1-docker.pkg.dev/molten-verve-216720/materialize-repository
# orchestratord reads the version from the tag of the environmentd image to
# decide which arguments environmentd gets. A tag cannot contain the `+` of
# SemVer build metadata, so orchestratord takes `--` for it.
version=$(bin/pyactivate -c 'from materialize.mz_version import MzVersion; print(MzVersion.parse_cargo())')
tag=$version--antithesis.g$(git rev-parse --short=12 HEAD)

# Writes the manifests to $1/manifests.
render() {
    mkdir -p "$1/manifests"
    cp misc/helm-charts/testing/{postgres,minio,redpanda}.yaml "$1/manifests"
    helm template operator misc/helm-charts/operator --namespace materialize \
        --values test/antithesis/operator-values.yaml \
        --set "operator.image.repository=$registry/orchestratord,operator.image.tag=$tag" \
        > "$1/manifests/operator.yaml"
    # The system parameters that CI varies, for the driver to draw from.
    parameters=$(bin/pyactivate -c 'import base64, json
from materialize.mz_version import MzVersion
from materialize.mzcompose import get_variable_system_parameters as parameters
values = {p.key: p.values for p in parameters(MzVersion.parse_cargo(), False, "postgres-metadata")}
print(base64.b64encode(json.dumps(values).encode()).decode())')
    # driver.yaml creates the namespaces, so it gets a name that sorts first.
    sed -e "s|\${REGISTRY}|$registry|" -e "s|\${TAG}|$tag|" -e "s|\${LICENSE_KEY}|$MZ_CI_LICENSE_KEY|" \
        -e "s|\${SYSTEM_PARAMETERS}|$parameters|" test/antithesis/driver.yaml > "$1/manifests/00-driver.yaml"
}

if [[ ${1:-} = --render ]]; then
    render "$2"
    exit
fi

docker login -u _json_key --password-stdin "https://${registry%%/*}" <<< "$ANTITHESIS_GCP_SERVICE_ACCOUNT_JSON"

bin/pyactivate -m ci.test.build

echo "--- Pushing to the Antithesis registry"
# orchestratord derives the clusterd image from the environmentd image by
# replacing the image name, so both need the same repository and tag.
for image in environmentd clusterd orchestratord antithesis-driver; do
    bin/mzimage acquire "$image"
    docker tag "$(bin/mzimage spec "$image")" "$registry/$image:$tag"
    docker push "$registry/$image:$tag"
done

# The manifests contain the license key. They must stay out of the checkout,
# and the config image must not go to any other registry.
config=$(mktemp -d)
render "$config"
docker build --tag "$registry/antithesis-config:$tag" --file - "$config" <<EOF
FROM scratch
COPY manifests /manifests
EOF
docker push "$registry/antithesis-config:$tag"

cat > antithesis-images.json <<EOF
{
  "environmentd": "$registry/environmentd:$tag",
  "clusterd": "$registry/clusterd:$tag",
  "orchestratord": "$registry/orchestratord:$tag",
  "antithesis-driver": "$registry/antithesis-driver:$tag",
  "config": "$(docker image inspect --format '{{index .RepoDigests 0}}' "$registry/antithesis-config:$tag")"
}
EOF
buildkite-agent artifact upload antithesis-images.json
