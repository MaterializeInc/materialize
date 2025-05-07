#!/bin/bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

. misc/shlib/shlib.bash

CHARTS_DIR=misc/helm-charts
GITHUB_PAGES_BRANCH=gh-pages
RELEASE_DIR=.cr-release-packages

export GIT_AUTHOR_NAME=Materialize Bot
export GIT_AUTHOR_EMAIL=infra+bot@materialize.com
export GIT_COMMITTER_NAME=$GIT_AUTHOR_NAME
export GIT_COMMITTER_EMAIL=$GIT_AUTHOR_EMAIL

: "${CI_HELM_CHART_VERSION:=}"
: "${CI_MZ_VERSION:=}"
if [[ -z "$CI_HELM_CHART_VERSION" || -z "$CI_MZ_VERSION" ]]; then
  echo "\$CI_HELM_CHART_VERSION and \$CI_MZ_VERSION have to be set, use https://trigger-ci.dev.materialize.com to trigger this pipeline"
  exit 1
fi

echo "--- Publishing Helm Chart $CI_HELM_CHART_VERSION with Materialize $CI_MZ_VERSION"
bin/helm-chart-version-bump --helm-chart-version "$CI_HELM_CHART_VERSION" "$CI_MZ_VERSION"
git commit -a -m "Bumping helm-chart version to $CI_HELM_CHART_VERSION with Materialize $CI_MZ_VERSION"
TAG="self-managed-$CI_HELM_CHART_VERSION"
git tag "$TAG"
git push "https://materializebot:$GITHUB_TOKEN@github.com/MaterializeInc/materialize.git" "$TAG"

# Find directories containing Chart.yaml
CHARTS=""
for dir in "$CHARTS_DIR"/*/; do
  if [ -f "${dir}Chart.yaml" ]; then
    chart_name=$(basename "$dir")
    CHARTS="${CHARTS:+${CHARTS} }$chart_name"
  fi
done
if [ -z "$CHARTS" ]; then
  echo "No valid Helm charts found"
  exit 0
fi
echo "Found valid charts: $CHARTS"

rm -rf gh-pages
git clone --branch "$GITHUB_PAGES_BRANCH" --depth 1 https://"$GITHUB_TOKEN"@github.com/MaterializeInc/materialize.git gh-pages

mkdir -p $RELEASE_DIR
CHANGES_MADE=0
for CHART in $CHARTS; do
  CHART_PATH="$CHARTS_DIR/$CHART"
  VERSION=$(yq eval '.version' "$CHART_PATH"/Chart.yaml)
  echo "Processing chart: $CHART version: $VERSION"
  # Check if version already exists
  if [ -f "gh-pages/$CHART-$VERSION.tgz" ]; then
    echo "Chart $CHART version $VERSION already exists, skipping"
    continue
  fi
  # Lint chart
  if ! helm lint "$CHART_PATH"; then
    echo "Linting failed for $CHART"
    exit 1
  fi
  # Package chart
  helm package "$CHART_PATH" --destination $RELEASE_DIR
  CHANGES_MADE=1
done
# Only proceed if we have new packages
if [ $CHANGES_MADE -eq 1 ]; then
  # Copy new charts to gh-pages
  cp $RELEASE_DIR/*.tgz gh-pages/
  # Update the repository index
  cd gh-pages
  REPO_URL="https://materializeinc.github.io/materialize"
  if [ -f index.yaml ]; then
    helm repo index . --url "$REPO_URL" --merge index.yaml
  else
    helm repo index . --url "$REPO_URL"
  fi
  # Commit and push changes
  git add .
  git config user.email "noreply@materialize.com"
  git config user.name "Buildkite"
  git commit -m "helm-charts: publish updated charts"
  git push origin $GITHUB_PAGES_BRANCH
  cd ..
else
  echo "No new chart versions to publish"
  exit 0
fi

ORCHESTRATORD_VERSION=$(yq -r '.operator.image.tag' misc/helm-charts/operator/values.yaml)
DOCS_BRANCH=self-managed-docs/$(echo "$CI_HELM_CHART_VERSION" | cut -d. -f1,2)
git fetch origin "$DOCS_BRANCH"
git checkout "origin/$DOCS_BRANCH"
git config user.email "noreply@materialize.com"
git config user.name "Buildkite"
VERSIONS_YAML_PATH=doc/user/data/self_managed/latest_versions.yml
yq -Y -i ".operator_helm_chart_version = \"$CI_HELM_CHART_VERSION\"" $VERSIONS_YAML_PATH
yq -Y -i ".environmentd_version = \"$CI_MZ_VERSION\"" $VERSIONS_YAML_PATH
yq -Y -i ".orchestratord_version = \"$ORCHESTRATORD_VERSION\"" $VERSIONS_YAML_PATH
git add $VERSIONS_YAML_PATH
git commit -m "docs: Bump to helm-chart $CI_HELM_CHART_VERSION, environmentd $CI_MZ_VERSION, orchestratord $ORCHESTRATORD_VERSION"
git push "https://materializebot:$GITHUB_TOKEN@github.com/MaterializeInc/materialize.git" "$DOCS_BRANCH"

i=0
while (( i < 30 )); do
  YAML=$(curl -s "https://materializeinc.github.io/materialize/index.yaml")
  CURRENT_HELM_CHART_VERSION=$(echo "$YAML" | yq '.entries["materialize-operator"][0].version')
  CURRENT_MZ_VERSION=$(echo "$YAML" | yq '.entries["materialize-operator"][0].appVersion')
  if [[ "$CURRENT_HELM_CHART_VERSION" == "$CI_HELM_CHART_VERSION" && "$CURRENT_MZ_VERSION" == "$CI_MZ_VERSION" ]]; then
    echo "Helm Chart $CURRENT_HELM_CHART_VERSION with Materialize $CURRENT_MZ_VERSION has successfully been published"
    exit 0
  fi
  echo "Latest version seems to be Helm Chart $CURRENT_HELM_CHART_VERSION with Materialize $CURRENT_MZ_VERSION"
  sleep 10
  ((i += 1))
done
exit 1
