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

run_if_not_dry() {
  if ! is_truthy "$CI_DRY_RUN"; then
    "$@"
  else
    echo "[DRY RUN] $*"
  fi
}

VERSION=$1

echo "--- Publishing Weekly Helm Chart $VERSION with Materialize $VERSION"
rm -rf gh-pages
git clone --branch "$GITHUB_PAGES_BRANCH" --depth 1 https://github.com/MaterializeInc/materialize.git gh-pages

mkdir -p $RELEASE_DIR
CHANGES_MADE=0
CHART=operator-weekly
CHART_PATH="$CHARTS_DIR/$CHART"
echo "Processing chart: $CHART version: $VERSION"
# Check if version already exists
if [ -f "gh-pages/$CHART-$VERSION.tgz" ]; then
  echo "Chart $CHART version $VERSION already exists, skipping"
  exit 0
fi
# Lint chart
if ! helm lint "$CHART_PATH"; then
  echo "Linting failed for $CHART"
  exit 1
fi
# Package chart
helm package "$CHART_PATH" --destination $RELEASE_DIR
CHANGES_MADE=1
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
  git diff HEAD~
  run_if_not_dry git push origin $GITHUB_PAGES_BRANCH
  cd ..
else
  echo "No new chart versions to publish"
  exit 0
fi

echo "--- Verifying that Helm Chart has been published"
i=0
if ! is_truthy "$CI_DRY_RUN"; then
  HELM_CHART_PUBLISHED=false
  while (( i < 60 )); do
    YAML=$(curl -s "https://materializeinc.github.io/materialize/index.yaml")

    MATCH_FOUND=$(echo "$YAML" | yq ".entries[\"materialize-operator-weekly\"][]
      | select(.version == \"$VERSION\" and .appVersion == \"$VERSION\")
      | .version")

    if [[ -n "$MATCH_FOUND" ]]; then
      echo "Helm Chart $VERSION with Materialize $VERSION has successfully been published"
      HELM_CHART_PUBLISHED=true
      break
    fi

    sleep 10
    ((i += 1))
  done

  if [ "$HELM_CHART_PUBLISHED" = false ]; then
    echo "Failing because Helm Chart was not successfully published"
    exit 1
  fi
else
  echo "[DRY RUN] Nothing to verify"
fi
