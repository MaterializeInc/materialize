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
: "${CI_DRY_RUN:=0}"

run_if_not_dry() {
  if ! is_truthy "$CI_DRY_RUN"; then
    "$@"
  else
    echo "[DRY RUN] $*"
  fi
}

REMOTE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --remote)
      REMOTE="$2"
      shift 2
      ;;
    --help|-h)
      cat <<EOF
Usage: publish-weekly.sh --version <version> --remote <remote>
Example: publish-weekly.sh --version v0.161.0 --remote origin
EOF
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

echo "--- Publishing Weekly Helm Chart $VERSION with Materialize $VERSION"
rm -rf gh-pages
if is_truthy "${CI:-0}"; then
  git clone --branch "$GITHUB_PAGES_BRANCH" --depth 1 https://github.com/MaterializeInc/materialize.git gh-pages
else
  git clone --branch "$GITHUB_PAGES_BRANCH" --depth 1 git@github.com:MaterializeInc/materialize.git gh-pages
fi

mkdir -p $RELEASE_DIR
CHART=operator-weekly
CHART_PATH="$CHARTS_DIR/$CHART"
echo "Processing chart: $CHART version: $VERSION"
git fetch --tags "$REMOTE"
git checkout "$VERSION"
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
cp "$CHARTS_DIR/operator/values.yaml" "$CHART_PATH"
cp -r "$CHARTS_DIR/operator/templates" "$CHART_PATH"
helm package "$CHART_PATH" --destination $RELEASE_DIR
rm -rf "$CHART_PATH/values.yaml" "$CHART_PATH/templates"
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
if is_truthy "${CI:-0}"; then
  git config user.email "noreply@materialize.com"
  git config user.name "Buildkite"
  git remote set-url origin https://x-access-token:"$GITHUB_TOKEN"@github.com/MaterializeInc/materialize.git
fi
git commit -m "helm-charts: publish updated charts"
git --no-pager diff HEAD~
run_if_not_dry git push origin $GITHUB_PAGES_BRANCH
cd ..
