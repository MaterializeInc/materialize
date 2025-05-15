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
export GIT_PAGER=cat

: "${CI_HELM_CHART_VERSION:=}"
: "${CI_MZ_VERSION:=}"
: "${CI_NO_TERRAFORM_BUMP:=0}"
: "${CI_DRY_RUN:=0}"
if [[ -z "$CI_HELM_CHART_VERSION" || -z "$CI_MZ_VERSION" ]]; then
  echo "\$CI_HELM_CHART_VERSION and \$CI_MZ_VERSION have to be set, use https://trigger-ci.dev.materialize.com to trigger this pipeline"
  exit 1
fi

cat > ~/.netrc <<EOF
machine github.com
login materializebot
password $GITHUB_TOKEN
EOF
chmod 600 ~/.netrc

run_if_not_dry() {
  if ! is_truthy "$CI_DRY_RUN"; then
    "$@"
  else
    echo "[DRY RUN] $*"
  fi
}

echo "--- Publishing Helm Chart $CI_HELM_CHART_VERSION with Materialize $CI_MZ_VERSION"
bin/helm-chart-version-bump --helm-chart-version "$CI_HELM_CHART_VERSION" "$CI_MZ_VERSION"
git commit -a -m "Bumping helm-chart version to $CI_HELM_CHART_VERSION with Materialize $CI_MZ_VERSION"
TAG="self-managed-$CI_HELM_CHART_VERSION"
git tag "$TAG"
git diff HEAD~
run_if_not_dry git push "https://github.com/MaterializeInc/materialize.git" "$TAG"

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
git clone --branch "$GITHUB_PAGES_BRANCH" --depth 1 https://github.com/MaterializeInc/materialize.git gh-pages

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
  while (( i < 30 )); do
    YAML=$(curl -s "https://materializeinc.github.io/materialize/index.yaml")
    CURRENT_HELM_CHART_VERSION=$(echo "$YAML" | yq '.entries["materialize-operator"][0].version')
    CURRENT_MZ_VERSION=$(echo "$YAML" | yq '.entries["materialize-operator"][0].appVersion')
    if [[ "$CURRENT_HELM_CHART_VERSION" == "$CI_HELM_CHART_VERSION" && "$CURRENT_MZ_VERSION" == "$CI_MZ_VERSION" ]]; then
      echo "Helm Chart $CURRENT_HELM_CHART_VERSION with Materialize $CURRENT_MZ_VERSION has successfully been published"
      HELM_CHART_PUBLISHED=true
      break
    fi
    echo "Latest version seems to be Helm Chart $CURRENT_HELM_CHART_VERSION with Materialize $CURRENT_MZ_VERSION"
    sleep 10
    ((i += 1))
  done

  if [ "$HELM_CHART_PUBLISHED" = false ]; then
    echo "Failing because Helm Chart was not successfully published"
    exit 1
  fi

  HIGHEST_HELM_CHART_VERSION=$(echo "$YAML" | yq '.entries["materialize-operator"][].version' | grep -v beta | sort -V | tail -n 1)

  if [ "$HIGHEST_HELM_CHART_VERSION" != "$CI_HELM_CHART_VERSION" ]; then
    echo "--- Higher helm-chart version $HIGHEST_HELM_CHART_VERSION > $CI_HELM_CHART_VERSION has already been released, not bumping terraform versions"
    CI_NO_TERRAFORM_BUMP=1
  fi
else
  echo "[DRY RUN] Nothing to verify"
fi

if is_truthy "$CI_NO_TERRAFORM_BUMP"; then
  echo "--- Not bumping terraform versions"
else
  echo "--- Bumping terraform-helm-materialize"
  rm -rf terraform-helm-materialize
  git clone https://github.com/MaterializeInc/terraform-helm-materialize.git
  cd terraform-helm-materialize
  sed -i "s/\".*\"\(.*\) # META: helm-chart version/\"$CI_HELM_CHART_VERSION\"\\1 # META: helm-chart version/" variables.tf
  sed -i "s/\".*\"\(.*\) # META: mz version/\"$CI_MZ_VERSION\"\\1 # META: mz version/" variables.tf
  terraform-docs markdown table --output-file README.md --output-mode inject .
  git config user.email "noreply@materialize.com"
  git config user.name "Buildkite"
  git add variables.tf README.md
  git commit -m "Bump to helm-chart $CI_HELM_CHART_VERSION, materialize $CI_MZ_VERSION"
# Bump the patch version by one (v0.1.12 -> v0.1.13)
  TERRAFORM_HELM_VERSION=$(git for-each-ref --sort=creatordate --format '%(refname:strip=2)' refs/tags | grep '^v' | tail -n1 | awk -F. -v OFS=. '{$NF += 1; print}')
  git tag "$TERRAFORM_HELM_VERSION"
  git diff HEAD~
  run_if_not_dry git push origin main "$TERRAFORM_HELM_VERSION"
  cd ..

  declare -A TERRAFORM_VERSION
  for repo in terraform-aws-materialize terraform-azurerm-materialize terraform-google-materialize; do
    echo "--- Bumping $repo"
    rm -rf $repo
    git clone https://github.com/MaterializeInc/$repo.git
    cd $repo
    sed -i "s|github.com/MaterializeInc/terraform-helm-materialize?ref=v[0-9.]*|github.com/MaterializeInc/terraform-helm-materialize?ref=$TERRAFORM_HELM_VERSION|" main.tf
    terraform-docs markdown table --output-file README.md --output-mode inject .
    # Initialize Terraform to update the lock file
    if ! is_truthy "$CI_DRY_RUN"; then
      terraform init -upgrade
      # If lock file doesn't exist yet (unlikely but possible)
      if [ ! -f .terraform.lock.hcl ]; then
        echo "No lock file found. Creating it with terraform init."
        terraform init
      fi
    fi
    git config user.email "noreply@materialize.com"
    git config user.name "Buildkite"
    git add main.tf README.md .terraform.lock.hcl
    git commit -m "Bump to terraform-helm-materialize $TERRAFORM_HELM_VERSION"
# Bump the patch version by one (v0.1.12 -> v0.1.13)
    TERRAFORM_VERSION[$repo]=$(git for-each-ref --sort=creatordate --format '%(refname:strip=2)' refs/tags | grep '^v' | tail -n1 | awk -F. -v OFS=. '{$NF += 1; print}')
    git tag "${TERRAFORM_VERSION[$repo]}"
    git diff HEAD~
    run_if_not_dry git push origin main "${TERRAFORM_VERSION[$repo]}"
    cd ..
  done
fi

echo "--- Bumping versions in Self-Managed Materialize documentation"
ORCHESTRATORD_VERSION=$(yq -r '.operator.image.tag' misc/helm-charts/operator/values.yaml)
DOCS_BRANCH=self-managed-docs/$(echo "$CI_HELM_CHART_VERSION" | cut -d. -f1,2)
git fetch origin "$DOCS_BRANCH"
git checkout "origin/$DOCS_BRANCH"
git submodule update --init --recursive
git config user.email "noreply@materialize.com"
git config user.name "Buildkite"
VERSIONS_YAML_PATH=doc/user/data/self_managed/latest_versions.yml
yq -i ".operator_helm_chart_version = \"$CI_HELM_CHART_VERSION\"" $VERSIONS_YAML_PATH
yq -i ".environmentd_version = \"$CI_MZ_VERSION\"" $VERSIONS_YAML_PATH
yq -i ".orchestratord_version = \"$ORCHESTRATORD_VERSION\"" $VERSIONS_YAML_PATH
if ! is_truthy "$CI_NO_TERRAFORM_BUMP"; then
  yq -i ".terraform_helm_version= \"$TERRAFORM_HELM_VERSION\"" $VERSIONS_YAML_PATH
  yq -i ".terraform_gcp_version= \"${TERRAFORM_VERSION[terraform-google-materialize]}\"" $VERSIONS_YAML_PATH
  yq -i ".terraform_azure_version= \"${TERRAFORM_VERSION[terraform-azurerm-materialize]}\"" $VERSIONS_YAML_PATH
  yq -i ".terraform_aws_version= \"${TERRAFORM_VERSION[terraform-aws-materialize]}\"" $VERSIONS_YAML_PATH
fi
git add $VERSIONS_YAML_PATH
git commit -m "docs: Bump to helm-chart $CI_HELM_CHART_VERSION, environmentd $CI_MZ_VERSION, orchestratord $ORCHESTRATORD_VERSION"
git diff HEAD~
run_if_not_dry git push origin "$DOCS_BRANCH"

if ! is_truthy "$CI_NO_TERRAFORM_BUMP"; then
  echo "--- Bumping versions in Terraform Nightly tests"
  git fetch origin main
  git checkout origin/main
  git submodule update --init --recursive
  sed -i "s|\"git::https://github.com/MaterializeInc/terraform-aws-materialize.git?ref=.*\"|\"git::https://github.com/MaterializeInc/terraform-aws-materialize.git?ref=${TERRAFORM_VERSION[terraform-aws-materialize]}\"|" test/terraform/aws-*/main.tf
  sed -i "s|\"git::https://github.com/MaterializeInc/terraform-azurerm-materialize.git?ref=.*\"|\"git::https://github.com/MaterializeInc/terraform-azurerm-materialize.git?ref=${TERRAFORM_VERSION[terraform-azurerm-materialize]}\"|" test/terraform/azure-*/main.tf
  sed -i "s|\"git::https://github.com/MaterializeInc/terraform-google-materialize.git?ref=.*\"|\"git::https://github.com/MaterializeInc/terraform-google-materialize.git?ref=${TERRAFORM_VERSION[terraform-google-materialize]}\"|" test/terraform/gcp-*/main.tf
  git add test/terraform/*/main.tf
  git commit -m "terraform tests: Bump to AWS ${TERRAFORM_VERSION[terraform-aws-materialize]}, GCP ${TERRAFORM_VERSION[terraform-google-materialize]}, Azure ${TERRAFORM_VERSION[terraform-azurerm-materialize]}"
  git diff HEAD~
  run_if_not_dry git push origin main
fi
