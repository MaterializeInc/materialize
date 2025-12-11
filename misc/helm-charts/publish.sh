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

: "${BUILDKITE_TAG:=}"

if [[ -z "$BUILDKITE_TAG" ]]; then
  echo "\$BUILDKITE_TAG not set, checking for version to publish"
  BUILDKITE_TAG=$(bin/helm-chart-version-to-publish)
  if [[ -z "$BUILDKITE_TAG" ]]; then
    echo "No version to publish, done"
    exit
  fi
  echo "Running for version $BUILDKITE_TAG"
  git fetch origin "$BUILDKITE_TAG"
  git checkout "$BUILDKITE_TAG"
fi

: "${CI_DRY_RUN:=0}"
: "${CI_NO_TERRAFORM_BUMP:=0}"

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

echo "--- Publishing Helm Chart $BUILDKITE_TAG"
rm -rf gh-pages
git clone --branch "$GITHUB_PAGES_BRANCH" --depth 1 https://github.com/MaterializeInc/materialize.git gh-pages

mkdir -p $RELEASE_DIR
CHANGES_MADE=0
CHART=operator
CHART_PATH="$CHARTS_DIR/$CHART"
VERSION=$(yq eval '.version' "$CHART_PATH"/Chart.yaml)
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
  git --no-pager diff HEAD~
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

    MATCH_FOUND=$(echo "$YAML" | yq ".entries[\"materialize-operator\"][]
      | select(.version == \"$BUILDKITE_TAG\" and .appVersion == \"$BUILDKITE_TAG\")
      | .version")

    if [[ -n "$MATCH_FOUND" ]]; then
      echo "Helm Chart $BUILDKITE_TAG has successfully been published"
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

  # sort -V doesn't do a proper semver sort, have to manually fix that, v26.0.0~rc.6 is considered to be less than v26.0.0
  HIGHEST_HELM_CHART_VERSION=$(echo "$YAML" | yq '.entries["materialize-operator"][].version' | grep -v beta | sed "s/-rc\./~rc./" | sort -V | tail -n 1)

  if [ "$HIGHEST_HELM_CHART_VERSION" != "$BUILDKITE_TAG" ]; then
    echo "--- Higher helm-chart version $HIGHEST_HELM_CHART_VERSION > $BUILDKITE_TAG has already been released, not bumping terraform versions"
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
  sed -i "s/\".*\"\(.*\) # META: helm-chart version/\"$BUILDKITE_TAG\"\\1 # META: helm-chart version/" variables.tf
  sed -i "s/\".*\"\(.*\) # META: mz version/\"$BUILDKITE_TAG\"\\1 # META: mz version/" variables.tf
  terraform-docs markdown table --output-file README.md --output-mode inject .
  git config user.email "noreply@materialize.com"
  git config user.name "Buildkite"
  git add variables.tf README.md
  git commit -m "Bump to helm-chart $BUILDKITE_TAG"
  # Bump the patch version by one (v0.1.12 -> v0.1.13)
  TERRAFORM_HELM_VERSION=$(git for-each-ref --sort=creatordate --format '%(refname:strip=2)' refs/tags | grep '^v' | tail -n1 | awk -F. -v OFS=. '{$NF += 1; print}')
  git tag "$TERRAFORM_HELM_VERSION"
  git --no-pager diff HEAD~
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
    git --no-pager diff HEAD~
    run_if_not_dry git push origin main "${TERRAFORM_VERSION[$repo]}"
    cd ..
  done

  # TODO(def-) Reenable when https://github.com/MaterializeInc/materialize-terraform-self-managed/issues/123 is fixed
  # echo "--- Bumping new terraform repository"
  # rm -rf materialize-terraform-self-managed
  # git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
  # cd materialize-terraform-self-managed
  # sed -i "s/\".*\"\(.*\) # META: helm-chart version/\"$BUILDKITE_TAG\"\\1 # META: helm-chart version/" aws/modules/operator/variables.tf azure/modules/operator/variables.tf gcp/modules/operator/variables.tf
  # sed -i "s/\".*\"\(.*\) # META: mz version/\"$BUILDKITE_TAG\"\\1 # META: mz version/" kubernetes/modules/materialize-instance/variables.tf
  # for dir in aws/modules/operator azure/modules/operator gcp/modules/operator kubernetes/modules/materialize-instance; do
  #   terraform-docs --config .terraform-docs.yml $dir > $dir/README.md
  # done
  # git config user.email "noreply@materialize.com"
  # git config user.name "Buildkite"
  # git add aws/modules/operator/variables.tf azure/modules/operator/variables.tf gcp/modules/operator/variables.tf kubernetes/modules/materialize-instance/variables.tf aws/modules/operator/README.md azure/modules/operator/README.md gcp/modules/operator/README.md kubernetes/modules/materialize-instance/README.md
  # git commit -m "Bump to helm-chart $BUILDKITE_TAG"
  # # Bump the patch version by one (v0.1.12 -> v0.1.13)
  # TERRAFORM_SELF_MANAGED_VERSION=$(git for-each-ref --sort=creatordate --format '%(refname:strip=2)' refs/tags | grep '^v' | tail -n1 | awk -F. -v OFS=. '{$NF += 1; print}')
  # git tag "$TERRAFORM_SELF_MANAGED_VERSION"
  # git --no-pager diff HEAD~
  # run_if_not_dry git push origin main "$TERRAFORM_SELF_MANAGED_VERSION"
fi

if [[ "$BUILDKITE_TAG" != *"-rc."* ]]; then
  echo "--- Bumping versions in Self-Managed Materialize documentation"
  ORCHESTRATORD_VERSION=$(yq -r '.operator.image.tag' misc/helm-charts/operator/values.yaml)
  DOCS_BRANCH=main
  git fetch origin "$DOCS_BRANCH"
  git checkout "origin/$DOCS_BRANCH"
  git submodule update --init --recursive
  git config user.email "noreply@materialize.com"
  git config user.name "Buildkite"
  VERSIONS_YAML_PATH=doc/user/data/self_managed/latest_versions.yml
  yq -i ".operator_helm_chart_version = \"$BUILDKITE_TAG\"" $VERSIONS_YAML_PATH
  yq -i ".environmentd_version = \"$BUILDKITE_TAG\"" $VERSIONS_YAML_PATH
  yq -i ".orchestratord_version = \"$ORCHESTRATORD_VERSION\"" $VERSIONS_YAML_PATH
  if ! is_truthy "$CI_NO_TERRAFORM_BUMP"; then
    yq -i ".terraform_helm_version= \"$TERRAFORM_HELM_VERSION\"" $VERSIONS_YAML_PATH
    yq -i ".terraform_gcp_version= \"${TERRAFORM_VERSION[terraform-google-materialize]}\"" $VERSIONS_YAML_PATH
    yq -i ".terraform_azure_version= \"${TERRAFORM_VERSION[terraform-azurerm-materialize]}\"" $VERSIONS_YAML_PATH
    yq -i ".terraform_aws_version= \"${TERRAFORM_VERSION[terraform-aws-materialize]}\"" $VERSIONS_YAML_PATH
  fi

  OPERATOR_COMPAT_YAML_PATH=doc/user/data/self_managed/self_managed_operator_compatibility.yml
  yq --prettyPrint --inplace ".rows = [{
    \"Materialize Operator\": \"$BUILDKITE_TAG\",
    \"orchestratord version\": \"$BUILDKITE_TAG\",
    \"environmentd version\": \"$BUILDKITE_TAG\",
    \"Release date\": \"$(date +%Y-%m-%d)\",
    \"Notes\": \"\"
  }] + .rows" $OPERATOR_COMPAT_YAML_PATH

  git add $VERSIONS_YAML_PATH
  git commit -m "docs: Bump self-managed to $BUILDKITE_TAG"
  git --no-pager diff HEAD~
  run_if_not_dry git push origin "HEAD:$DOCS_BRANCH"
fi

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
  git --no-pager diff HEAD~
  run_if_not_dry git push origin HEAD:main
fi
