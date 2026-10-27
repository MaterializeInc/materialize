#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Deploys an impersonation-flavored console preview for this branch. The build
# can then be selected in a Teleport impersonation session via
# /internal-console/?preview_build=<label>. See
# console/doc/organization-impersonation.md.

set -euo pipefail

. misc/shlib/shlib.bash

cd console
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
export VERCEL_ENVIRONMENT=preview
export SENTRY_RELEASE="$BUILDKITE_COMMIT"
corepack enable
retry yarn install --immutable --network-timeout 30000

# The deploy/internal-impersonation branch scope carries the impersonation
# build settings, notably BASENAME=/internal-console.
npx vercel@latest pull --yes \
  --environment="$VERCEL_ENVIRONMENT" \
  --git-branch=deploy/internal-impersonation \
  --token="$VERCEL_TOKEN"
bin/apply-vercel-csp.js --sentry-release="$SENTRY_RELEASE"
npx vercel@latest build --token="$VERCEL_TOKEN"

deployment_url="$(npx vercel@latest deploy --prebuilt --token="$VERCEL_TOKEN")"
alias_url="$(node bin/vercel-preview-url.js "${BUILDKITE_BRANCH}" internal.console.materialize.com)"
echo "Aliasing $deployment_url to $alias_url"
npx vercel@latest alias --scope=materialize --token="$VERCEL_TOKEN" \
  set "$deployment_url" "$alias_url"

label="${alias_url%%.*}"
printf "+++ Impersonation preview deployed. Select it in a Teleport session with ?preview_build=%s\n" "$label"
