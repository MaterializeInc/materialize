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
# Deploys the console to internal.console.materialize.com, the internal host
# Materialize employees use to access customer environments via impersonation.

set -euo pipefail

cd console
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
export VERCEL_ENVIRONMENT=preview
export SENTRY_RELEASE="$BUILDKITE_COMMIT"
corepack enable
yarn install --immutable --network-timeout 30000

npx vercel@latest pull --yes \
  --environment="$VERCEL_ENVIRONMENT" \
  --git-branch=deploy/internal-impersonation \
  --token="$VERCEL_TOKEN"
npx vercel@latest build --token="$VERCEL_TOKEN"

deployment_url="$(npx vercel@latest deploy --prebuilt --token="$VERCEL_TOKEN")"
npx vercel@latest alias --scope=materialize --token="$VERCEL_TOKEN" \
  set "$deployment_url" "internal.console.materialize.com"
