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
# console-e2e-prod.sh — Build the console with Vercel production environment,
# deploy to a Vercel preview URL, and run e2e tests against it.
#
# This mirrors the old GHA merge_queue.yml e2e-tests job from the console repo.
#
# Required environment variables:
#   VERCEL_TOKEN        — Vercel API token
#   VERCEL_ORG_ID       — Vercel organization ID
#   VERCEL_PROJECT_ID   — Vercel project ID
#   E2E_TEST_PASSWORD   — Frontegg e2e test user password (production)

set -euo pipefail

. misc/shlib/shlib.bash

cd console

export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
corepack enable

ci_collapsed_heading "Installing dependencies"
yarn install --immutable --network-timeout 30000
yarn playwright install --with-deps
npm install --global vercel@latest

ci_collapsed_heading "Pulling Vercel production environment"
vercel pull --yes --environment=production --token="$VERCEL_TOKEN"
mv .vercel/.env.production.local .vercel/.env.preview.local

ci_collapsed_heading "Building with Vercel production environment"
export SENTRY_RELEASE="$BUILDKITE_COMMIT"
export FORCE_OVERRIDE_STACK=production
bin/apply-vercel-csp.js
vercel build --token="$VERCEL_TOKEN"

ci_collapsed_heading "Deploying to Vercel preview"
CONSOLE_ADDR=$(vercel deploy --prebuilt --token="$VERCEL_TOKEN")
export CONSOLE_ADDR
echo "Console deployed at: $CONSOLE_ADDR"

ci_uncollapsed_heading "Running console e2e tests (production)"
export CLOUD_HOST=cloud.materialize.com
e2e_exit=0
set -o pipefail
yarn test:e2e:all | tee run.log || e2e_exit=$?

exit "$e2e_exit"
