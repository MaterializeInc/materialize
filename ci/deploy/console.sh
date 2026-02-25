#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

cd console
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0
export VERCEL_ENVIRONMENT=production
export SENTRY_RELEASE="$BUILDKITE_COMMIT"
corepack enable
yarn install --immutable --network-timeout 30000

npx vercel@latest pull --yes --environment="$VERCEL_ENVIRONMENT" --token="$VERCEL_TOKEN"
npx vercel@latest build --token="$VERCEL_TOKEN" --prod

npx vercel@latest deploy --prebuilt --prod --token="$VERCEL_TOKEN"
