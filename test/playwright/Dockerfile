# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so Renovate refreshes the
# digest for security fixes. mzbuild only rebuilds when this directory changes,
# so an unpinned tag kept serving whatever Node it resolved to at the last edit.
FROM node:26-trixie-slim@sha256:5adad64f9a1fca2d51972caa65983634c8a774eb300d72b20f7f2a0f60368072

COPY package.json /tmp/playwright-package.json
RUN playwright_version=$(node -p 'require("/tmp/playwright-package.json").devDependencies["@playwright/test"]') \
    && npx --yes "playwright@$playwright_version" install --with-deps chromium \
    && rm -rf /root/.npm /var/lib/apt/lists/*
