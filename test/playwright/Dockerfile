# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM node:24-trixie-slim

# NOTE: Keep this version in sync with `@playwright/test` in
# test/dataflow-visualizer/package.json. Each Playwright release pins its own
# browser build, and the test run resolves the browser from this prebaked cache.
# A mismatch fails at launch with "Executable doesn't exist", not at install.
RUN npx --yes playwright@1.62.1 install --with-deps chromium \
    && rm -rf /root/.npm /var/lib/apt/lists/*
