# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so dependabot refreshes the
# digest for security fixes. mzbuild only rebuilds when this directory changes,
# so an unpinned tag kept serving whatever Node it resolved to at the last edit.
FROM node:24-trixie-slim@sha256:43b9ba1142b8dcf33fa8f922ca1be85714adc444e0586c7cf7726daaeabc1bea

# NOTE: Keep this version in sync with `@playwright/test` in
# test/dataflow-visualizer/package.json. Each Playwright release pins its own
# browser build, and the test run resolves the browser from this prebaked cache.
# A mismatch fails at launch with "Executable doesn't exist", not at install.
RUN npx --yes playwright@1.62.1 install --with-deps chromium \
    && rm -rf /root/.npm /var/lib/apt/lists/*
