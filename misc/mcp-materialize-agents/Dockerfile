# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so dependabot refreshes the
# digest for security fixes.
FROM python:3.13-alpine@sha256:540c7d91f98ff6880174c40e99067bf5941eb54d818a7a5e094d188b196a934d

RUN pip install uv

WORKDIR /app/mcp-materialize

COPY . .

RUN uv sync --dev
