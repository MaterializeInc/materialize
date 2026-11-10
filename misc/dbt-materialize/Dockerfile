# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so Renovate refreshes the
# digest for security fixes.
# NOTE: requirements.txt is compiled for this Python minor version. Bump
# --python-version in its header and recompile when changing it.
FROM python:3.15.2@sha256:d650864079477d7468a0f384777be3b534f2bbdf5beb6155dbd73a4723b89300

# The official image can lag Debian security updates by weeks, so a digest bump
# alone does not pick them up. libbrotli is CVE-2026-111497.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade libbrotli1 libbrotli-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . dbt-materialize/

RUN cd dbt-materialize && pip install --no-cache-dir -r requirements.txt
