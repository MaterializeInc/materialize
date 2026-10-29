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
FROM python:3.15.0@sha256:4e324f579bdd7d0ce3cbae52e27bddeb316c9c23aa7563ac6a9811bbca55541b

# The official image can lag Debian security updates by weeks, so a digest bump
# alone does not pick them up. libbrotli is CVE-2026-111497.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade libbrotli1 libbrotli-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . dbt-materialize/

RUN pip install --no-cache-dir -r dbt-materialize/requirements.txt --find-links /dbt-materialize
