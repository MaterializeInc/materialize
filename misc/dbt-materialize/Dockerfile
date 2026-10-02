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
FROM python:3.15.0rc2@sha256:97ac60d150c8fa23220cab3933a9302cd59d4d54e062ed18a02058e64e52a5f5

COPY . dbt-materialize/

RUN pip install --no-cache-dir -r dbt-materialize/requirements.txt --find-links /dbt-materialize
