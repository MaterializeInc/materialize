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
FROM python:3.10.14@sha256:9c0e621579faf384d982986f2e0ba86bf09619076842cd0fbd2f24a3bf09f0bc

COPY . dbt-materialize/

RUN pip install --no-cache-dir -r dbt-materialize/requirements.txt --find-links /dbt-materialize
