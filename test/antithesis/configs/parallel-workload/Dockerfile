# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Antithesis config image (group: parallel-workload). FROM-scratch tarball
# carrying the per-group docker-compose.yaml + a `.env` mapping
# `${MATERIALIZED_IMAGE}` / `${ANTITHESIS_WORKLOAD_IMAGE}` to the
# current mzbuild fingerprints. Compose loads `.env` automatically.

FROM scratch
COPY docker-compose.yaml .env /
