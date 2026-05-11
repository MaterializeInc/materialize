# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Antithesis workload client for Materialize.
#
# Python-based test driver that connects to materialized via pgwire,
# produces Kafka messages, and emits Antithesis assertions.

FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    psycopg[binary]==3.2.9 \
    confluent-kafka==2.8.0 \
    antithesis==0.2.0

# setup-complete script
COPY setup-complete.sh /usr/local/bin/setup-complete.sh
RUN chmod +x /usr/local/bin/setup-complete.sh

# Test template directory — populated by antithesis-workload skill later
RUN mkdir -p /opt/antithesis/test/v1/materialize

# Catalog directory for Python assertion cataloging
RUN mkdir -p /opt/antithesis/catalog

# Copy test templates and entrypoint
COPY test/ /opt/antithesis/test/v1/materialize/
COPY workload-entrypoint.sh /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /opt/antithesis/test/v1/materialize/* 2>/dev/null || true

ENTRYPOINT ["/usr/local/bin/workload-entrypoint.sh"]
