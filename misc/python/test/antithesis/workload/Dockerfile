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
# produces Kafka messages, and emits Antithesis assertions. The
# parallel-workload driver reuses the real `materialize.parallel_workload`
# Python package — see mzbuild.yml for the pre-image copy of the slice it
# needs, and stubs/materialize/mzcompose/ for the docker-compose surface we
# have to mock out.

FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# `confluent-kafka[avro]` pulls fastavro, which `data_ingest.executor`
# imports at module top via `confluent_kafka.schema_registry.avro`.
# `pg8000`, `websocket-client`, `requests`, `xxhash`, `zstandard` cover the
# rest of the module-load-time imports walking from `parallel_workload` →
# `data_ingest` → `materialize.util`.
RUN pip install --no-cache-dir \
    psycopg[binary]==3.2.9 \
    "confluent-kafka[avro]==2.8.0" \
    antithesis==0.2.0 \
    PyMySQL==1.1.1 \
    pg8000==1.31.2 \
    websocket-client==1.8.0 \
    requests==2.32.3 \
    xxhash==3.5.0 \
    zstandard==0.23.0

# setup-complete script
COPY setup-complete.sh /usr/local/bin/setup-complete.sh
RUN chmod +x /usr/local/bin/setup-complete.sh

# Test template directory — populated by antithesis-workload skill later
RUN mkdir -p /opt/antithesis/test/v1/materialize

# Catalog directory for Python assertion cataloging
RUN mkdir -p /opt/antithesis/catalog

# Ship the `materialize.*` Python package needed by the parallel-workload
# driver. Stubs are copied first so that the real parallel-workload code
# layered on top can satisfy its top-level `from materialize.mzcompose...`
# imports against tiny placeholders. `MZ_ROOT` is required by
# `materialize/__init__.py` at import time — point it at the package root.
COPY stubs/materialize/ /opt/antithesis-pkg/materialize/
COPY materialize/ /opt/antithesis-pkg/materialize/
ENV PYTHONPATH=/opt/antithesis-pkg
ENV MZ_ROOT=/opt/antithesis-pkg

# Copy test templates and entrypoint
COPY test/ /opt/antithesis/test/v1/materialize/
COPY workload-entrypoint.sh /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /opt/antithesis/test/v1/materialize/* 2>/dev/null || true

ENTRYPOINT ["/usr/local/bin/workload-entrypoint.sh"]
