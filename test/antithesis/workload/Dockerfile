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

# Multi-stage source for the testdrive binary used by the testdrive-runner
# driver (singleton_driver_pg_cdc_testdrive). MZFROM is mzbuild's multi-
# stage directive — it registers a build-order dependency on the
# `testdrive` image and substitutes the resolved fingerprint at build
# time. The final workload image carries the binary plus libduckdb.so (a
# transitive dep of testdrive's foundationdb + duckdb-sys features), so
# the workload container can run testdrive without any further service
# plumbing.
MZFROM testdrive AS testdrive-src

# Debian 13 (trixie) base ships GLIBC 2.41; testdrive's mzbuild image is
# built against ubuntu-base (Ubuntu 24.04, GLIBC 2.39) and references
# GLIBC 2.38 / 2.39 symbols. The previous `python:3.12-slim-bookworm`
# (Debian 12, GLIBC 2.36) couldn't load the testdrive binary.
FROM python:3.12-slim-trixie

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

# Copy the testdrive binary plus the two shared libs it dynamically
# links against (testdrive is built with the foundationdb feature which
# pulls libfdb_c, and duckdb-sys which pulls libduckdb). `ldconfig`
# after the copy rebuilds the dynamic-linker cache so the loader finds
# them. Without libfdb_c.so testdrive errors at startup with
# "cannot open shared object file"; without libduckdb.so it errors
# inside the duckdb-backed `kafka-create-topic` builtins.
COPY --from=testdrive-src /usr/local/bin/testdrive /usr/local/bin/testdrive
COPY --from=testdrive-src /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=testdrive-src /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
RUN ldconfig

# Repo-checked-in testdrive .td files for the testdrive-runner drivers,
# brought into the build context by the `pre-image: copy` step in
# mzbuild.yml. The drivers reference these by repo-relative path under
# /opt/testdrive-files, e.g. /opt/testdrive-files/test/pg-cdc/dropped-slot-errors.td.
COPY testdrive-files/ /opt/testdrive-files/

# Copy test templates and entrypoint
COPY test/ /opt/antithesis/test/v1/materialize/
COPY workload-entrypoint.sh /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /opt/antithesis/test/v1/materialize/* 2>/dev/null || true

ENTRYPOINT ["/usr/local/bin/workload-entrypoint.sh"]
