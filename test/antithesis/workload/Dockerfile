# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Antithesis workload client for Materialize — per-group flavor.
#
# Python-based test driver that connects to materialized via pgwire,
# produces Kafka messages, and emits Antithesis assertions. The
# parallel-workload driver reuses the real `materialize.parallel_workload`
# Python package — see mzbuild.yml for the pre-image copy of the slice it
# needs, and stubs/materialize/mzcompose/ for the docker-compose surface
# we have to mock out.
#
# This Dockerfile is shared by every `antithesis-workload-<group>` image
# under test/antithesis/workloads/<group>/.  Each per-group image dir
# symlinks `Dockerfile -> ../../workload/Dockerfile` and passes
# `--build-arg ANTITHESIS_WORKLOAD_GROUP=<group>` via mzbuild.yml.  The
# build arg drives a one-shot filter that pre-bakes only that group's
# `first_/parallel_driver_/singleton_driver_/anytime_` scripts (plus
# all `helper_*.py`) into `/opt/antithesis/test/v1/materialize/`.  Test
# Composer scans that directory once at run start and runs whatever it
# finds — so the filter happens at image-build time, not at container
# startup.

ARG ANTITHESIS_WORKLOAD_GROUP=combined

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

# Re-declare the build arg so it's in scope for RUN steps in this stage.
# (Args before the first FROM are global; reusing them post-FROM requires
# this re-declaration.)
ARG ANTITHESIS_WORKLOAD_GROUP
ENV ANTITHESIS_WORKLOAD_GROUP=${ANTITHESIS_WORKLOAD_GROUP}

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# `mc` (minio client) — single static binary.  Used by
# `parallel-workload/first_polaris_setup.py` to provision the
# iceberg-backing bucket in minio before any CreateIcebergSinkAction
# runs.  Pinned to a recent release for determinism — upstream's
# `latest` rotates on push.
RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2025-04-16T18-13-26Z \
      -o /usr/local/bin/mc \
 && chmod +x /usr/local/bin/mc

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
    pymssql==2.3.4 \
    PyYAML==6.0.2 \
    semver==3.0.4 \
    websocket-client==1.8.0 \
    requests==2.32.3 \
    xxhash==3.5.0 \
    zstandard==0.23.0

# setup-complete script
COPY setup-complete.sh /usr/local/bin/setup-complete.sh
RUN chmod +x /usr/local/bin/setup-complete.sh

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
# Protobuf compiler + well-known-types include tree.  testdrive's
# `$ protobuf-compile-descriptors` builtin shells out to protoc — used
# by 14 .td files (protobuf-*.td, kafka-include-key-sources.td,
# kafka-upsert-sources.td, connection-create-drop.td).  Without these
# the testdrive runner errors with `invoking protoc failed: No such file
# or directory (os error 2)`, which doesn't match `looks_like_fault` and
# would fail the testdrive group's safety assertion on every pick of a
# protoc-using file.  Same protoc binary the testdrive image ships at
# build time (sourced from `protobuf-src`), so behavior matches
# upstream test runs.
COPY --from=testdrive-src /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=testdrive-src /usr/local/include/google /usr/local/include/google
ENV PROTOC=/usr/local/bin/protoc
ENV PROTOC_INCLUDE=/usr/local/include
RUN ldconfig

# Repo-checked-in testdrive .td files for the testdrive-runner drivers,
# brought into the build context by the `pre-image: copy` step in
# mzbuild.yml. The drivers reference these by repo-relative path under
# /opt/testdrive-files, e.g. /opt/testdrive-files/test/pg-cdc/dropped-slot-errors.td.
COPY testdrive-files/ /opt/testdrive-files/

# Workload-group manifest + script selector, used at *build time*
# below to pick the per-group subset of scripts.  The manifest stays
# installed in the image so triage / debugging can recover the same
# enumeration the build used.
COPY groups.yaml /opt/antithesis-config/groups.yaml
COPY scripts-for-group.py /usr/local/bin/scripts-for-group
RUN chmod +x /usr/local/bin/scripts-for-group

# Shared workload helpers (helper_*.py) live in test/helpers/.  Land
# them in /opt/antithesis-pkg/helpers/ and add to PYTHONPATH so every
# driver's `import helper_pg`-style import resolves regardless of
# which template directory under /opt/antithesis/test/v1/ it lives in.
# Antithesis ignores `helper_`-prefixed files for command discovery
# anyway, so it doesn't matter where they sit — PYTHONPATH avoids
# duplicating them into every template's directory.
COPY test/helpers/ /opt/antithesis-pkg/helpers/
ENV PYTHONPATH="${PYTHONPATH}:/opt/antithesis-pkg/helpers"

# Stage the full source tree, then bake the per-template runtime
# directories.  Antithesis Test Composer scans
# /opt/antithesis/test/v1/<template>/ once at run start and selects
# exactly one template per execution history; the bake script reads
# the manifest via `scripts-for-group` and physically copies each
# entry into the right runtime directory (with the `defaults/`
# pseudo-template merged into every real template).  See
# bake-test-templates.sh for the routing details.
COPY test/ /opt/antithesis-stage/
COPY bake-test-templates.sh /usr/local/bin/bake-test-templates.sh
RUN chmod +x /usr/local/bin/bake-test-templates.sh \
 && /usr/local/bin/bake-test-templates.sh "$ANTITHESIS_WORKLOAD_GROUP"


COPY workload-entrypoint.sh /usr/local/bin/workload-entrypoint.sh
RUN chmod +x /usr/local/bin/workload-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/workload-entrypoint.sh"]
