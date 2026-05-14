# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Antithesis Test Composer client container.
#
# Inherits the testdrive image so the `testdrive` binary, postgres client,
# protobuf, and libduckdb are already in place. Adds Python plus the Antithesis
# Python SDK, copies the workload helpers and test commands, and installs a
# readiness entrypoint that emits `setup_complete` once the SUT is up.

MZFROM testdrive

USER root

# Disable color output everywhere (Antithesis stores raw bytes; ANSI escapes
# are garbage in logs and triage).
ENV NO_COLOR=1
ENV FORCE_COLOR=0
ENV PY_COLORS=0
ENV CLICOLOR=0
ENV TERM=dumb

RUN apt-get update \
 && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends \
        python3 python3-pip ca-certificates curl netcat-openbsd \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Install the Antithesis Python SDK.
# Pinned to the latest stable release at image-creation time. Bump in lockstep
# with `misc/python/materialize/antithesis/sdk.py` import expectations.
RUN pip3 install --no-cache-dir --break-system-packages antithesis==0.2.0

# Layout convention: MZ_ROOT mirrors the repo root, so existing helpers that
# look up `MZ_ROOT/misc/python` continue to work.
COPY misc/python /opt/materialize/misc/python
COPY antithesis/test /opt/materialize/antithesis/test

# Subset of test/testdrive/*.td files referenced by the Tier 1 wrappers. The
# mzbuild.yml `pre-image: copy` blocks restrict which files are pulled in, so
# only those land here; full test/testdrive/ is intentionally not bundled.
COPY td/testdrive /opt/materialize/td/testdrive

# Antithesis Test Composer command discovery.
RUN mkdir -p /opt/antithesis/test \
 && cp -R /opt/materialize/antithesis/test/v1 /opt/antithesis/test/v1 \
 && chmod -R +x /opt/antithesis/test/v1 \
 && find /opt/antithesis/test/v1 -name 'helper_*' -prune -o -type f -name '*.py' -print -exec chmod -x {} +

# Antithesis Python SDK assertion cataloging. Symlinking under
# /opt/antithesis/catalog/ lets the platform discover .py files that contain
# SDK calls without duplicating them on disk.
RUN mkdir -p /opt/antithesis/catalog \
 && ln -s /opt/materialize/misc/python/materialize/antithesis /opt/antithesis/catalog/materialize_antithesis \
 && ln -s /opt/antithesis/test/v1 /opt/antithesis/catalog/test_v1

ENV MZ_ROOT=/opt/materialize
# Two roots on PYTHONPATH: misc/python so `materialize.antithesis.*` resolves,
# and the test/v1 directory so command files can `from helper_bootstrap
# import …` regardless of which template directory they live in.
ENV PYTHONPATH=/opt/antithesis/test/v1:/opt/materialize/misc/python
ENV PATH=/opt/antithesis/test/v1:$PATH

# Default workload-side environment so the upsert_sources commands can find
# Materialize, Kafka, and the Schema Registry. docker-compose can override.
ENV MZ_TESTDRIVE=testdrive
ENV MZ_MATERIALIZE_URL=postgres://materialize@materialized:6875
ENV MZ_MATERIALIZE_INTERNAL_URL=postgres://mz_system@materialized:6877
ENV MZ_KAFKA_ADDR=kafka:9092
ENV MZ_SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV MZ_TESTDRIVE_DEFAULT_TIMEOUT=60s
ENV MZ_HOST=materialized
ENV MZ_SQL_PORT=6875
ENV MZ_HTTP_PORT=6878
ENV KAFKA_HOST=kafka
ENV KAFKA_PORT=9092
ENV SR_HOST=schema-registry
ENV SR_PORT=8081

COPY entrypoint.sh /usr/local/bin/antithesis-test-driver-entrypoint
RUN chmod +x /usr/local/bin/antithesis-test-driver-entrypoint

WORKDIR /opt/antithesis

ENTRYPOINT ["/usr/local/bin/antithesis-test-driver-entrypoint"]
