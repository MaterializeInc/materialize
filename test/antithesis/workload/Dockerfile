# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The shard auditor runs `persistcli inspect` against consensus and blob.
MZFROM jobs AS persistcli

FROM python:3.12-slim-trixie

COPY --from=persistcli /usr/local/bin/persistcli /usr/local/bin/persistcli

# In the Antithesis flavor persistcli is instrumented, and Antithesis
# symbolizes coverage from binaries linked under /symbols.
RUN mkdir -p /symbols && ln -s /usr/local/bin/persistcli /symbols/persistcli

ENV MZ_ROOT=/usr/local/share/materialize
ENV PYTHONPATH=/usr/local/share/materialize/misc/python
ENV PYTHONUNBUFFERED=1

COPY misc/python /usr/local/share/materialize/misc/python

# Only the packages the workload imports, at the versions the rest of the
# repository pins. Constraint files cannot carry extras or pip options, so
# those lines are dropped from the pin list first. The Antithesis SDK is pinned
# here rather than in requirements.txt, which is shared with ci-builder and
# feeds every cargo image fingerprint.
RUN grep -v -e '\[' -e '^--' /usr/local/share/materialize/misc/python/requirements.txt > /tmp/constraints.txt \
    && pip install --no-cache-dir --only-binary=:all: --constraint /tmp/constraints.txt \
        antithesis==0.4.0 boto3 colored confluent-kafka kubernetes psycopg psycopg-binary pyyaml requests semver \
    && rm /tmp/constraints.txt

# Antithesis catalogs every assertion under /opt/antithesis/catalog, so that
# assertions a run never reaches are still reported.
RUN mkdir -p /opt/antithesis/catalog \
    && ln -s /usr/local/share/materialize/misc/python/materialize/antithesis /opt/antithesis/catalog/materialize-antithesis

COPY test /opt/antithesis/test/v1
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
