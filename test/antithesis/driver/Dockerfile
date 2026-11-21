# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM python:3.12-slim-trixie

# Debugging tools for `snouty runs exec` at a moment of a run.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl jq postgresql-client \
    && rm -rf /var/lib/apt/lists/*

ENV MZ_ROOT=/usr/local/share/materialize
ENV PYTHONPATH=/usr/local/share/materialize/misc/python
ENV PYTHONUNBUFFERED=1
# A native crash (SIGSEGV, SIGABRT) prints every thread's Python stack before
# the process dies, which Antithesis otherwise reports as only a signal number.
ENV PYTHONFAULTHANDLER=1

COPY misc/python /usr/local/share/materialize/misc/python

# Only the packages the driver imports, at the versions the rest of the
# repository pins. Constraint files cannot carry extras or pip options, so
# those lines are dropped from the pin list first.
RUN grep -v -e '\[' -e '^--' /usr/local/share/materialize/misc/python/requirements.txt > /tmp/constraints.txt \
    && pip install --no-cache-dir --only-binary=:all: --constraint /tmp/constraints.txt \
        --requirement /usr/local/share/materialize/misc/python/materialize/antithesis/requirements.txt \
        boto3 confluent-kafka cryptography fastavro kubernetes psycopg psycopg-binary pymssql \
        PyMySQL pyyaml requests \
    && rm /tmp/constraints.txt

# Antithesis catalogs the assertions and collects coverage of the Python code
# under /opt/antithesis/catalog. Only the driver's package is linked: the
# legacy workload's assertions would otherwise be reported as never reached.
RUN mkdir -p /opt/antithesis/catalog \
    && ln -s /usr/local/share/materialize/misc/python/materialize/antithesis_driver /opt/antithesis/catalog/driver

COPY first_start_workload /opt/antithesis/test/v1/driver/first_start_workload

CMD ["python3", "-m", "materialize.antithesis_driver"]
