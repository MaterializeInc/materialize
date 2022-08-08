# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install \
        ca-certificates \
        curl \
        postgresql-client-14 \
        tini \
    && groupadd --system --gid=999 materialize \
    && useradd --system --gid=999 --uid=999 --create-home materialize \
    && mkdir /mzdata \
    && mkdir /cockroach-data \
    && chown materialize /mzdata /cockroach-data

ARG COCKROACH_VERSION=22.1.5

RUN curl https://binaries.cockroachdb.com/cockroach-v${COCKROACH_VERSION}.linux-amd64.tgz | tar -xz \
    && cp cockroach-v${COCKROACH_VERSION}.linux-amd64/cockroach /usr/local/bin/

COPY storaged computed environmentd entrypoint.sh /usr/local/bin/

USER materialize

ENTRYPOINT ["tini", "--", "entrypoint.sh"]
