# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# 22.1.2 community images with amd and arm variants. Used here so we can copy
# the cockroach binary if this is an arm host. We are temporarily using this
# image from the community until an official image is published.
FROM juliuszaromskis/cockroachdb:22.1.2 AS crdb-arm

# Create an image that determines the cockroach binary (so we don't have two
# copies in the final image).
MZFROM ubuntu-base AS crdb-bin

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install \
        ca-certificates \
        curl

COPY --from=crdb-arm /cockroach/cockroach /cockroach-arm

ARG COCKROACH_VERSION=22.1.5

RUN set -eux; \
	arch="$(dpkg --print-architecture)"; arch="${arch##*-}"; \
	case "$arch" in \
		'amd64') \
		         curl https://binaries.cockroachdb.com/cockroach-v${COCKROACH_VERSION}.linux-amd64.tgz | tar -xz;  \
		         cp cockroach-v${COCKROACH_VERSION}.linux-amd64/cockroach /cockroach; \
			;; \
		'arm64') \
			mv /cockroach-arm /cockroach; \
			;; \
		*) echo >&2 "error: unsupported architecture '$arch'"; exit 1 ;; \
	esac;

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

COPY --from=crdb-bin /cockroach /usr/local/bin/cockroach

COPY storaged computed environmentd entrypoint.sh /usr/local/bin/

USER materialize

ENTRYPOINT ["tini", "--", "entrypoint.sh"]
