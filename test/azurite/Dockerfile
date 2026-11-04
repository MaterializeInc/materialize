# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:984d31d4bd6e71bb9469353d93e73c68f7b3c17142b2aec45f2a960489095b22 AS ca-certs

FROM node:22-slim

# Fetch apt indexes and packages over HTTPS only. The build fails if any
# http:// apt source remains.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && for f in /etc/apt/sources.list /etc/apt/sources.list.d/*; do \
        [ -f "$f" ] || continue; \
        sed 's|http://|https://|g' "$f" > "$f.tmp" && mv "$f.tmp" "$f"; \
    done \
    && ! grep -rqs 'http://' /etc/apt/sources.list /etc/apt/sources.list.d

RUN apt-get update && \
    apt-get install -y eatmydata netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN npm install -g azurite@3.37.0
