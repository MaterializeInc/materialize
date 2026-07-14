# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TLC model checker for TLA+ formal verification specs.

FROM eclipse-temurin:21-jre-alpine

ARG TLA_VERSION=1.8.0
ARG TLA_SHA1=5251de86a3465c332fefeeb15b494cc808458f86
RUN apk add --no-cache wget && \
    wget -q "https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar" \
         -O /opt/tla2tools.jar && \
    echo "${TLA_SHA1}  /opt/tla2tools.jar" | sha1sum -c - && \
    apk del wget

WORKDIR /spec

ENTRYPOINT ["java", "-XX:+UseParallelGC", "-jar", "/opt/tla2tools.jar"]
