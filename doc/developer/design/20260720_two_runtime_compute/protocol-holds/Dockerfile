# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# TLC image for model checking the two-runtime read-hold protocol in this
# directory. TLC ships as a single jar (SANY, the PlusCal translator and
# the checker), so this is a headless JRE plus that jar and nothing else.
#
# The jar is pinned by digest. TLC's own version determines which
# counterexamples a run reports, so an unpinned jar makes a green run
# unreproducible. `tla2tools.jar` is MIT licensed.
#
# Runtime contract: the specs are bind-mounted at /spec, which is the
# working directory. TLC writes states/ and a .out next to the spec it
# runs, so the mount must be writable.

FROM ubuntu:26.04

ARG TLA_VERSION
ARG TLA_SHA256

ENV DEBIAN_FRONTEND=noninteractive
# TLC picks up JVM options from here, and the default heap is far too
# small for even a modest state space.
ENV JAVA_TOOL_OPTIONS="-Xmx3g"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# NOTE: no `tlc2.TLC -help` smoke check here. TLC exits non-zero after
# printing its usage, so it cannot serve as one. The digest check is the
# verification that matters, and `Smoke.tla` proves the jar actually runs.
RUN curl --proto '=https' --tlsv1.2 -fsSL \
        "https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar" \
        -o /opt/tla2tools.jar \
    && echo "${TLA_SHA256}  /opt/tla2tools.jar" | sha256sum --check --strict

WORKDIR /spec

ENTRYPOINT ["java", "-cp", "/opt/tla2tools.jar"]
CMD ["tlc2.TLC", "-help"]
