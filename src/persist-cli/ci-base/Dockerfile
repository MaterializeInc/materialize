# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This is a separate mzimage from maelstrom-persist so that we don't have to
# re-install the apt things or maelstrom every time we get a CI builder with a
# cold cache.

MZFROM ubuntu-base

RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    bzip2 \
    curl \
    git \
    gnuplot \
    graphviz \
    openjdk-11-jre \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/share/java \
    && curl -fsSL https://github.com/jepsen-io/maelstrom/releases/download/v0.2.1/maelstrom.tar.bz2 \
        | tar -xj --strip-components=2 -C /usr/local/share/java maelstrom/lib/maelstrom.jar
