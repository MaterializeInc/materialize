# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build and run SQLancer ported for Materialize

MZFROM ubuntu-base

WORKDIR /workdir

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    default-jdk \
    maven

# Rebuild since SQLancer repo might have changed
ADD https://api.github.com/repos/def-/sqlancer/git/refs/heads/master version.json

# Build SQLancer
RUN git clone --depth=1 --single-branch https://github.com/def-/sqlancer \
    && cd sqlancer \
    && mvn package -DskipTests

ENTRYPOINT ["/usr/bin/java", "-jar", "sqlancer/target/sqlancer-2.0.0.jar"]
