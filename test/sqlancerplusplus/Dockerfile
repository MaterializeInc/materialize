# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build and run SQLancer++ ported for Materialize

MZFROM ubuntu-base

WORKDIR /workdir

RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    default-jdk \
    maven \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/*

# Build SQLancer++
RUN git clone --single-branch --depth=1 https://github.com/MaterializeInc/sqlancerplusplus \
    && cd sqlancerplusplus \
    && git checkout bc84af4f22f6c2f598d061dd203f45a4ffdaad93 \
    && rm -rf .git \
    && mvn package -DskipTests \
    && sed -i "s/MATERIALIZE.host=localhost/MATERIALIZE.host=materialized/g" dbconfigs/jdbc.properties

WORKDIR /workdir/sqlancerplusplus
ENTRYPOINT ["/usr/bin/eatmydata", "/usr/bin/java", "-Xmx16g", "-jar", "target/sqlancer-2.0.0.jar"]
