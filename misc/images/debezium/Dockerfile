# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# There is no pre-built Docker image that includes both Confluent's Avro
# converter (which knows how to write schemas to the Confluent Schema Registry)
# and Debezium connectors, so we have to build our own.

FROM confluentinc/cp-kafka-connect-base:8.0.0

# Be sure to use a `X.Y.Z.Final` tag here; `X.Y` tags refer to the latest minor
# version in the release series, and minor versions have been known to introduce
# breakage.
ARG DEBEZIUM_VERSION="3.3.1.Final"

RUN : \
    && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/${DEBEZIUM_VERSION}/debezium-connector-mysql-${DEBEZIUM_VERSION}-plugin.tar.gz | tar -zxC /usr/share/java \
    && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz | tar -zxC /usr/share/java \
    && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/${DEBEZIUM_VERSION}/debezium-connector-sqlserver-${DEBEZIUM_VERSION}-plugin.tar.gz | tar -zxC /usr/share/java
