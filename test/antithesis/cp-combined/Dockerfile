# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update \
    && apt-get install -y ca-certificates wait-for-it \
    && echo "deb [trusted=yes] https://packages.confluent.io/deb/5.5 stable main" >> /etc/apt/sources.list.d/confluent.list \
    && apt-get update \
    && apt-get install -y openjdk-11-jdk confluent-kafka-2.11 confluent-schema-registry

COPY launch-cp /usr/bin/launch-cp

CMD ["launch-cp"]
