# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic

ARG DEBIAN_FRONTEND="noninteractive"

RUN apt-get update && apt-get install -qy curl gnupg2 \
    && curl -s 'https://packages.confluent.io/deb/6.1/archive.key' | apt-key add - \
    && echo 'deb [arch=amd64] https://packages.confluent.io/deb/6.1 stable main' >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y librdkafka-dev python3-dev python3 python3-pip tzdata

COPY requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt

RUN mkdir -p /usr/share/benchmarks/schemas
COPY schemas /usr/share/benchmarks/schemas

COPY wait_metric /usr/local/bin
ENTRYPOINT ["wait_metric"]
