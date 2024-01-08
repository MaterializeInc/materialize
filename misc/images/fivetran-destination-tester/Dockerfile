# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM bazel

WORKDIR /home/ubuntu

COPY build build

RUN cd build \
    && bazel build //testers:run_sdk_destination_tester_deploy.jar

RUN apt-get update \
    && apt-get -qy install openjdk-17-jre

ENTRYPOINT ["java", "-jar", "/home/ubuntu/build/bazel-bin/testers/run_sdk_destination_tester_deploy.jar", "--working-dir=/data"]
