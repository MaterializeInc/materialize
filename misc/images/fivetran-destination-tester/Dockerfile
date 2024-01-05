# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM gcr.io/bazel-public/bazel:7.0.0

COPY --chown=ubuntu:root build build

RUN cd build \
    && bazel build //testers:run_sdk_destination_tester_deploy.jar

MZFROM ubuntu-base

RUN apt-get update \
    && apt-get -qy install openjdk-17-jre

COPY --from=0 /home/ubuntu/build/bazel-bin/testers/run_sdk_destination_tester_deploy.jar /usr/local/share/java/

ENTRYPOINT ["java", "-jar", "/usr/local/share/java/run_sdk_destination_tester_deploy.jar", "--working-dir=/data"]
