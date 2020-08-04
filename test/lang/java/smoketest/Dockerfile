# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -qy maven xmlstarlet

COPY pom.xml /scratch/

# Convince Maven to download all necessary plugins to a local repository, so
# that they get baked into the image. The `dependency:go-offline` plugin is a
# bit buggy, so we have to make sure to actually execute a test to get the
# maven-surefire plugin installed.
RUN xmlstarlet ed --inplace \
      --subnode /_:settings --type elem --name localRepository --value /var/lib/maven/repository \
      /usr/share/maven/conf/settings.xml \
   && (cd /scratch \
      && echo "public class NoopTest { @org.junit.jupiter.api.Test void testNoop() {} }" > NoopTest.java \
      && mvn -Dtest=NoopTest dependency:go-offline package) \
   && xmlstarlet ed --inplace \
      --subnode /_:settings --type elem --name offline --value true \
      /usr/share/maven/conf/settings.xml
