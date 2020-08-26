# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -qy maven

# Package twice to take advantage of Docker caching for local development.
# The first packaging only uses pom.xml as input.

COPY pom.xml /scratch/pom.xml
RUN cd /scratch && mvn package

COPY . /scratch
RUN cd /scratch && mvn -o package

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -qy openjdk-11-jre-headless

COPY --from=0 /scratch/target/tb-0.1-SNAPSHOT.jar /usr/local/share/java/tb.jar

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/local/share/java/tb.jar"]

CMD ["-h"]
