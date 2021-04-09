#
# Copyright 2019 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG=5.5.3

FROM ${DOCKER_UPSTREAM_REGISTRY}confluentinc/cp-base-new:${DOCKER_UPSTREAM_TAG}

ARG PROJECT_VERSION
ARG ARTIFACT_ID

LABEL maintainer="partner-support@confluent.io"
LABEL io.confluent.docker=true
ARG GIT_COMMIT
LABEL io.confluent.docker.git.id=$GIT_COMMIT
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER
LABEL io.confluent.docker.git.repo="confluentinc/kafka-images"

ARG CONFLUENT_VERSION=5.5.3
ARG CONFLUENT_PACKAGES_REPO=https://packages.confluent.io/deb/5.5
ARG CONFLUENT_PLATFORM_LABEL
ARG CONFLUENT_DEB_VERSION
ARG ALLOW_UNSIGNED=true
ARG SCALA_VERSION=2.12

EXPOSE 2181 2888 3888

ENV COMPONENT=zookeeper

RUN echo "===> Installing ${COMPONENT}..." \
    && apt-get update \
    && echo "===> Adding confluent repository...${CONFLUENT_PACKAGES_REPO}" \
    && if [ "x$ALLOW_UNSIGNED" = "xtrue" ]; then echo "APT::Get::AllowUnauthenticated \"true\";" > /etc/apt/apt.conf.d/allow_unauthenticated; else curl -s -L ${CONFLUENT_PACKAGES_REPO}/archive.key -o /tmp/archive.key && apt-key add /tmp/archive.key; fi \
    && echo "deb [arch=amd64] ${CONFLUENT_PACKAGES_REPO} stable main" >> /etc/apt/sources.list \
    && cat /etc/apt/sources.list \
    && apt-get install -y apt-transport-https \
    && apt-get update \
    && apt-get install -y confluent-kafka-${SCALA_VERSION}=${CONFLUENT_VERSION}${CONFLUENT_PLATFORM_LABEL}-${CONFLUENT_DEB_VERSION} \
    && echo "===> clean up ..." \
    && rm -rf /tmp/* \
    && echo "===> Setting up ${COMPONENT} dirs" \
    && mkdir -p /etc/kafka /var/log/kafka /var/log/confluent /var/lib/kafka /var/lib/zookeeper /var/lib/${COMPONENT}/data /var/lib/${COMPONENT}/log /etc/${COMPONENT}/secrets \
    && chmod -R ag+w /etc/kafka /var/lib/${COMPONENT}/data /var/lib/${COMPONENT}/log /etc/${COMPONENT}/secrets \
    && chown -R root:root /var/log/kafka /var/log/confluent /var/lib/kafka /var/lib/zookeeper

VOLUME ["/var/lib/${COMPONENT}/data", "/var/lib/${COMPONENT}/log", "/etc/${COMPONENT}/secrets"]

ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG=latest

ARG PROJECT_VERSION
ARG ARTIFACT_ID

LABEL maintainer="partner-support@confluent.io"
LABEL io.confluent.docker=true
ARG GIT_COMMIT
LABEL io.confluent.docker.git.id=$GIT_COMMIT
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER
LABEL io.confluent.docker.git.repo="confluentinc/kafka-images"

# allow arg override of required env params
ARG KAFKA_ZOOKEEPER_CONNECT
ENV KAFKA_ZOOKEEPER_CONNECT=${KAFKA_ZOOKEEPER_CONNECT}
ARG KAFKA_ADVERTISED_LISTENERS
ENV KAFKA_ADVERTISED_LISTENERS=${KAFKA_ADVERTISED_LISTENERS}

ENV COMPONENT=kafka

# primary
EXPOSE 9092

RUN echo "===> Installing ${COMPONENT}..." \
    && echo "===> clean up ..." \
    && rm -rf /tmp/* \
    && echo "===> Setting up ${COMPONENT} dirs..." \
    && mkdir -p /etc/kafka /var/log/kafka /var/log/confluent /var/lib/kafka /var/lib/zookeeper /var/lib/${COMPONENT}/data /etc/${COMPONENT}/secrets\
    && chmod -R ag+w /etc/${COMPONENT} /var/lib/${COMPONENT}/data /etc/${COMPONENT}/secrets \
    && chown -R root:root /var/log/kafka /var/log/confluent /var/lib/kafka /var/lib/zookeeper

VOLUME ["/var/lib/${COMPONENT}/data", "/etc/${COMPONENT}/secrets"]

ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG=latest

ARG PROJECT_VERSION
ARG ARTIFACT_ID

LABEL maintainer="partner-support@confluent.io"
LABEL io.confluent.docker=true
ARG GIT_COMMIT
LABEL io.confluent.docker.git.id=$GIT_COMMIT
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER
LABEL io.confluent.docker.git.repo="confluentinc/schema-registry-images"

ENV COMPONENT=schema-registry

# Default listener
EXPOSE 8081

ARG CONFLUENT_PACKAGES_REPO=https://packages.confluent.io/deb/5.5

RUN echo "===> Installing ${COMPONENT}..." \
    && apt-get install -y confluent-${COMPONENT}=${CONFLUENT_VERSION}${CONFLUENT_PLATFORM_LABEL}-${CONFLUENT_DEB_VERSION} \
    confluent-control-center=${CONFLUENT_VERSION}${CONFLUENT_PLATFORM_LABEL}-${CONFLUENT_DEB_VERSION} \
    confluent-security=${CONFLUENT_VERSION}${CONFLUENT_PLATFORM_LABEL}-${CONFLUENT_DEB_VERSION} \
    && echo "===> clean up ..." \
    && rm -rf /tmp/* \
    && echo "===> Setting up ${COMPONENT} dirs" \
    && mkdir -p /etc/${COMPONENT}/secrets \
    && chmod -R ag+w /etc/${COMPONENT} /etc/${COMPONENT}/secrets

VOLUME ["/etc/${COMPONENT}/secrets"]

COPY zookeeper/include/etc/confluent/docker /etc/confluent/docker/zookeeper/
COPY kafka/include/etc/confluent/docker /etc/confluent/docker/kafka/
COPY schema-registry/include/etc/confluent/docker /etc/confluent/docker/schema-registry

CMD /bin/bash -c 'COMPONENT=zookeeper /etc/confluent/docker/zookeeper/run; COMPONENT=kafka /etc/confluent/docker/kafka/run ; COMPONENT=schema-registry ; /etc/confluent/docker/schema-registry/run '
