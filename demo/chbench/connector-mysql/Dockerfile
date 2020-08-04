# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM confluentinc/cp-enterprise-kafka:5.3.0

# https://github.com/confluentinc/cp-docker-images/issues/764
RUN sed -i s,https://s3-us-west-2.amazonaws.com/staging-confluent-packages-5.3.0/deb/5.3,https://packages.confluent.io/deb/5.3, /etc/apt/sources.list

RUN apt-get update && apt-get -qy install curl

RUN curl -fsSL https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /usr/local/bin/wait-for-it \
    && chmod +x /usr/local/bin/wait-for-it

COPY docker-entrypoint.sh /usr/local/bin

ENTRYPOINT ["docker-entrypoint.sh"]
