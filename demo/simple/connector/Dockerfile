# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM confluentinc/cp-enterprise-kafka:5.3.0

# https://github.com/confluentinc/cp-docker-images/issues/764
RUN sed -i s,https://s3-us-west-2.amazonaws.com/staging-confluent-packages-5.3.0/deb/5.3,https://packages.confluent.io/deb/5.3, /etc/apt/sources.list

RUN apt-get update && apt-get -qy install curl

RUN curl -fsSL https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh > /usr/local/bin/wait-for-it \
    && chmod +x /usr/local/bin/wait-for-it

COPY docker-entrypoint.sh /usr/local/bin

ENTRYPOINT ["docker-entrypoint.sh"]
