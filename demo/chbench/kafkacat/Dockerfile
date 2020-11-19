# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM edenhill/kafkacat:1.6.0

RUN apk add bash curl jq

COPY ./dump_topics /usr/local/bin/dump_topics
RUN chmod +x /usr/local/bin/dump_topics

COPY ./topic_names.txt /etc/chbenchmark/topic_names.txt
