# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM confluentinc/cp-schema-registry:5.5.3

RUN mkdir /data

RUN mkdir /usr/share/producer
COPY key-schema.json /usr/share/producer/
COPY value-schema.json /usr/share/producer/

COPY produce_rows /usr/local/bin
CMD /usr/local/bin/produce_rows
