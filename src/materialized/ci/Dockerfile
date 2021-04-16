# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get -qy install ca-certificates gnupg wget

RUN    export version=v0.19.0 \
    && export digest=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c \
    && wget "https://github.com/krallin/tini/releases/download/${version}/tini" -O /tini \
    && echo "${digest} /tini" | sha256sum -c - \
    && chmod +x /tini

COPY kdestroy kinit materialized /usr/local/bin/

ENTRYPOINT ["/tini", "--", "materialized", "--log-file=stderr" ]
