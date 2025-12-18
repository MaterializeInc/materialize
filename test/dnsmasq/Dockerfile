# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM alpine:3.22.2

RUN apk update && apk add dnsmasq

COPY entrypoint.sh /

RUN chmod +x /entrypoint.sh

WORKDIR /

ENTRYPOINT ["/entrypoint.sh"]
